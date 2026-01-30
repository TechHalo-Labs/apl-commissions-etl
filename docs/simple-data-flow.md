# Data Flow Documentation

## Data Ingestion & Schema Setup

### Data Source
- **Source Schema**: `poc_etl` or `new_data` (configurable via SOURCE_SCHEMA parameter)
- **Raw Data**: CSV files loaded into SQL Server tables (certificates, brokers, schedules, premiums, etc.)
- **Volume**: 1.5-1.7M certificates + supporting data tables

### Transformation Process

1. **Schema Creation** (`sql/00-schema-setup.sql`)
   - Drops and recreates the [etl] schema
   - Creates raw_*, input_*, and stg_* table structures
   - Sets up prestage and conformance tables
   - Initializes state management tables for pipeline tracking

2. **Raw Data Copy** (`sql/ingest/copy-from-poc-etl.sql`)
   - **Phase 1**: Copies all raw tables from source schema to ETL schema
   - Transfers certificate_info, schedule_rates, premiums, broker data, etc.
   - Bulk INSERT operations with row count verification
   - Handles both poc_etl and new_data source schemas

3. **Input Table Population** (`sql/ingest/populate-input-tables.sql`)
   - **Phase 2**: Transforms raw_* tables into structured input_* tables
   - Processes certificate data with proper typing and validation
   - Creates commission hierarchy and performance group inputs
   - Prepares data for transform phase consumption

4. **Data Quality Validation**
   - Verifies row counts match between source and destination
   - Validates data type conversions and null handling
   - Ensures referential integrity for downstream processing

### Script Responsible
- **Schema Setup**: `sql/00-schema-setup.sql`, `sql/01-raw-tables.sql`, `sql/02-input-tables.sql`, `sql/03-staging-tables.sql`
- **Data Ingestion**: `sql/ingest/copy-from-poc-etl.sql`, `sql/ingest/populate-input-tables.sql`
- **Pipeline Integration**: Phase 1 (schema) and Phase 2 (ingest) in `scripts/run-pipeline.ts`
- **Order**: Runs before all transforms, establishes data foundation

### Output Tables
- **Raw Tables**: `etl.raw_*` - Direct copies from source (certificates, brokers, schedules, premiums)
- **Input Tables**: `etl.input_*` - Structured input data ready for transformation
- **Schema**: Complete ETL schema with all required table structures

## Brokers

### Data Source
- **Source Tables**: `new_data.IndividualRoster`, `new_data.OrganizationRoster`
- **Raw Data**: Latest broker information from APL source systems
- **Volume**: Individual brokers + organization brokers (complete roster, not filtered to active only)

### Transformation Process

1. **Raw Data Copy** (`sql/ingest/05-copy-new-data-to-etl.sql`)
   - Copies from `new_data.IndividualRoster` → `etl.raw_individual_brokers`
   - Copies from `new_data.OrganizationRoster` → `etl.raw_org_brokers`
   - Includes ALL brokers (active, terminated, terminated residual)

2. **Input Staging** (`sql/ingest/02-copy-from-new-data.sql`)
   - Copies from raw tables to input tables for processing
   - `etl.raw_individual_brokers` → `etl.input_individual_brokers`
   - `etl.raw_org_brokers` → `etl.input_organization_brokers`

3. **Broker Transform** (`sql/transforms/01-brokers.sql`)
   - Loads ALL individual brokers from raw tables
   - Loads ALL organization brokers from raw tables
   - Handles legacy roster fallback tables if available
   - Creates placeholder brokers for missing references
   - Includes brokers from certificates for referential integrity
   - Also processes broker licenses and E&O insurance data

4. **Data Quality Rules**
   - Status normalization (Active/Terminated/TerminatedResiduals)
   - ID generation from PartyUniqueId (remove 'P' prefix)
   - Name formatting (LastName, FirstName for individuals)
   - Date validation and corrections for licenses
   - Fallback data sources for completeness

### Script Responsible
- **Primary Script**: `sql/transforms/01-brokers.sql`
- **Pipeline Integration**: Executed as step 2 in `scripts/run-pipeline.ts`
- **Order**: Runs after reference data setup (step 1: `00-references.sql`)

### Output Tables
- `etl.stg_brokers` - Main broker data
- `etl.stg_broker_licenses` - License information
- `etl.stg_broker_eo_insurances` - E&O insurance coverage

## Broker Licenses

### Data Source
- **Source Table**: `new_data.LicenseInfo` (via `raw_licenses`)
- **Raw Data**: Broker license details including state, license number, effective/expiration dates
- **Volume**: ALL licenses (not filtered to active brokers only)

### Transformation Process

1. **Raw Data Copy** (`sql/ingest/05-copy-new-data-to-etl.sql`)
   - Copies license data to `etl.raw_licenses`

2. **Input Staging** (`sql/ingest/populate-input-tables.sql`)
   - Prepares license data for processing

3. **License Transform** (`sql/transforms/01-brokers.sql`)
   - Loads ALL broker licenses from raw tables
   - Applies date corrections:
     - If EffectiveDate is NULL but ExpirationDate exists, uses ExpirationDate as EffectiveDate
     - If ExpirationDate is NULL or in the past for Active licenses, sets to 2027-01-01
     - Trusts CurrentStatus over date validation (Active = valid license)
   - Maps PartyUniqueId to BrokerId

4. **Data Quality Rules**
   - Status mapping (Active=0, Inactive=1, other=2)
   - Date validation and future-dating corrections
   - License code and resident license handling

### Script Responsible
- **Primary Script**: `sql/transforms/01-brokers.sql` (licenses section)
- **Pipeline Integration**: Executed as part of step 2 in `scripts/run-pipeline.ts`
- **Order**: Runs together with main broker processing

### Output Tables
- `etl.stg_broker_licenses` - Complete license records with date corrections

## Employer Groups

### Data Source
- **Primary Source**: `new_data.PerfGroupModel` - Group names, states, broker associations
- **Fallback Sources**:
  - `new_data.Premiums` - Alternative group names and states
  - `new_data.CertificateInfo` - Unique group IDs and certificate states
- **Volume**: All unique groups found in certificates (~groups)

### Transformation Process

1. **Raw Data Copy** (`sql/ingest/05-copy-new-data-to-etl.sql`)
   - Copies PerfGroupModel to `etl.raw_perf_groups`
   - Copies Premiums data to `etl.raw_premiums`

2. **Input Staging** (`sql/ingest/02-copy-from-new-data.sql`)
   - Copies PerfGroupModel to `etl.input_perf_groups`

3. **Group Transform** (`sql/transforms/02-groups.sql`)
   - Builds group name lookup from perf-groups (primary source, 97% coverage)
   - Adds fallback names from premiums for missing groups
   - Gets all unique groups from certificates
   - Creates PrimaryBrokerId by matching BrokerUniqueId to stg_brokers
   - Applies name priority: perf-group name > premium name > generated name
   - Applies state priority: perf-group state > premium state > certificate state

4. **Data Quality Rules**
   - Canonical ID format: G{GroupNumber}
   - Special G00000 group for Direct-to-Consumer policies
   - All groups from certificates marked as active
   - Name generation fallback: "Group {GroupNumber}"

### Script Responsible
- **Primary Script**: `sql/transforms/02-groups.sql`
- **Pipeline Integration**: Executed as step 3 in `scripts/run-pipeline.ts`
- **Order**: Runs after brokers (needs PrimaryBrokerId lookup)

### Output Tables
- `etl.stg_groups` - Employer groups with PrimaryBrokerId links and comprehensive naming

## Products & Plans

### Data Source
- **Primary Source**: `new_data` (via `input_certificate_info`) - Product codes, categories, plan codes from certificates
- **Secondary Source**: `raw_data.raw_schedule_rates` (imported to `etl.raw_schedule_rates`) - Additional products from schedule definitions
- **Volume**: All unique products and plans found in certificates and schedules

### Transformation Process

1. **Raw Data Copy** (`sql/ingest/05-copy-new-data-to-etl.sql`)
   - Copies certificate data to `etl.input_certificate_info`
   - Imports schedule rates to `etl.raw_schedule_rates` from `raw_data.raw_schedule_rates`

2. **Input Staging** (`sql/ingest/02-copy-from-new-data.sql`)
   - Prepares certificate data for processing

3. **Products Transform** (`sql/transforms/03-products.sql`)
   - **Products**: Extracts unique products from certificates (primary source)
   - **Products**: Adds products from schedule rates not already present
   - **Product Codes**: Creates product code mappings with state information
   - **Plans**: Creates unique plan codes linked to products from certificate data
   - Applies name resolution and category assignments

4. **Data Quality Rules**
   - Product ID format: Uses ProductCode as primary key
   - Plan ID format: {ProductCode}-{PlanCode}
   - Deduplication based on trimmed values
   - Fallback naming for missing descriptions
   - State-specific product code mappings

### Script Responsible
- **Primary Script**: `sql/transforms/03-products.sql`
- **Pipeline Integration**: Executed as step 4 in `scripts/run-pipeline.ts`
- **Order**: Runs after groups, before schedules

### Output Tables
- `etl.stg_products` - Product master data with categories and descriptions
- `etl.stg_product_codes` - Product code mappings with state information
- `etl.stg_plans` - Plan codes linked to their parent products

## Schedules & Schedule Rates

### Data Source
- **Source Table**: `raw_data.raw_schedule_rates` (imported to `etl.raw_schedule_rates`)
- **Raw Data**: Complete commission schedule definitions with rates, tiers, and state variations
- **Volume**: ALL schedules (no filtering - includes unused schedules for complete library)

### Transformation Process

1. **Raw Data Copy** (`sql/ingest/01-ingest-schedules-from-source.sql`)
   - Imports complete schedule rates from `raw_data.raw_schedule_rates` to `etl.raw_schedule_rates`
   - Includes all schedules regardless of certificate usage (critical fix)

2. **Schedules Transform** (`sql/transforms/04-schedules.sql`)
   - **Schedules**: Creates unique schedules from ScheduleName values
   - **Schedule Versions**: Creates versioned schedule configurations (one per schedule)
   - **Schedule Rates**: Creates individual rate tiers with first-year and renewal rates
   - Preserves ALL state-specific rate variations (no consolidation)
   - Applies rate hierarchy: Year1 → Year2/Year66 → Level (base rate)

3. **Data Quality Rules**
   - Imports ALL schedules (no filtering by usage)
   - Preserves state-specific rates (critical for regulatory compliance)
   - Rate fallback hierarchy for missing values
   - Tiered rate structure with group size ranges
   - Effective date ranges from source data

### Script Responsible
- **Primary Script**: `sql/transforms/04-schedules.sql`
- **Pipeline Integration**: Executed as step 5 in `scripts/run-pipeline.ts`
- **Order**: Runs after products, before proposal processing

### Output Tables
- `etl.stg_schedules` - Schedule master data with effective dates and product counts
- `etl.stg_schedule_versions` - Versioned schedule configurations
- `etl.stg_schedule_rates` - Individual rate tiers with first-year/renewal rates and state variations

## Proposal Builder

### Data Source
- **Source Table**: `etl.input_certificate_info` (processed certificates)
- **Raw Data**: Certificate records with split configurations, broker hierarchies, and commission schedules
- **Volume**: All certificates from input processing (~1.7M records)

### Transformation Process

1. **Certificate Loading** (`loadCertificates()`)
   - Loads all certificates from `input_certificate_info`
   - Groups certificates by unique combinations for processing

2. **Selection Criteria Extraction** (`extractSelectionCriteria()`)
   - Groups certificates by (GroupId, ConfigurationHash)
   - Builds split configurations with broker hierarchies and commission schedules
   - Computes SHA256 hashes for configuration deduplication
   - Performance optimized with O(n) grouping instead of O(n²) filtering

3. **Non-Conformant Case Identification** (`identifyNonConformantCases()`)
   - **DTC Policies**: Routes GroupId='00000' to Policy Hierarchy Assignments
   - **Non-Conformant Groups**: Routes groups flagged as IsNonConformant=1 to PHA
   - **Split Mismatches**: Routes certificates with total split percent ≠ 100% to PHA
   - **Invalid Groups**: Routes null/empty/all-zero GroupIds to PHA

4. **Proposal Building** (`buildProposals()`)
   - Creates proposals from conformant selection criteria
   - Groups certificates by (GroupId, ConfigHash) keys
   - Generates proposal numbers and extracts broker assignments
   - Builds premium split versions and hierarchy structures

5. **Staging Output Generation** (`generateStagingOutput()`)
   - Creates all staging tables with proper relationships
   - Links proposals to hierarchies and commission schedules
   - Generates lookup tables for downstream processing

### Script Responsible
- **Primary Script**: `scripts/proposal-builder.ts`
- **Pipeline Integration**: Executed conditionally via `--use-ts-builder` flag in `scripts/run-pipeline.ts`
- **Order**: Runs as part of proposal processing phase (between schedules and hierarchies)

### Output Tables
- `etl.stg_proposals` - Commission agreements with broker assignments
- `etl.stg_proposal_key_mapping` - Lookup table for proposal resolution
- `etl.stg_premium_split_versions` - Premium split configurations
- `etl.stg_premium_split_participants` - Split participants linked to hierarchies
- `etl.stg_hierarchies` - Hierarchy containers with broker chains
- `etl.stg_hierarchy_versions` - Time-versioned hierarchy structures
- `etl.stg_hierarchy_participants` - Individual brokers in hierarchy chains
- `etl.stg_policy_hierarchy_assignments` - Direct assignments for non-conformant policies
- `etl.stg_policy_hierarchy_participants` - Embedded participants for PHA records

## Policies

### Data Source
- **Source Table**: `etl.input_certificate_info` (processed certificates)
- **Raw Data**: Certificate records with policy details, groups, products, and effective dates
- **Volume**: One policy per active certificate (~1.7M records)

### Transformation Process

1. **Minimum Split Sequence Identification**
   - Finds the minimum CertSplitSeq for each certificate
   - Filters to active records (RecStatus = 'A')

2. **Policy Data Aggregation**
   - Groups certificate data by CertificateId
   - Aggregates policy attributes (company, product, plan, dates)
   - Normalizes GroupId (converts empty/null to '00000' for DTC)

3. **Proposal Assignment Resolution**
   - Links policies to proposals via ProposalId
   - Determines assignment source (Proposal vs Direct)
   - Resolves broker relationships and hierarchy links

4. **Data Quality Validation**
   - Validates required fields and data types
   - Applies business rules for policy status
   - Ensures referential integrity with groups and products

### Script Responsible
- **Primary Script**: `sql/transforms/09-policies.sql`
- **Pipeline Integration**: Executed as step 10 in `scripts/run-pipeline.ts`
- **Order**: Runs after proposal builder, before premium transactions

### Output Tables
- `etl.stg_policies` - Individual policy records linked to proposals, groups, and brokers

## Premium Transactions

### Data Source
- **Source Table**: `etl.raw_premiums` (imported from source system)
- **Raw Data**: Premium payment records with amounts, dates, and policy references
- **Volume**: Individual premium transactions from billing system

### Transformation Process

1. **Staging Table Preparation**
   - Truncates existing premium transactions data

2. **Transaction Data Loading**
   - Loads premium records from raw_premiums
   - Converts data types (Policy to BIGINT, Amount to DECIMAL)
   - Calculates billing period dates from DatePaidTo

3. **Data Validation and Filtering**
   - Filters out invalid records (null Policy, empty Policy, null Amount)
   - Applies date conversions and validations
   - Sets standard values (PaymentStatus = 'Completed', SourceSystem = 'raw_premiums')

4. **Record Deduplication**
   - Uses ROW_NUMBER for sequential ID assignment
   - Maintains chronological ordering by Policy and DatePost

### Script Responsible
- **Primary Script**: `sql/transforms/10-premium-transactions.sql`
- **Pipeline Integration**: Executed as step 11 in `scripts/run-pipeline.ts`
- **Order**: Runs after policies, before audit and cleanup

### Output Tables
- `etl.stg_premium_transactions` - Individual premium payment records with billing periods

## Audit & Cleanup

### Data Source
- **Source Tables**: All staging tables (stg_*)
- **Raw Data**: Complete ETL output for validation and cleanup
- **Volume**: All transformed records across the entire pipeline

### Transformation Process

1. **Referential Integrity Validation**
   - Checks orphaned records across all relationships
   - Validates foreign key constraints between tables
   - Identifies data quality issues and missing links

2. **Data Completeness Assessment**
   - Verifies coverage percentages for critical fields
   - Checks for null values in required columns
   - Validates business rule compliance

3. **Automated Cleanup Operations**
   - Applies fixes for identified data issues
   - Updates missing references where possible
   - Normalizes inconsistent data values

4. **Final Data Quality Reporting**
   - Generates comprehensive statistics on ETL success
   - Reports error rates and data quality metrics
   - Provides audit trail for compliance and troubleshooting

### Script Responsible
- **Primary Script**: `sql/transforms/99-audit-and-cleanup.sql`
- **Pipeline Integration**: Executed as step 12 in `scripts/run-pipeline.ts`
- **Order**: Runs after all transforms, before export phase

### Output Tables
- **No new tables created** - validates and cleans existing staging data
- **Reports**: Comprehensive data quality metrics and audit findings

## Production Deployment

### Data Source
- **Source Schema**: `etl` (staging tables - stg_*)
- **Target Schema**: `dbo` (production database)
- **Volume**: Complete transformed dataset (~400K+ records across all entities)

### Deployment Process

1. **Production Data Clearance** (`sql/export/00-clear-production.sql`)
   - **DESTRUCTIVE**: Deletes ALL existing production data
   - Clears commission data, policies, hierarchies, and assignments
   - Maintains proper foreign key dependency order (reverse of creation)
   - Requires backup confirmation before execution

2. **Entity-by-Entity Export** (20+ export scripts)
   - **Brokers**: `02-export-brokers.sql` - Exports broker master data and licenses
   - **Groups**: `05-export-groups.sql` - Exports employer groups with PrimaryBrokerId
   - **Products & Plans**: `06-export-products.sql`, `06a-export-plans.sql` - Product catalog
   - **Schedules**: `01-export-schedules.sql` - Commission schedules and rate structures
   - **Proposals**: `07-export-proposals.sql` - Proposal agreements and assignments
   - **Hierarchies**: `08-export-hierarchies.sql` - Broker hierarchy chains
   - **Policies**: `09-export-policies.sql` - Individual policy records
   - **Premium Transactions**: `10-export-premium-transactions.sql` - Billing data
   - **Splits & Assignments**: Multiple export scripts for commission distributions

3. **Data Validation & Linking**
   - Links proposals to hierarchies and broker assignments
   - Establishes foreign key relationships across all entities
   - Validates referential integrity before completion
   - Generates audit trails for compliance

4. **Completion Verification**
   - Runs final data quality checks on production tables
   - Verifies record counts match staging expectations
   - Confirms all relationships are properly established

### Script Responsible
- **Primary Pipeline**: `scripts/run-pipeline.ts` (full run includes export phase)
- **Export-Only Mode**: `scripts/run-pipeline.ts --export-only` (skip ingest/transform)
- **Individual Exports**: 20+ export scripts in `sql/export/` directory
- **Order**: Runs after complete ETL transformation and audit phases

### Output Tables
- **Production Tables**: `dbo.*` - Live production database with all entities
- **Commission System Ready**: Complete data set for commission calculations
- **Audit Trail**: Full deployment history with record counts and validation results

### Key Considerations
- **Backup Required**: Always backup production data before deployment
- **Destructive Operation**: Export phase clears all existing production data
- **Validation Critical**: Each export includes data quality checks
- **Resume Capability**: Pipeline can resume from failed export steps
- **State Tracking**: All deployment steps tracked in ETL state management tables