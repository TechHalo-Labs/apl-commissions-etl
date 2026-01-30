# v5-etl - SQL Server ETL Pipeline

SQL Server-based ETL pipeline for APL Commissions calculation, replacing the previous ClickHouse implementation.

## Architecture

```
CSV Files → SQL Server [etl] Schema → TypeScript Builder → Stage → 8-Stage Calculation → [dbo] Production
```

### Key Features
- **No intermediate database**: All processing happens in SQL Server
- **TypeScript proposal builder**: Generates all 9 staging entities from certificates (NEW)
- **Full SHA256 hashing**: 64-char hashes with collision detection for data integrity
- **Batched processing**: Handle 400K+ certificates efficiently with configurable batch sizes
- **Certificate resolution validation**: Random sampling to verify correctness against source data
- **8-stage cascading calculation pipeline**: Each stage is a simple `INSERT INTO...SELECT FROM`
- **Transferee bug fix**: Correctly handles self-payments (`BrokerId === PaidBrokerId`)
- **Full traceability**: One traceability report per premium, one broker traceability per GL entry

## TypeScript Proposal Builder (Recommended)

The new TypeScript-based proposal builder replaces the complex SQL scripts (06a-06e) with a clean, maintainable TypeScript implementation that generates all 9 staging entity types.

### Quick Start

```bash
# Run pipeline with TypeScript builder
npm run pipeline:ts

# Or with explicit flag
npx tsx scripts/run-pipeline.ts --use-ts-builder

# Run builder standalone
npm run build-proposals

# Dry run mode (no database writes)
npm run build-proposals:dry

# With certificate limit for testing
npx tsx scripts/proposal-builder.ts --limit 100 --verbose
```

### What It Generates

The TypeScript builder generates all 9 staging entity types from `input_certificate_info`:

1. **stg_proposals** - Commission agreements (deduplicated by GroupId + ConfigHash)
2. **stg_proposal_key_mapping** - Lookup table for certificate → proposal resolution
3. **stg_premium_split_versions** - Premium split configurations
4. **stg_premium_split_participants** - Split participants (links to hierarchies)
5. **stg_hierarchies** - Hierarchy containers (deduplicated by hierarchy hash)
6. **stg_hierarchy_versions** - Time-versioned hierarchy structures
7. **stg_hierarchy_participants** - Brokers in hierarchy chains
8. **stg_policy_hierarchy_assignments** - Non-conformant policy assignments (invalid groups)
9. **stg_policy_hierarchy_participants** - Embedded participants for PHA

### Features

- **Active Certificate Filter**: Only processes `CertStatus='A' AND RecStatus='A'` (~150K certificates)
- **Full SHA256 Hashing**: Uses complete 64-character hashes (not truncated) for ConfigHash
- **Collision Detection**: Built-in collision detection prevents silent data corruption
- **Batched Processing**: Optional batching for memory efficiency with large datasets
- **Audit Logging**: Structured JSON logs for operation tracking
- **Dry Run Mode**: Test without writing to database
- **Verbose Mode**: Detailed progress reporting

### CLI Options

```bash
--limit <n>         # Process only first N certificates (for testing)
--batch-size <n>    # Enable batched mode with batch size (default: process all)
--dry-run           # Simulate without database writes
--verbose           # Show detailed progress
--schema <name>     # Target schema (default: 'etl')
```

### Validation

After running the builder, validate certificate resolution:

```bash
# Small sample (20 certificates, quick check)
npm run validate-certificates -- --sample small

# Medium sample (200 certificates, comprehensive)
npm run validate-certificates -- --sample medium

# Large sample (1000 certificates, statistical confidence)
npm run validate-certificates -- --sample large
```

**Success Criteria:** Pass rate >= 95%

See `docs/TESTING-GUIDE.md` for complete testing instructions.

### SQL Builder (Legacy)

The original SQL-based proposal generation (scripts 06a-06e) is still available as a fallback:

```bash
# Run pipeline with SQL builder (default)
npm run pipeline

# Or explicitly without TypeScript builder
npx tsx scripts/run-pipeline.ts
```

**Note:** The SQL builder will be deprecated after the TypeScript builder is validated in production.

## Proposal Consolidation

### Two-Phase Consolidation Approach

The ETL uses a transparent two-phase consolidation process:

1. **Pre-Stage Phase**: All proposals are retained in granular form with full split configuration JSON
2. **Consolidation Phase**: TypeScript algorithm consolidates proposals in-memory using explicit rules

### Pre-Stage Schema

Pre-stage tables (schema: `prestage`) retain unconsolidated proposals for audit purposes:

- `prestage_proposals` - All granular proposals with `SplitConfigurationJSON` and `SplitConfigurationMD5`
- `prestage_hierarchies` - Unconsolidated hierarchies
- `prestage_hierarchy_versions` - Unconsolidated hierarchy versions
- `prestage_hierarchy_participants` - Unconsolidated hierarchy participants
- `prestage_premium_split_versions` - Unconsolidated split versions
- `prestage_premium_split_participants` - Unconsolidated split participants

### Consolidation Rules

1. **Different GroupId** → Close retained proposal, start new
2. **Different SplitConfigurationMD5** → Close retained proposal, start new
3. **Conflicting PlanCode** → Close retained proposal, start new
4. **Same configuration** → Extend date range (including non-contiguous gaps), accumulate product codes

### Split Configuration JSON

Each proposal includes a comprehensive split configuration JSON with full hierarchy details:

```json
{
  "totalSplitPercent": 100.0,
  "splits": [
    {
      "splitPercent": 60.0,
      "hierarchyId": "H-G25992-1",
      "hierarchy": {
        "hierarchyId": "H-G25992-1",
        "hierarchyName": "Main Hierarchy",
        "participants": [
          {
            "level": 1,
            "brokerId": 12345,
            "brokerName": "John Doe",
            "splitPercent": 50.0,
            "commissionRate": 15.0,
            "scheduleCode": "RZ4",
            "scheduleId": 789,
            "scheduleName": "Standard Rates"
          }
        ]
      }
    }
  ]
}
```

### Audit Trail

Query to see consolidation for a group:

```sql
SELECT 
  Id, IsRetained, ConsumedByProposalId, ConsolidationReason,
  DateRangeFrom, DateRangeTo, ProductCodes
FROM [prestage].[prestage_proposals]
WHERE GroupId = 'G12345'
ORDER BY IsRetained DESC, DateRangeFrom;
```

### Running Consolidation

```bash
# Full pipeline (includes consolidation)
npx tsx scripts/run-pipeline.ts

# Verify consolidation results
npx tsx scripts/verify-consolidation.ts
```

### Schema Lifecycle

The `prestage` schema is retained during ETL audit phase and dropped when moving to production:

```bash
# When ready for production, drop pre-stage schema
sqlcmd -S server -d database -i sql/99-drop-prestage-schema.sql
```

## Group Conformance & Export Filtering

The ETL pipeline includes **automatic conformance analysis** to ensure data quality:

### Conformance Levels

| Level | Criteria | Export |
|-------|----------|--------|
| **Conformant** | 100% of certificates map to exactly one proposal | ✅ **Exported** |
| **Nearly Conformant** | >=95% of certificates map correctly | ✅ **Exported** |
| **Non-Conformant** | <95% conformance | ❌ **Skipped** |

### How It Works

1. **Analysis Phase** (`08-analyze-conformance.sql`):
   - Deduplicates certificates across source tables
   - Maps each certificate to proposals via (GroupId, Year, Product, PlanCode)
   - Calculates conformance percentage per group
   - Stores results in `GroupConformanceStatistics` table

2. **Export Filtering** (all export scripts):
   - Groups, Proposals, Hierarchies, Policies filter by conformance
   - Only conformant + nearly conformant groups are exported
   - Direct-to-Consumer (DTC) policies with NULL GroupId are always exported

### View Conformance Statistics

```sql
SELECT 
    GroupClassification,
    COUNT(*) AS GroupCount,
    SUM(TotalCertificates) AS TotalCerts,
    AVG(ConformancePercentage) AS AvgConformance
FROM [etl].[GroupConformanceStatistics]
GROUP BY GroupClassification
ORDER BY GroupClassification;
```

## Directory Structure

```
v5-etl/
├── sql/
│   ├── 00-schema-setup.sql          # Create/reset etl schema
│   ├── 01-raw-tables.sql            # Raw table definitions
│   ├── 02-input-tables.sql          # Input table population
│   ├── 03-staging-tables.sql        # Staging table definitions
│   ├── 03a-prestage-tables.sql      # Pre-stage schema (consolidation audit)
│   ├── 03b-conformance-table.sql    # Conformance statistics table
│   ├── bulk-insert.sql              # Template for BULK INSERT
│   ├── transforms/
│   │   ├── 00-references.sql        # Build reference tables
│   │   ├── 01-brokers.sql           # Transform brokers
│   │   ├── 07-hierarchies.sql       # Transform hierarchies (WITH BUG FIX)
│   │   └── 08-analyze-conformance.sql # Group conformance analysis
│   ├── calc/
│   │   ├── 00-calc-tables.sql       # Calculation table definitions
│   │   └── run-calculation.sql      # 8-stage calculation pipeline
│   └── export/
│       └── 05-export-groups.sql     # Export to production (filtered by conformance)
├── scripts/
│   ├── run-pipeline.ts              # TypeScript orchestrator
│   └── transforms/
│       └── consolidate-proposals.ts # In-memory proposal consolidation
├── package.json
└── tsconfig.json
```

## Quick Start

```bash
cd tools/v5-etl

# Install dependencies
npm install

# Run full pipeline
npm run pipeline

# Or with options
npx tsx scripts/run-pipeline.ts --skip-ingest  # Skip CSV loading
npx tsx scripts/run-pipeline.ts --skip-calc    # Skip calculation
```

## Configuration

Set environment variables or modify defaults in `run-pipeline.ts`:

```bash
export SQLSERVER_HOST=halo-sql.database.windows.net
export SQLSERVER_DATABASE=halo-sqldb
export SQLSERVER_USER=***REMOVED***
export SQLSERVER_PASSWORD=your_password
export CSV_DATA_PATH=/path/to/csv/files
```

## 8-Stage Calculation Pipeline

| Stage | Table | Purpose | Rows |
|-------|-------|---------|------|
| 1 | `calc_1_premium_context` | Enrich with policy/group | 1:1 |
| 2 | `calc_2_proposals_resolved` | Match proposal | 1:1 |
| 3 | `calc_3_splits_applied` | Explode by split % | 1:N |
| 4 | `calc_4_hierarchies_resolved` | Find hierarchy | 1:1 |
| 5 | `calc_5_participants_expanded` | Explode by tier | 1:N |
| 6 | `calc_6_rates_applied` | Lookup rate | 1:1 |
| 7 | `calc_7_commissions_calculated` | Calculate amount | 1:1 |
| 8 | `calc_8_assignments_applied` | Apply assignments | 1:1 |

## Critical Bug Fix: Transferee Exclusion

The hierarchy transform (`07-hierarchies.sql`) includes a critical fix for the transferee exclusion logic:

**Problem**: When `BrokerId === PaidBrokerId` (self-payment), the broker was incorrectly excluded as a "transferee".

**Fix**: A broker is only excluded if ALL of these are true:
1. They appear as `PaidBrokerId`
2. `ReassignedType` is 'Transferred' or 'Assigned'
3. `PaidBrokerId != SplitBrokerId` (NOT a self-payment)
4. `PaidBrokerId` is NOT also a `SplitBrokerId` for the same cert/split

```sql
-- Step 1a: Potential transferees EXCLUDE self-payments
SELECT ...
WHERE ci.PaidBrokerId <> ci.SplitBrokerId;  -- CRITICAL

-- Step 1c: TRUE transferees = NOT ALSO earners
SELECT pt.*
FROM #tmp_potential_transferees pt
WHERE NOT EXISTS (
    SELECT 1 FROM #tmp_all_earners ae
    WHERE ae.SplitBrokerId = pt.TransfereeBrokerId
);
```

## Output

The pipeline generates:
- `calc_gl_journal_entries`: Commission GL entries (Original + Assigned)
- `calc_traceability`: One report per premium (success + failure)
- `calc_broker_traceabilities`: One entry per GL entry

## Manual SQL Execution

For testing individual steps:

```bash
# Connect to SQL Server
sqlcmd -S halo-sql.database.windows.net -d halo-sqldb -U ***REMOVED*** -P password

# Run schema setup
:r sql/00-schema-setup.sql

# Run transforms
:r sql/transforms/07-hierarchies.sql

# Run calculation
:r sql/calc/run-calculation.sql
```

