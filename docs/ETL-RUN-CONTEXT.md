# ETL Pipeline Execution Context Guide

**Purpose:** Complete guide for running the full ETL pipeline from start to finish.

**Reference:** Based on `docs/simple-data-flow.md` - see that document for detailed data flow architecture.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Configuration Setup](#configuration-setup)
3. [Execution Modes](#execution-modes)
4. [Running the Full Pipeline](#running-the-full-pipeline)
5. [Pipeline Phases Overview](#pipeline-phases-overview)
6. [Verification & Testing](#verification--testing)
7. [Troubleshooting](#troubleshooting)
8. [Quick Reference Commands](#quick-reference-commands)

---

## Prerequisites

### 1. Database Access
- SQL Server connection configured and accessible
- Permissions to create/drop schemas (`etl`, `prestage`, `dbo`)
- Source data available in one of:
  - `poc_etl` schema (legacy POC data)
  - `new_data` schema (production source data)
  - CSV files ready for ingestion

### 2. Node.js Environment
```bash
# Verify Node.js is installed
node --version  # Should be v16+ or v18+

# Install dependencies
npm install
```

### 3. Source Data Requirements
- **Certificates**: ~1.5-1.7M certificate records
- **Brokers**: Individual + Organization rosters
- **Schedules**: Commission schedule rates (~600-700 schedules)
- **Premiums**: Premium transaction records
- **Supporting Data**: Licenses, E&O insurance, fees, etc.

### 4. Configuration File
Copy `appsettings.example.json` to `appsettings.json` and configure:
```bash
cp appsettings.example.json appsettings.json
# Edit appsettings.json with your database credentials
```

---

## Configuration Setup

### Database Configuration (`appsettings.json`)

```json
{
  "database": {
    "connectionString": "Server=YOUR_SERVER;Database=YOUR_DB;User Id=USER;Password=PASSWORD;TrustServerCertificate=True;Encrypt=True;",
    "schemas": {
      "source": "new_data",        // Source schema (poc_etl or new_data)
      "transition": "raw_data",   // Transitional schema
      "processing": "etl",        // ETL working schema
      "production": "dbo"          // Production target schema
    },
    "pocMode": false              // Set true if using poc_etl schema
  }
}
```

### Environment Variables (Alternative)
```bash
export SQLSERVER_HOST=your-server.database.windows.net
export SQLSERVER_DATABASE=your-database
export SQLSERVER_USER=your-username
export SQLSERVER_PASSWORD=your-password
```

### Debug Mode (Optional)
Enable debug mode to limit records for testing:
```json
{
  "debugMode": {
    "enabled": true,
    "maxRecords": {
      "brokers": 100,
      "groups": 50,
      "policies": 1000,
      "premiums": 5000,
      "hierarchies": 100,
      "proposals": 50
    }
  }
}
```

---

## Execution Modes

### Mode 1: Auto Mode (Production)
Runs continuously without pauses. Best for production runs.

```bash
npx tsx scripts/run-pipeline.ts
```

**Features:**
- ✅ Runs all phases automatically
- ✅ Shows progress logs
- ✅ State management (can resume if fails)
- ❌ No pauses between steps

### Mode 2: Step-by-Step Mode (Testing/Validation)
Pauses after each step for verification. Best for first-time setup and testing.

```bash
npx tsx scripts/run-pipeline.ts --step-by-step
```

**Features:**
- ✅ Pauses after each step
- ✅ Shows verification results
- ✅ Can abort at any step
- ✅ State management (can resume later)

### Mode 3: Phase-Specific Execution

**Transforms Only** (data already loaded):
```bash
npx tsx scripts/run-pipeline.ts --transforms-only
# Alias for: --skip-ingest --skip-export
```

**Export Only** (transforms already complete):
```bash
npx tsx scripts/run-pipeline.ts --export-only
# Alias for: --skip-schema --skip-ingest --skip-transform
```

**Skip Specific Phases:**
```bash
npx tsx scripts/run-pipeline.ts --skip-schema    # Skip schema setup
npx tsx scripts/run-pipeline.ts --skip-ingest    # Skip data ingestion
npx tsx scripts/run-pipeline.ts --skip-transform # Skip transforms
npx tsx scripts/run-pipeline.ts --skip-export    # Skip export to production
```

---

## Running the Full Pipeline

### Recommended: First-Time Setup (Step-by-Step)

```bash
# 1. Verify configuration
cat appsettings.json

# 2. Run with step-by-step verification
npx tsx scripts/run-pipeline.ts --step-by-step

# 3. At each pause, verify the step completed successfully
#    - Review verification output
#    - Run suggested test queries if needed
#    - Type 'y' to continue or 'n' to stop
```

### Production Run (Fully Automated)

```bash
# Full pipeline execution
npx tsx scripts/run-pipeline.ts

# With TypeScript proposal builder (recommended)
npx tsx scripts/run-pipeline.ts --use-ts-builder

# With specific config file
npx tsx scripts/run-pipeline.ts --config appsettings.poc.json
```

### Resuming Failed Runs

```bash
# Resume from last failed run
npx tsx scripts/run-pipeline.ts --resume

# Resume from specific run ID
npx tsx scripts/run-pipeline.ts --resume-from <run-id>
```

---

## Pipeline Phases Overview

### Phase 1: Schema Setup
**Duration:** ~30-60 seconds  
**Scripts:** 5-7 SQL scripts (depends on POC mode)

**What it does:**
- Creates/resets `etl` schema
- Creates `raw_*`, `input_*`, `stg_*` table structures
- Sets up `prestage` schema for consolidation audit
- Initializes state management tables

**Key Tables Created:**
- `etl.raw_*` - Raw data tables
- `etl.input_*` - Input staging tables
- `etl.stg_*` - Staging tables
- `prestage.*` - Pre-stage consolidation tables

**Verification:**
```sql
SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME LIKE 'raw_%';
-- Should show ~10+ raw tables
```

---

### Phase 2: Data Ingestion
**Duration:** ~60-120 seconds  
**Scripts:** 2 SQL scripts

**What it does:**
1. **Copy from Source** (`copy-from-poc-etl.sql`):
   - Copies all raw tables from source schema (`poc_etl` or `new_data`) to `etl.raw_*`
   - Bulk INSERT operations with row count verification
   - Handles ~1.5-1.7M certificates + supporting data

2. **Populate Input Tables** (`populate-input-tables.sql`):
   - Transforms `raw_*` tables into structured `input_*` tables
   - Processes certificate data with proper typing and validation
   - Prepares data for transform phase consumption

**Key Tables Populated:**
- `etl.raw_certificate_info` (~1.5-1.7M records)
- `etl.raw_premiums` (~138K records)
- `etl.raw_schedule_rates` (~1.1M records)
- `etl.input_certificate_info` (~1.5-1.7M records)

**Verification:**
```sql
-- Check certificate count
SELECT COUNT(*) FROM [etl].[raw_certificate_info];
-- Expected: ~1.5-1.7M

-- Check input table population
SELECT COUNT(*) FROM [etl].[input_certificate_info];
-- Expected: ~1.5-1.7M
```

---

### Phase 3: Data Transforms
**Duration:** ~5-15 minutes (varies by data size)  
**Scripts:** 12+ SQL scripts + optional TypeScript builder

**What it does:**
Transforms raw/input data into staging tables through multiple steps:

1. **Reference Data** (`00-references.sql`):
   - Builds reference lookup tables

2. **Core Entities:**
   - **Brokers** (`01-brokers.sql`): Individual + Organization brokers, licenses, E&O
   - **Groups** (`02-groups.sql`): Employer groups with PrimaryBrokerId
   - **Products** (`03-products.sql`): Product catalog and plan codes
   - **Schedules** (`04-schedules.sql`): Commission schedules and rate structures ⚠️ **CRITICAL**

3. **Proposal Building** (Choose one):
   - **TypeScript Builder** (`--use-ts-builder` flag): Recommended, generates all 9 staging entities
   - **SQL Scripts** (default): Legacy approach using 06a-06e scripts

4. **Hierarchies** (`07-hierarchies.sql`):
   - Creates broker hierarchy chains with commission splits ⚠️ **CRITICAL**

5. **Conformance Analysis** (`08-analyze-conformance.sql`):
   - Analyzes group conformance for export filtering

6. **Policies** (`09-policies.sql`):
   - Creates policy records linked to proposals/groups

7. **Premium Transactions** (`10-premium-transactions.sql`):
   - Transforms premium payment records

8. **Audit & Cleanup** (`99-audit-and-cleanup.sql`):
   - Final data quality audit and cleanup

**Key Output Tables:**
- `etl.stg_brokers` (~12K records)
- `etl.stg_groups` (~33K records)
- `etl.stg_schedules` (~600-700 records) ⚠️ **Must be > 0**
- `etl.stg_proposals` (~400K+ records)
- `etl.stg_hierarchies` (~81K records)
- `etl.stg_policies` (~415K records)

**Critical Verification Points:**

```sql
-- 1. Schedules MUST exist (if 0 = FAIL!)
SELECT COUNT(*) FROM [etl].[stg_schedules];
-- Expected: ~600-700, if 0 = CRITICAL FAILURE

-- 2. Hierarchies with schedule links
SELECT 
  COUNT(*) AS total_hierarchies,
  SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) AS with_schedules
FROM [etl].[stg_hierarchy_participants];
-- Expected: High percentage with schedules

-- 3. Policies linked to proposals
SELECT 
  COUNT(*) AS total_policies,
  SUM(CASE WHEN ProposalId IS NOT NULL THEN 1 ELSE 0 END) AS with_proposals
FROM [etl].[stg_policies];
-- Expected: ~378K conformant policies with proposals
```

---

### Phase 4: Export to Production
**Duration:** ~2-5 minutes  
**Scripts:** 20+ export scripts

**What it does:**
Exports staging data to production `dbo` schema with conformance filtering.

**Export Order:**
1. Brokers & Licenses
2. Groups (filtered by conformance)
3. Products & Plans
4. Schedules & Schedule Rates
5. Proposals (filtered by conformance)
6. Hierarchies (filtered by conformance)
7. Policies (filtered by conformance)
8. Premium Transactions
9. Policy Hierarchy Assignments (non-conformant policies)
10. Commission Assignments
11. Additional entities (fees, banking info, etc.)

**⚠️ WARNING:** Export phase is **DESTRUCTIVE** - clears all existing production data!

**Verification:**
```sql
-- Check production table counts
SELECT 'Brokers' as tbl, COUNT(*) as cnt FROM [dbo].[Brokers]
UNION ALL SELECT 'EmployerGroups', COUNT(*) FROM [dbo].[EmployerGroups]
UNION ALL SELECT 'Policies', COUNT(*) FROM [dbo].[Policies]
UNION ALL SELECT 'Proposals', COUNT(*) FROM [dbo].[Proposals]
UNION ALL SELECT 'Hierarchies', COUNT(*) FROM [dbo].[Hierarchies]
UNION ALL SELECT 'Schedules', COUNT(*) FROM [dbo].[Schedules];

-- Expected counts:
-- Brokers: ~12K
-- Groups: ~33K (conformant + nearly conformant)
-- Policies: ~415K (conformant + non-conformant)
-- Proposals: ~400K+
-- Hierarchies: ~81K
-- Schedules: ~600-700
```

---

## Verification & Testing

### Quick Verification Queries

**After Ingest Phase:**
```sql
-- Verify data copied successfully
SELECT 
  'raw_certificate_info' as table_name, COUNT(*) as row_count 
FROM [etl].[raw_certificate_info]
UNION ALL
SELECT 'raw_premiums', COUNT(*) FROM [etl].[raw_premiums]
UNION ALL
SELECT 'input_certificate_info', COUNT(*) FROM [etl].[input_certificate_info];
```

**After Transform Phase:**
```sql
-- Critical checks
SELECT COUNT(*) as schedule_count FROM [etl].[stg_schedules];
-- MUST be > 0 (600-700 expected)

SELECT 
  COUNT(*) as total_policies,
  SUM(CASE WHEN ProposalId IS NOT NULL THEN 1 ELSE 0 END) as with_proposals,
  SUM(CASE WHEN ProposalId IS NULL THEN 1 ELSE 0 END) as non_conformant
FROM [etl].[stg_policies];

-- Hierarchy schedule linkage
SELECT 
  CAST(SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) 
  AS schedule_link_percentage
FROM [etl].[stg_hierarchy_participants];
-- Should be high (>80%)
```

**After Export Phase:**
```sql
-- Production data verification
SELECT 
  'Brokers' as entity, COUNT(*) as count FROM [dbo].[Brokers]
UNION ALL SELECT 'Groups', COUNT(*) FROM [dbo].[EmployerGroups]
UNION ALL SELECT 'Policies', COUNT(*) FROM [dbo].[Policies]
UNION ALL SELECT 'Proposals', COUNT(*) FROM [dbo].[Proposals]
UNION ALL SELECT 'Hierarchies', COUNT(*) FROM [dbo].[Hierarchies]
UNION ALL SELECT 'Schedules', COUNT(*) FROM [dbo].[Schedules];
```

### Comprehensive Testing

**Step-by-Step Test Guide:**
```bash
# Run dedicated step-by-step scripts with rich verification
npx tsx scripts/run-ingest-step-by-step.ts      # Ingest phase only
npx tsx scripts/run-transforms-step-by-step.ts   # Transform phase only
```

**Certificate Resolution Validation** (if using TypeScript builder):
```bash
# Small sample (20 certificates, quick check)
npm run validate-certificates -- --sample small

# Medium sample (200 certificates, comprehensive)
npm run validate-certificates -- --sample medium

# Large sample (1000 certificates, statistical confidence)
npm run validate-certificates -- --sample large
```

**Success Criteria:** Pass rate >= 95%

---

## Troubleshooting

### Common Issues

#### 1. "No schedules found" (stg_schedules count = 0)
**Symptom:** `stg_schedules` table is empty after transform phase.

**Cause:** Schedule rates not imported or source data missing.

**Solution:**
```sql
-- Check raw schedule data
SELECT COUNT(*) FROM [etl].[raw_schedule_rates];
-- Should be ~1.1M records

-- Check source schema
SELECT COUNT(*) FROM [new_data].[ScheduleRates] 
-- OR
SELECT COUNT(*) FROM [poc_etl].[raw_schedule_rates];
```

**Fix:** Ensure schedule rates are loaded in source schema before running pipeline.

---

#### 2. "Pipeline failed, can I resume?"
**Symptom:** Pipeline failed mid-execution.

**Solution:**
```bash
# Check last run status
# (State is tracked in database)

# Resume from last failure
npx tsx scripts/run-pipeline.ts --resume

# Or resume from specific run ID
npx tsx scripts/run-pipeline.ts --resume-from <run-id>
```

**Note:** Resume capability depends on error type. Some errors (schema issues) may require full restart.

---

#### 3. "Connection timeout" or "Database connection failed"
**Symptom:** Cannot connect to SQL Server.

**Solution:**
1. Verify connection string in `appsettings.json`
2. Check network connectivity
3. Verify SQL Server is running and accessible
4. Check firewall rules
5. Verify credentials

**Test Connection:**
```bash
# Test with sqlcmd (if available)
sqlcmd -S YOUR_SERVER -d YOUR_DB -U USER -P PASSWORD -Q "SELECT 1"
```

---

#### 4. "Schema already exists" error
**Symptom:** Schema creation fails because schema already exists.

**Solution:**
```bash
# Option 1: Drop schema manually (DESTRUCTIVE)
sqlcmd -S SERVER -d DB -Q "DROP SCHEMA IF EXISTS [etl]"

# Option 2: Skip schema setup (if schema is already correct)
npx tsx scripts/run-pipeline.ts --skip-schema
```

---

#### 5. "No proposals created" or "Low proposal count"
**Symptom:** `stg_proposals` has very few records.

**Possible Causes:**
- Certificate data not loaded correctly
- GroupId issues (null/invalid groups)
- Proposal builder failed silently

**Solution:**
```sql
-- Check certificate data
SELECT COUNT(*) FROM [etl].[input_certificate_info];
-- Should be ~1.5-1.7M

-- Check for invalid groups
SELECT 
  COUNT(*) as total,
  SUM(CASE WHEN GroupId IS NULL OR GroupId = '' OR GroupId = '00000' THEN 1 ELSE 0 END) as invalid_groups
FROM [etl].[input_certificate_info];

-- Check proposal builder output (if using TypeScript)
SELECT COUNT(*) FROM [etl].[stg_proposals];
```

---

#### 6. "Export phase clears production data"
**Symptom:** Production tables are empty after export.

**Expected Behavior:** Export phase is **DESTRUCTIVE** by design. It clears all production data before exporting.

**Prevention:**
- Always backup production data before running export
- Use `--skip-export` flag if you only want to test transforms
- Verify staging data before running export phase

---

### Debug Mode

Enable debug mode to limit records for faster testing:

```bash
npx tsx scripts/run-pipeline.ts --debug
```

This limits:
- Brokers: 100
- Groups: 50
- Policies: 1000
- Premiums: 5000
- Hierarchies: 100
- Proposals: 50

---

## Quick Reference Commands

### Most Common Commands

```bash
# First-time setup (with verification)
npx tsx scripts/run-pipeline.ts --step-by-step

# Production run (fully automated)
npx tsx scripts/run-pipeline.ts

# Production run with TypeScript builder (recommended)
npx tsx scripts/run-pipeline.ts --use-ts-builder

# Run only transforms (data already loaded)
npx tsx scripts/run-pipeline.ts --transforms-only

# Run only export (transforms already complete)
npx tsx scripts/run-pipeline.ts --export-only

# Resume failed run
npx tsx scripts/run-pipeline.ts --resume

# Debug mode (limited records)
npx tsx scripts/run-pipeline.ts --debug
```

### Phase-Specific Scripts

```bash
# Ingest phase only (with verification)
npx tsx scripts/run-ingest-step-by-step.ts

# Transform phase only (with verification)
npx tsx scripts/run-transforms-step-by-step.ts

# Standalone ingest (quick, no pauses)
npx tsx scripts/copy-poc-etl-to-etl.ts
```

### Configuration

```bash
# Use specific config file
npx tsx scripts/run-pipeline.ts --config appsettings.poc.json

# View current configuration
cat appsettings.json
```

---

## Pipeline Execution Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    START: Full ETL Pipeline                  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 1: Schema Setup                                      │
│  • Create/reset etl schema                                  │
│  • Create raw_*, input_*, stg_* tables                      │
│  • Setup prestage schema                                    │
│  Duration: ~30-60 seconds                                   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 2: Data Ingestion                                    │
│  • Copy from source schema (poc_etl/new_data)              │
│  • Populate input_* tables                                  │
│  Duration: ~60-120 seconds                                   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 3: Data Transforms                                   │
│  • Reference data                                           │
│  • Brokers, Groups, Products, Schedules                    │
│  • Proposal building (TypeScript or SQL)                   │
│  • Hierarchies, Policies, Premiums                         │
│  • Audit & cleanup                                          │
│  Duration: ~5-15 minutes                                    │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 4: Export to Production                              │
│  ⚠️ DESTRUCTIVE: Clears all production data                  │
│  • Export all staging tables to dbo.*                       │
│  • Apply conformance filtering                             │
│  Duration: ~2-5 minutes                                     │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    COMPLETE: Production Ready                │
└─────────────────────────────────────────────────────────────┘
```

---

## Additional Resources

- **Data Flow Details:** `docs/simple-data-flow.md` - Complete data flow architecture
- **Execution Modes:** `EXECUTION-MODES-GUIDE.md` - Detailed execution mode documentation
- **Quick Reference:** `QUICK-REFERENCE.md` - Command cheat sheet
- **Testing Guide:** `docs/TESTING-GUIDE.md` - Comprehensive testing instructions
- **Step-by-Step Guide:** `STEP-BY-STEP-TEST-GUIDE.md` - Detailed verification queries

---

## Support & Next Steps

After successful pipeline execution:

1. **Verify Production Data:** Run verification queries above
2. **Check Commission Calculations:** Ensure policies are linked to hierarchies and schedules
3. **Review Audit Output:** Check `99-audit-and-cleanup.sql` output for data quality metrics
4. **Trace Policy to Schedule:** Use policy tracing guide (`policy-tracing.mdc`) to verify end-to-end links

**Ready to calculate commissions!** 🎉
