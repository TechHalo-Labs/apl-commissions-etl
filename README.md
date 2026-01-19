# v5-etl - SQL Server ETL Pipeline

SQL Server-based ETL pipeline for APL Commissions calculation, replacing the previous ClickHouse implementation.

## Architecture

```
CSV Files → SQL Server [etl] Schema → 8-Stage Calculation → [dbo] Production
```

### Key Features
- **No intermediate database**: All processing happens in SQL Server
- **8-stage cascading calculation pipeline**: Each stage is a simple `INSERT INTO...SELECT FROM`
- **Transferee bug fix**: Correctly handles self-payments (`BrokerId === PaidBrokerId`)
- **Full traceability**: One traceability report per premium, one broker traceability per GL entry

## Directory Structure

```
v5-etl/
├── sql/
│   ├── 00-schema-setup.sql          # Create/reset etl schema
│   ├── 01-raw-tables.sql            # Raw table definitions
│   ├── 02-input-tables.sql          # Input table population
│   ├── 03-staging-tables.sql        # Staging table definitions
│   ├── bulk-insert.sql              # Template for BULK INSERT
│   ├── transforms/
│   │   ├── 00-references.sql        # Build reference tables
│   │   ├── 01-brokers.sql           # Transform brokers
│   │   └── 07-hierarchies.sql       # Transform hierarchies (WITH BUG FIX)
│   ├── calc/
│   │   ├── 00-calc-tables.sql       # Calculation table definitions
│   │   └── run-calculation.sql      # 8-stage calculation pipeline
│   └── export/
│       └── 01-export-to-dbo.sql     # Export to production
├── scripts/
│   └── run-pipeline.ts              # TypeScript orchestrator
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

