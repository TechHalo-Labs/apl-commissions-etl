# Complete Data Flow: From CSV Files to Production Database

## 🎯 Quick Answer

The `poc_etl.raw_*` tables are populated by **CSV ingestion scripts** that load data from the file system. Here's the complete flow:

```
CSV Files (Legacy LION System)
         ↓
   [CSV INGEST SCRIPTS]
         ↓
poc_etl schema (raw_* tables) ← YOU ARE HERE
         ↓
   [ETL PIPELINE - INGEST PHASE]
         ↓
etl schema (raw_* tables)
         ↓
   [ETL PIPELINE - TRANSFORM PHASE]
         ↓
etl schema (stg_* tables)
         ↓
   [ETL PIPELINE - EXPORT PHASE]
         ↓
dbo schema (production tables)
```

---

## 📊 Complete Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                         STEP 0: SOURCE DATA                          │
│                    (Legacy LION System Exports)                      │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                 CSV Files in ZIP Archive or Directory:
                    - CertificateInfo.csv (1.5M rows)
                    - premiums.csv (138K rows)
                    - perf.csv (Schedule rates - 1.1M rows)
                    - perf-group.csv (33K rows)
                    - IndividualRosterExtract.csv (Brokers)
                    - OrganizationRosterExtract.csv (Brokers)
                    - BrokerLicenseExtract.csv
                    - BrokerEO.csv
                    - Fees.csv
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│           STEP 1: CSV INGESTION (File System → Database)             │
│                                                                       │
│  Scripts:                                                            │
│    • scripts/ingest-raw-data.ts (ZIP-based, full-featured)          │
│    • scripts/load-csv.ts (Direct directory loader)                  │
│                                                                       │
│  What happens:                                                       │
│    1. Extract ZIP file (or read CSV directory)                      │
│    2. Match CSV files to target tables by prefix                    │
│    3. Validate CSV column headers                                   │
│    4. Create schema (poc_etl, poc_raw_data, or raw_data1-N)        │
│    5. Create raw_* tables (all NVARCHAR columns)                    │
│    6. Bulk insert data (1000-5000 rows per batch)                   │
│    7. Verify row counts                                             │
│                                                                       │
│  Commands:                                                           │
│    npx tsx scripts/ingest-raw-data.ts                              │
│    npx tsx scripts/load-csv.ts                                      │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                 RESULT: poc_etl.raw_* Tables Populated               │
│                                                                       │
│  Tables created:                                                     │
│    [poc_etl].[raw_certificate_info]    - 1.5M rows                  │
│    [poc_etl].[raw_schedule_rates]      - 1.1M rows                  │
│    [poc_etl].[raw_perf_groups]         - 33K rows                   │
│    [poc_etl].[raw_premiums]            - 138K rows                  │
│    [poc_etl].[raw_individual_brokers]  - ~12K rows                  │
│    [poc_etl].[raw_org_brokers]         - ~500 rows                  │
│    [poc_etl].[raw_broker_licenses]     - Variable                   │
│    [poc_etl].[raw_broker_eo]           - Variable                   │
│    [poc_etl].[raw_fees]                - Variable                   │
│    [poc_etl].[raw_commissions_detail]  - Variable                   │
│                                                                       │
│  ✅ DATA IS NOW IN SQL SERVER                                       │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│       STEP 2: ETL PIPELINE - INGEST PHASE (Schema Copy)              │
│                                                                       │
│  Script: sql/ingest/copy-from-poc-etl.sql                           │
│                                                                       │
│  What happens:                                                       │
│    Copies ALL raw_* tables from [poc_etl] to [etl] schema          │
│    (This allows ETL to work in isolated [etl] workspace)            │
│                                                                       │
│  Example SQL:                                                        │
│    INSERT INTO [etl].[raw_certificate_info]                         │
│    SELECT * FROM [poc_etl].[raw_certificate_info];                  │
│                                                                       │
│  Command:                                                            │
│    npx tsx scripts/run-pipeline.ts                                  │
│    (Includes ingest phase by default)                               │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│               RESULT: etl.raw_* Tables Populated                     │
│                                                                       │
│  Tables:                                                             │
│    [etl].[raw_certificate_info]    - Copy of poc_etl data           │
│    [etl].[raw_schedule_rates]      - Copy of poc_etl data           │
│    [etl].[raw_perf_groups]         - Copy of poc_etl data           │
│    [etl].[raw_premiums]            - Copy of poc_etl data           │
│    ... (all raw_ tables copied)                                     │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│      STEP 3: ETL PIPELINE - INGEST PHASE (Input Table Population)   │
│                                                                       │
│  Script: sql/ingest/populate-input-tables.sql                       │
│                                                                       │
│  What happens:                                                       │
│    Populates input_* tables from raw_* tables                       │
│    (Cleans data, normalizes formats)                                │
│                                                                       │
│  Example:                                                            │
│    INSERT INTO [etl].[input_certificate_info]                       │
│    SELECT <cleaned columns>                                         │
│    FROM [etl].[raw_certificate_info];                               │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│        STEP 4: ETL PIPELINE - TRANSFORM PHASE (40+ Scripts)          │
│                                                                       │
│  Scripts: sql/transforms/00-references.sql through                  │
│           sql/transforms/99-audit-and-cleanup.sql                   │
│                                                                       │
│  What happens:                                                       │
│    - Build reference tables (States, Products)                      │
│    - Transform brokers (Individual + Org merge)                     │
│    - Transform groups (with PrimaryBrokerID)                        │
│    - Create schedules (from raw_schedule_rates)                     │
│    - Build proposals (multi-tiered)                                 │
│    - Create hierarchies (with ScheduleId linking)                   │
│    - Transform policies/certificates                                │
│    - Create premium transactions                                    │
│    - Build policy-hierarchy assignments                             │
│                                                                       │
│  Result: etl.stg_* tables (staging tables ready for export)         │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│           STEP 5: ETL PIPELINE - EXPORT PHASE                        │
│                                                                       │
│  Scripts: sql/export/01-export-brokers.sql through                  │
│           sql/export/12-export-policy-hierarchy-assignments.sql     │
│                                                                       │
│  What happens:                                                       │
│    Copies staging tables (etl.stg_*) to production (dbo.*)         │
│                                                                       │
│  Example:                                                            │
│    INSERT INTO [dbo].[Brokers] (Id, Name, Status, ...)             │
│    SELECT Id, Name, Status, ...                                     │
│    FROM [etl].[stg_brokers]                                         │
│    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[Brokers] ...);           │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    FINAL: Production Database                        │
│                                                                       │
│  Tables:                                                             │
│    [dbo].[Brokers]                                                  │
│    [dbo].[EmployerGroups]                                           │
│    [dbo].[Products]                                                 │
│    [dbo].[Schedules]                                                │
│    [dbo].[ScheduleRates]                                            │
│    [dbo].[Proposals]                                                │
│    [dbo].[Hierarchies]                                              │
│    [dbo].[HierarchyParticipants]                                    │
│    [dbo].[Policies]                                                 │
│    [dbo].[PremiumTransactions]                                      │
│    [dbo].[PolicyHierarchyAssignments]                               │
│    ... (and more)                                                   │
│                                                                       │
│  ✅ READY FOR COMMISSION CALCULATIONS                               │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 🔍 Detailed Explanation: How poc_etl.raw_* Tables Get Populated

### Option 1: Advanced ZIP-Based Ingest (Recommended)

**Script:** `scripts/ingest-raw-data.ts`

**Features:**
- Auto-detects ZIP files in `~/Downloads`
- Extracts CSV files from ZIP
- Validates column headers against expected schema
- Auto-selects next available schema (`raw_data1`, `raw_data2`, etc.)
- Preview mode (10 records per table)
- Dry-run mode (show what would happen)

**Usage:**
```bash
# Auto-detect ZIP and load into poc_etl
npx tsx scripts/ingest-raw-data.ts --schema poc_etl

# With specific ZIP file
npx tsx scripts/ingest-raw-data.ts \
  --zip ~/Downloads/data-2026-01-19.zip \
  --schema poc_etl

# Preview mode (10 records per table)
npx tsx scripts/ingest-raw-data.ts --schema poc_etl --preview

# Dry run (see what would happen)
npx tsx scripts/ingest-raw-data.ts --schema poc_etl --dry-run
```

**What it does:**
1. Finds ZIP file (or uses specified path)
2. Extracts all CSV files to temp directory
3. Matches CSV files to target tables by prefix:
   - `CertificateInfo*.csv` → `raw_certificate_info`
   - `premiums*.csv` → `raw_premiums`
   - `APL-Perf_Schedule*.csv` → `raw_schedule_rates`
   - `IndividualRosterExtract*.csv` → `raw_individual_brokers`
   - etc.
4. Creates `poc_etl` schema (if doesn't exist)
5. Creates all `raw_*` tables (with all NVARCHAR columns)
6. Bulk inserts data (1000 rows per batch)
7. Verifies row counts

---

### Option 2: Direct CSV Loader

**Script:** `scripts/load-csv.ts`

**Usage:**
```bash
# Load from hardcoded CSV directory
npx tsx scripts/load-csv.ts

# Test with 100 rows per file
npx tsx scripts/load-csv.ts --limit 100
```

**What it does:**
1. Reads CSV files from configured directory
2. Dynamically detects column names from CSV headers
3. Creates tables with all NVARCHAR(MAX) columns
4. Uses SQL Server `BULK INSERT` (5000 rows per batch - very fast)
5. Handles patterns like `CommissionsDetail_*.csv` (multiple files)

**CSV Directory:**
```typescript
// Hardcoded in script (line 37)
const csvDataPath = '/Users/kennpalm/Downloads/source/APL/apl-commissions-frontend/docs/data-map/rawdata';
```

---

### Option 3: POC Schema Setup (For Testing)

**Script:** `scripts/setup-poc-schemas.ts`

**Usage:**
```bash
npx tsx scripts/setup-poc-schemas.ts
```

**What it does:**
1. Creates `poc_etl`, `poc_dbo`, `poc_raw_data` schemas
2. Copies **sample data** from existing `etl` schema (if it exists)
3. Creates only 100 records per table (for testing)
4. Sets up state management tables

**Use case:** When you already have data in `etl` and want a small POC copy

---

## 📋 File-to-Table Mapping

| CSV File Pattern | Target Table | Key Fields |
|------------------|--------------|------------|
| `CertificateInfo*.csv` | `raw_certificate_info` | CertificateId, GroupId, Product |
| `premiums*.csv` | `raw_premiums` | Policy, GroupNumber, Amount |
| `APL-Perf_Schedule*.csv` | `raw_schedule_rates` | ScheduleName, ProductCode, State |
| `APL-Perf_Group*.csv` | `raw_perf_groups` | GroupId, GroupName, Size |
| `IndividualRosterExtract*.csv` | `raw_individual_brokers` | PartyUniqueId, Name, Status |
| `OrganizationRosterExtract*.csv` | `raw_org_brokers` | PartyUniqueId, OrgName |
| `BrokerLicenseExtract*.csv` | `raw_broker_licenses` | PartyUniqueId, State |
| `BrokerEO*.csv` | `raw_broker_eo` | PartyUniqueId, PolicyId |
| `CommissionsDetail*.csv` | `raw_commissions_detail` | CertificateId, BrokerId |
| `Fees*.csv` | `raw_fees` | Various |

---

## 🚀 Complete Workflow Example

### Step 1: Get CSV Data from Client
```bash
# Receive ZIP file from client
# File: APL-Data-Export-2026-01-19.zip
# Location: ~/Downloads/
```

### Step 2: Ingest CSV → poc_etl
```bash
cd /Users/kennpalm/Downloads/source/APL/apl-commissions-etl

# Test with preview (10 records per table)
npx tsx scripts/ingest-raw-data.ts --schema poc_etl --preview

# Verify preview data looks good
# Then do full load
npx tsx scripts/ingest-raw-data.ts --schema poc_etl
```

**Result:** `poc_etl.raw_*` tables populated with ~2.8M rows

### Step 3: Run ETL Pipeline
```bash
# Full pipeline (includes copy from poc_etl → etl)
npx tsx scripts/run-pipeline.ts

# Or with step-by-step verification
npx tsx scripts/run-pipeline.ts --step-by-step
```

**Pipeline phases:**
1. ✅ Schema Setup (creates/resets etl schema)
2. ✅ **Data Ingest** (copies poc_etl → etl, populates input tables)
3. ✅ Transforms (40+ SQL scripts)
4. ✅ Export (etl.stg_* → dbo.*)

### Step 4: Verify Production Data
```sql
-- Check production table counts
SELECT 'Brokers' as tbl, COUNT(*) as cnt FROM [dbo].[Brokers]
UNION ALL SELECT 'EmployerGroups', COUNT(*) FROM [dbo].[EmployerGroups]
UNION ALL SELECT 'Policies', COUNT(*) FROM [dbo].[Policies]
UNION ALL SELECT 'Hierarchies', COUNT(*) FROM [dbo].[Hierarchies]
UNION ALL SELECT 'Proposals', COUNT(*) FROM [dbo].[Proposals];
```

---

## 🎯 Summary

**Your Question:** "Where does the data come from? How are `poc_etl.raw_*` tables populated?"

**Answer:**

1. **Original Source:** CSV files exported from legacy LION system
2. **First Step:** CSV ingest scripts (`ingest-raw-data.ts` or `load-csv.ts`) read CSV files from disk
3. **Result:** Create `poc_etl` schema and populate `raw_*` tables via bulk insert
4. **ETL Pipeline:** Copies from `poc_etl` to `etl` schema, then transforms to `stg_*`, then exports to `dbo.*`

**The `poc_etl.raw_*` tables are populated by running CSV ingestion scripts, NOT by the main ETL pipeline. The ETL pipeline starts AFTER those tables are already populated.**

---

## 📚 Additional Documentation

- **CSV Ingest Features:** `INGEST_IMPROVEMENTS.md`
- **Pipeline Flow:** `PIPELINE-FLOW-DIAGRAM.md`
- **Pipeline Updates:** `PIPELINE-UPDATES-INGEST-PHASE.md`
- **Step-by-Step Guide:** `INGEST-STEP-BY-STEP-GUIDE.md`
