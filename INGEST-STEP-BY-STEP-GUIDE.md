# Ingest Phase: Step-by-Step Execution & Verification Guide

## Overview

There are **three ways** to run the ingest phase with verification:

1. **Automated Step-by-Step Script** (NEW) - Pauses between steps for verification
2. **Manual SQL Execution** - Run each SQL script individually via sqlcmd
3. **Full Pipeline** - Automated, but no pauses (production mode)

---

## Method 1: Automated Step-by-Step Script (RECOMMENDED FOR TESTING)

### Usage:

```bash
cd /Users/kennpalm/Downloads/source/APL/apl-commissions-etl
npx tsx scripts/run-ingest-step-by-step.ts
```

### What It Does:

1. ✅ Executes **Step 1: Copy Raw Data**
   - Shows row counts for each raw table
   - Shows sample data
   - **Pauses and asks: "Continue to next step? (y/n)"**

2. ✅ After you verify and confirm → Executes **Step 2: Populate Input Tables**
   - Shows input table statistics
   - Shows unique schedules referenced
   - **Pauses and asks: "Continue to next step? (y/n)"**

3. ✅ Shows completion summary

### Example Output:

```
╔════════════════════════════════════════════════════════╗
║  Step 1: Copy Raw Data                                ║
╚════════════════════════════════════════════════════════╝

📋 Description: Copies raw data from poc_etl to etl schema
📄 Script: copy-from-poc-etl.sql

⏳ Executing...

✅ Step completed in 51.3s
📊 Records affected: 2,867,577

┌─────────────────────────────────────────────┐
│         VERIFICATION RESULTS                │
└─────────────────────────────────────────────┘

Result Set 1:
┌─────────────────────────┬───────────┐
│ table                   │ row_count │
├─────────────────────────┼───────────┤
│ raw_certificate_info    │ 1,550,752 │
│ raw_schedule_rates      │ 1,133,420 │
│ raw_perf_groups         │    32,753 │
│ raw_premiums            │   138,436 │
│ raw_individual_brokers  │     9,572 │
│ raw_org_brokers         │     2,644 │
└─────────────────────────┴───────────┘

Result Set 2:
┌───────────────┬──────────┬────────────────────┬──────────────────┐
│ CertificateId │ GroupId  │ CommissionsSchedule│ WritingBrokerID  │
├───────────────┼──────────┼────────────────────┼──────────────────┤
│ 101           │ 25992    │ RZ3                │ P14388           │
│ 102           │ 25992    │ RZ3                │ P14388           │
│ 103           │ 25992    │ HOME               │ P14388           │
└───────────────┴──────────┴────────────────────┴──────────────────┘

Continue to next step? (y/n): _
```

### Benefits:

- ✅ Pause after each step to review results
- ✅ Automatic verification queries
- ✅ Can abort if something looks wrong
- ✅ Clean, formatted output
- ✅ No manual SQL needed

---

## Method 2: Manual SQL Execution (MAXIMUM CONTROL)

For complete control, run each SQL script individually via `sqlcmd`:

### Step 1: Copy Raw Data

```bash
cd /Users/kennpalm/Downloads/source/APL/apl-commissions-etl

sqlcmd -S "halo-sql.database.windows.net" \
       -d "halo-sqldb" \
       -U "azadmin" \
       -P "AzureSQLWSXHjj!jks7600" \
       -C \
       -v ETL_SCHEMA="etl" \
       -i sql/ingest/copy-from-poc-etl.sql
```

**Verify Results:**

```bash
sqlcmd -S "halo-sql.database.windows.net" \
       -d "halo-sqldb" \
       -U "azadmin" \
       -P "AzureSQLWSXHjj!jks7600" \
       -C \
       -Q "
SELECT 'raw_certificate_info' AS [table], COUNT(*) AS cnt FROM [etl].[raw_certificate_info];
SELECT 'raw_schedule_rates' AS [table], COUNT(*) AS cnt FROM [etl].[raw_schedule_rates];
SELECT 'raw_perf_groups' AS [table], COUNT(*) AS cnt FROM [etl].[raw_perf_groups];
SELECT 'raw_premiums' AS [table], COUNT(*) AS cnt FROM [etl].[raw_premiums];
SELECT 'raw_individual_brokers' AS [table], COUNT(*) AS cnt FROM [etl].[raw_individual_brokers];
SELECT 'raw_org_brokers' AS [table], COUNT(*) AS cnt FROM [etl].[raw_org_brokers];
"
```

**Expected Results:**

```
table                    cnt
------------------------ -----------
raw_certificate_info     1,550,752
raw_schedule_rates       1,133,420
raw_perf_groups             32,753
raw_premiums               138,436
raw_individual_brokers       9,572
raw_org_brokers              2,644
```

**✅ If counts match expectations, proceed to Step 2.**

---

### Step 2: Populate Input Tables

```bash
sqlcmd -S "halo-sql.database.windows.net" \
       -d "halo-sqldb" \
       -U "azadmin" \
       -P "AzureSQLWSXHjj!jks7600" \
       -C \
       -v ETL_SCHEMA="etl" \
       -i sql/ingest/populate-input-tables.sql
```

**Verify Results:**

```bash
sqlcmd -S "halo-sql.database.windows.net" \
       -d "halo-sqldb" \
       -U "azadmin" \
       -P "AzureSQLWSXHjj!jks7600" \
       -C \
       -Q "
SELECT 
    'input_certificate_info' AS [table],
    COUNT(*) AS total_rows,
    COUNT(DISTINCT GroupId) AS unique_groups,
    COUNT(DISTINCT CommissionsSchedule) AS unique_schedules
FROM [etl].[input_certificate_info];

-- Check for schedules referenced
SELECT COUNT(DISTINCT CommissionsSchedule) AS schedules_referenced
FROM [etl].[input_certificate_info]
WHERE CommissionsSchedule IS NOT NULL AND CommissionsSchedule != '';
"
```

**Expected Results:**

```
table                   total_rows  unique_groups  unique_schedules
----------------------- ----------- -------------- ----------------
input_certificate_info  1,550,752   ~3,100         688
```

**✅ If results look good, ready for transform phase!**

---

## Method 3: Full Pipeline (PRODUCTION MODE)

For production, run the full automated pipeline:

```bash
cd /Users/kennpalm/Downloads/source/APL/apl-commissions-etl
npx tsx scripts/run-pipeline.ts
```

**Pros:**
- ✅ Fully automated
- ✅ State management (can resume if fails)
- ✅ Progress tracking
- ✅ Error handling

**Cons:**
- ❌ No pauses for verification
- ❌ Harder to debug if something goes wrong

**Use when:**
- You've already tested with step-by-step method
- Running in production environment
- Data quality is already validated

---

## Verification Checklist

After each step, verify:

### After Step 1 (Copy Raw Data):

- [ ] `raw_certificate_info`: ~1.5M rows
- [ ] `raw_schedule_rates`: ~1.1M rows (51K unique schedules)
- [ ] `raw_perf_groups`: ~33K rows
- [ ] `raw_premiums`: ~138K rows
- [ ] `raw_individual_brokers`: ~9.5K rows
- [ ] `raw_org_brokers`: ~2.6K rows
- [ ] Sample data looks correct (GroupId, CommissionsSchedule populated)

### After Step 2 (Populate Input Tables):

- [ ] `input_certificate_info`: Same count as `raw_certificate_info`
- [ ] Unique schedules: ~688 schedules referenced
- [ ] Unique groups: ~3,100 groups
- [ ] Unique brokers: ~20K+ writing brokers
- [ ] No NULL CommissionsSchedule on active certificates

---

## Troubleshooting

### Problem: "Invalid object name 'poc_etl.raw_certificate_info'"

**Cause:** Source data not in `poc_etl` schema

**Solution:** Check data location:
```sql
SELECT TABLE_SCHEMA, TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME LIKE '%certificate%';
```

Update source schema in `copy-from-poc-etl.sql` if needed.

---

### Problem: Row counts are 0

**Cause:** Source tables are empty

**Solution:**
1. Verify source data exists:
   ```sql
   SELECT COUNT(*) FROM [poc_etl].[raw_certificate_info];
   ```
2. Check if data is in different schema (e.g., `new_data`, `raw_data`)
3. Run data ingestion from CSVs if source is empty

---

### Problem: "Unique schedules: 0"

**Cause:** `CommissionsSchedule` column is NULL or empty

**Solution:** Check raw data quality:
```sql
SELECT TOP 10 
    CertificateId, 
    GroupId, 
    CommissionsSchedule,
    CASE 
        WHEN CommissionsSchedule IS NULL THEN 'NULL'
        WHEN CommissionsSchedule = '' THEN 'EMPTY'
        ELSE 'OK'
    END AS schedule_status
FROM [etl].[raw_certificate_info];
```

---

## Quick Reference: Commands

### Step-by-Step Script:
```bash
npx tsx scripts/run-ingest-step-by-step.ts
```

### Manual Step 1:
```bash
sqlcmd -S "halo-sql.database.windows.net" -d "halo-sqldb" -U "azadmin" -P 'AzureSQLWSXHjj!jks7600' -C -v ETL_SCHEMA="etl" -i sql/ingest/copy-from-poc-etl.sql
```

### Manual Step 2:
```bash
sqlcmd -S "halo-sql.database.windows.net" -d "halo-sqldb" -U "azadmin" -P 'AzureSQLWSXHjj!jks7600' -C -v ETL_SCHEMA="etl" -i sql/ingest/populate-input-tables.sql
```

### Full Pipeline:
```bash
npx tsx scripts/run-pipeline.ts
```

### Skip Ingest (If Already Done):
```bash
npx tsx scripts/run-pipeline.ts --skip-ingest
```

---

## Summary

| Method | Control | Automation | Best For |
|--------|---------|------------|----------|
| Step-by-Step Script | Medium | High | Initial testing, validation |
| Manual SQL | Maximum | Low | Debugging, custom verification |
| Full Pipeline | Low | Maximum | Production, automated runs |

**Recommendation:** Use **step-by-step script** for first run, then switch to **full pipeline** once validated.
