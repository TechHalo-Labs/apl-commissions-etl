# ✅ Answer: Schema Targeting for poc_etl2

## Your Question

> "If I run this, what schema will it target? Can you set it to target poc_etl2?"

---

## Answer

### **Default Target: `etl` schema**

When you run without any config:

```bash
npx tsx scripts/run-pipeline.ts --step-by-step
```

**This targets:** `etl.stg_brokers`, `etl.stg_groups`, `etl.stg_schedules`, etc.

---

### **✅ To Target `poc_etl2`: Use `--config` Flag**

**I just created:** `appsettings.poc2.json`

**Usage:**
```bash
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step
```

**This targets:** `poc_etl2.stg_brokers`, `poc_etl2.stg_groups`, `poc_etl2.stg_schedules`, etc.

---

## Complete Commands for poc_etl2

### Step-by-Step Modes:

```bash
# Ingest step-by-step (2 steps with verification)
npx tsx scripts/run-ingest-step-by-step.ts --config appsettings.poc2.json

# Transform step-by-step (12+ steps with verification)
npx tsx scripts/run-transforms-step-by-step.ts --config appsettings.poc2.json

# Full pipeline step-by-step (all phases with verification)
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step
```

### Auto Modes:

```bash
# Full pipeline auto mode
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json

# Transforms only
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --transforms-only
```

---

## What Tables Get Created in poc_etl2?

### Ingest Phase Creates:
```
poc_etl2.raw_certificate_info      (~1.5M rows)
poc_etl2.raw_schedule_rates        (~1.1M rows)
poc_etl2.raw_perf_groups           (~33K rows)
poc_etl2.input_certificate_info    (~1.5M rows)
```

### Transform Phase Creates:
```
poc_etl2.stg_brokers               (~12K rows)
poc_etl2.stg_groups                (~3.1K rows)
poc_etl2.stg_schedules             (~686 schedules)
poc_etl2.stg_schedule_rates        (~10K rates)
poc_etl2.stg_proposals             (~12K proposals)
poc_etl2.stg_hierarchies           (~8K hierarchies)
poc_etl2.stg_hierarchy_participants
poc_etl2.stg_premium_split_versions
poc_etl2.stg_premium_split_participants
poc_etl2.stg_policies
poc_etl2.stg_policy_hierarchy_assignments
... (all staging tables)
```

### Export Phase Writes To:
```
dbo.Brokers                        (production - unchanged)
dbo.EmployerGroups                 (production - unchanged)
dbo.Proposals                      (production - unchanged)
... (all production tables in dbo schema)
```

**Note:** Export destination is always `dbo` (production), regardless of processing schema.

---

## Verification Example

```bash
$ npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step

════════════════════════════════════════════════════════════════
SQL Server ETL Pipeline
════════════════════════════════════════════════════════════════

📋 ETL Configuration:
{
  "database": {
    "schemas": {
      "source": "poc_etl2",       ← Data source
      "processing": "poc_etl2",   ← Working tables here!
      "production": "dbo"          ← Export destination
    }
  }
}
════════════════════════════════════════════════════════════════

Phase 2: Data Ingest (2/6)
├─ [1/30] copy-from-poc-etl.sql
   ⏳ Copying to poc_etl2.raw_certificate_info...
   ✅ Completed in 51.3s (1.5M rows)

Continue? (y/n): y

... (continues with all operations on poc_etl2)
```

---

## Alternative: Environment Variable

**If you don't want to use config files:**

```bash
export PROCESSING_SCHEMA="poc_etl2"
export SOURCE_SCHEMA="poc_etl2"

npx tsx scripts/run-pipeline.ts --step-by-step
```

**One-liner:**
```bash
PROCESSING_SCHEMA=poc_etl2 SOURCE_SCHEMA=poc_etl2 \
  npx tsx scripts/run-pipeline.ts --step-by-step
```

---

## Schema Isolation Benefits

Using `poc_etl2` instead of `etl` allows you to:

✅ Test changes without affecting main `etl` schema
✅ Run parallel ETL processes (one on `etl`, one on `poc_etl2`)
✅ Compare results between schemas
✅ Keep production `dbo` safe while testing

---

## Compare Results Between Schemas

After running on `poc_etl2`, compare with `etl`:

```sql
-- Compare broker counts
SELECT 'etl' AS schema_name, COUNT(*) AS broker_count 
FROM [etl].[stg_brokers]
UNION ALL
SELECT 'poc_etl2', COUNT(*) 
FROM [poc_etl2].[stg_brokers];

-- Compare schedule linking
SELECT 
    'etl' AS schema_name,
    CAST(SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS schedule_link_pct
FROM [etl].[stg_hierarchy_participants]
UNION ALL
SELECT 
    'poc_etl2',
    CAST(SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2))
FROM [poc_etl2].[stg_hierarchy_participants];
```

---

## Summary

### Your Question Answered:

**Q:** "What schema will it target?"
**A:** By default: `etl`. With `--config appsettings.poc2.json`: `poc_etl2`

**Q:** "Can you set it to target poc_etl2?"
**A:** ✅ **YES!**

### Your Commands:

```bash
# Test ingest on poc_etl2 (step-by-step)
npx tsx scripts/run-ingest-step-by-step.ts --config appsettings.poc2.json

# Test transforms on poc_etl2 (step-by-step)
npx tsx scripts/run-transforms-step-by-step.ts --config appsettings.poc2.json

# Full pipeline on poc_etl2 (step-by-step)
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step
```

**All staging operations will use `poc_etl2` schema.**
**Production export still targets `dbo` schema.**

---

## See Also

- **SCHEMA-CONFIGURATION-GUIDE.md** - Complete schema configuration documentation
- **QUICK-REFERENCE.md** - Command cheat sheet (updated with schema examples)
- **EXECUTION-MODES-GUIDE.md** - Full execution mode documentation
