# ETL Pipeline: Quick Reference Card

## 🚀 Most Common Commands

### First-Time Setup (Verify Everything):
```bash
npx tsx scripts/run-pipeline.ts --step-by-step

# Or target specific schema:
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step
```
**Pauses after each step for verification. Best for initial testing.**

---

### Production Run (Fully Automated):
```bash
npx tsx scripts/run-pipeline.ts

# Or target specific schema:
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json
```
**No pauses, runs all phases automatically.**

---

### Run Only Transforms (Data Already Loaded):
```bash
npx tsx scripts/run-pipeline.ts --skip-ingest --skip-export
```
**Useful when testing transform logic changes.**

---

### Step-by-Step Transforms Only:
```bash
npx tsx scripts/run-pipeline.ts --step-by-step --transforms-only
```
**Best for validating transform changes with verification pauses.**

---

## 📋 Execution Modes

| Mode | Flag | Behavior |
|------|------|----------|
| **Auto** | _(default)_ | Runs continuously, no pauses |
| **Manual** | `--step-by-step` | Pauses after each step for verification |

---

## 🎯 Phase Control Flags

| Flag | Effect |
|------|--------|
| `--skip-ingest` | Skip data copy from poc_etl |
| `--skip-transform` | Skip all transforms |
| `--skip-export` | Skip export to production |
| `--transforms-only` | Alias: `--skip-ingest --skip-export` |
| `--export-only` | Alias: `--skip-ingest --skip-transform` |

---

## 🔍 Verification Scripts (Detailed)

### Ingest Phase:
```bash
npx tsx scripts/run-ingest-step-by-step.ts
```
**2 steps with verification queries, ~78 seconds**

### Transform Phase:
```bash
npx tsx scripts/run-transforms-step-by-step.ts
```
**12+ steps with verification queries, varies by data size**

### Standalone Ingest (Quick):
```bash
npx tsx scripts/copy-poc-etl-to-etl.ts
```
**Auto mode, no pauses, ~60 seconds**

---

## ⚙️ Advanced Options

| Flag | Description | Example |
|------|-------------|---------|
| `--debug` | Limit records for testing (100 brokers, 50 groups, etc.) | `--debug` |
| `--resume` | Resume from last failed run | `--resume` |
| `--resume-from <id>` | Resume from specific run ID | `--resume-from abc123` |
| `--config <path>` | Use custom config file (changes target schema) | `--config appsettings.poc2.json` |

---

## 🗂️ Schema Configuration

| Config File | Target Schema | Use Case |
|-------------|---------------|----------|
| _default_ | `etl` | Production ETL |
| `appsettings.poc.json` | `poc_etl` | POC/testing environment |
| `appsettings.poc2.json` | `poc_etl2` | Alternative testing schema |

**Schema can also be set via environment variables:**
```bash
PROCESSING_SCHEMA=poc_etl2 npx tsx scripts/run-pipeline.ts
```

**See:** `SCHEMA-CONFIGURATION-GUIDE.md` for full details

---

## 📚 Step-by-Step Testing Guide

When running with `--step-by-step`, each step now displays:
- **📋 Rich descriptions** - What the step does and why it matters
- **🎯 Purpose** - Business context and technical goals
- **✅ Expected results** - Specific metrics (row counts, percentages)
- **🔍 Test queries** - Ready-to-copy SQL verification queries

### Full Testing Guide
**See:** `STEP-BY-STEP-TEST-GUIDE.md` - Comprehensive testing guide with:
- Detailed descriptions for all 12+ transform steps
- Multiple test queries per step
- Expected results and success criteria
- Common issues and troubleshooting
- Critical step indicators (⚠️ for must-pass steps)

---

## 📊 Expected Results (Verification Targets)

After successful pipeline run:

| Table | Expected Count | Critical Check |
|-------|----------------|----------------|
| `stg_schedules` | ~686 | ✅ Must have 600+ |
| `stg_schedule_rates` | ~10,090 | ✅ Must have rates |
| `stg_hierarchy_participants` | Varies | ✅ >95% with ScheduleId |
| `stg_proposals` | ~12K | ✅ >95% with BrokerId |
| `stg_groups` | ~3,100 | ✅ >95% with PrimaryBrokerId |
| `stg_policy_hierarchy_assignments` | ~90K | ✅ Multiple per policy for multi-earnings |

---

## 🐛 Troubleshooting Quick Commands

### Check if data is loaded:
```bash
sqlcmd -S "halo-sql.database.windows.net" -d "halo-sqldb" -U "azadmin" -P 'AzureSQLWSXHjj!jks7600' -C -Q "
SELECT 'raw_certificate_info' AS t, COUNT(*) FROM [etl].[raw_certificate_info];
SELECT 'input_certificate_info' AS t, COUNT(*) FROM [etl].[input_certificate_info];
"
```

### Check if schedules were created:
```bash
sqlcmd -S "halo-sql.database.windows.net" -d "halo-sqldb" -U "azadmin" -P 'AzureSQLWSXHjj!jks7600' -C -Q "
SELECT COUNT(*) AS schedules FROM [etl].[stg_schedules];
SELECT COUNT(*) AS rates FROM [etl].[stg_schedule_rates];
"
```

### Check schedule linking on hierarchies:
```bash
sqlcmd -S "halo-sql.database.windows.net" -d "halo-sqldb" -U "azadmin" -P 'AzureSQLWSXHjj!jks7600' -C -Q "
SELECT 
    COUNT(*) AS total,
    SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) AS with_schedule,
    CAST(SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct
FROM [etl].[stg_hierarchy_participants];
"
```

---

## 💡 Tips

### When to Use Step-by-Step Mode:
- ✅ First time running pipeline
- ✅ After modifying transform logic
- ✅ Debugging data quality issues
- ✅ Validating fix implementations

### When to Use Auto Mode:
- ✅ Production runs
- ✅ Automated/scheduled runs
- ✅ After pipeline is validated
- ✅ CI/CD pipelines

### Resume After Pause:
If you pause in step-by-step mode, resume with:
```bash
npx tsx scripts/run-pipeline.ts --resume
```

---

## 🎓 Example Session

```bash
# Terminal session showing step-by-step mode
$ npx tsx scripts/run-pipeline.ts --step-by-step

Phase 2: Data Ingest (2/6)
├─ [1/30] copy-from-poc-etl.sql ✅ (51s)
   📊 Verification: 2.8M records copied

Continue? (y/n/q): y

├─ [2/30] populate-input-tables.sql ✅ (26s)
   📊 Verification: 688 unique schedules found

Continue? (y/n/q): y

Phase 3: Data Transforms (3/6)
├─ [3/30] 00-references.sql ✅ (2s)
   📊 Verification: 50 states, 120 products

Continue? (y/n/q): y

├─ [4/30] 01-brokers.sql ✅ (6s)
   📊 Verification: 12,216 brokers (9,572 individuals, 2,644 orgs)

Continue? (y/n/q): y

... (continues with pauses between each step)
```

---

## 📚 Full Documentation

- **Execution Modes:** `EXECUTION-MODES-GUIDE.md` (detailed)
- **Ingest Details:** `INGEST-STEP-BY-STEP-GUIDE.md`
- **Pipeline README:** `README.md` (main documentation)
- **This Quick Reference:** `QUICK-REFERENCE.md` (you are here)
