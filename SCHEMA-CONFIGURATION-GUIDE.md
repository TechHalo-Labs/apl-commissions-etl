# Schema Configuration Guide

## Overview

The ETL pipeline supports **configurable schemas** so you can work with different schema names (e.g., `etl`, `poc_etl`, `poc_etl2`) without changing SQL scripts.

---

## Default Schemas

| Purpose | Default Schema | Description |
|---------|----------------|-------------|
| **Source** | `new_data` | Source data location (PerfGroupModel, etc.) |
| **Transition** | `raw_data` | Legacy raw data (not actively used) |
| **Processing** | `etl` | Working schema for staging tables (`stg_*`) |
| **Production** | `dbo` | Production schema |

---

## Configuration Methods

### Method 1: Custom Config File (Recommended)

**Create a config file for your target schema:**

#### Example: `appsettings.poc2.json` (targets `poc_etl2`)

```json
{
  "database": {
    "connectionString": "",
    "schemas": {
      "source": "poc_etl2",
      "transition": "poc_etl2",
      "processing": "poc_etl2",
      "production": "dbo"
    },
    "pocMode": true
  }
}
```

**Usage:**
```bash
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json
```

**Step-by-step mode:**
```bash
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step
```

---

### Method 2: Environment Variables

**Set schema names via environment:**

```bash
export PROCESSING_SCHEMA="poc_etl2"
export SOURCE_SCHEMA="poc_etl2"
export TRANSITION_SCHEMA="poc_etl2"

# Then run normally
npx tsx scripts/run-pipeline.ts --step-by-step
```

**One-liner:**
```bash
PROCESSING_SCHEMA=poc_etl2 SOURCE_SCHEMA=poc_etl2 TRANSITION_SCHEMA=poc_etl2 \
  npx tsx scripts/run-pipeline.ts --step-by-step
```

---

## Available Config Files

| File | Target Schemas | Use Case |
|------|----------------|----------|
| `appsettings.json` | `etl` (default) | Production ETL |
| `appsettings.poc.json` | `poc_etl` | POC/testing with poc_etl schema |
| `appsettings.poc2.json` | `poc_etl2` | NEW: Testing with poc_etl2 schema |

---

## Schema Mapping in SQL Scripts

### How It Works:

SQL scripts use **SQL variables** that are replaced at execution time:

```sql
-- In SQL script:
SELECT * FROM [$(ETL_SCHEMA)].[stg_brokers];

-- When running with default config (etl):
SELECT * FROM [etl].[stg_brokers];

-- When running with poc_etl2 config:
SELECT * FROM [poc_etl2].[stg_brokers];
```

### Schema Variables:

| SQL Variable | Maps To | Example (default) | Example (poc_etl2) |
|--------------|---------|-------------------|---------------------|
| `$(SOURCE_SCHEMA)` | `schemas.source` | `new_data` | `poc_etl2` |
| `$(ETL_SCHEMA)` | `schemas.processing` | `etl` | `poc_etl2` |
| `$(PRODUCTION_SCHEMA)` | `schemas.production` | `dbo` | `dbo` |

---

## Complete Command Examples

### Target Default `etl` Schema:

```bash
# Auto mode
npx tsx scripts/run-pipeline.ts

# Step-by-step mode
npx tsx scripts/run-pipeline.ts --step-by-step
```

---

### Target `poc_etl` Schema:

```bash
# Auto mode
npx tsx scripts/run-pipeline.ts --config appsettings.poc.json

# Step-by-step mode
npx tsx scripts/run-pipeline.ts --config appsettings.poc.json --step-by-step
```

---

### Target `poc_etl2` Schema (NEW):

```bash
# Auto mode
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json

# Step-by-step mode  
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step

# Transforms only
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step --transforms-only
```

---

### Target Custom Schema via Environment Variable:

```bash
# Target poc_etl3 without creating config file
PROCESSING_SCHEMA=poc_etl3 SOURCE_SCHEMA=poc_etl3 \
  npx tsx scripts/run-pipeline.ts --step-by-step
```

---

## Phase-Specific Scripts with Schema Override

The standalone phase scripts also support schema configuration:

### Ingest Step-by-Step:

```bash
# Default (etl)
npx tsx scripts/run-ingest-step-by-step.ts

# Target poc_etl2
npx tsx scripts/run-ingest-step-by-step.ts --config appsettings.poc2.json

# Via environment
PROCESSING_SCHEMA=poc_etl2 npx tsx scripts/run-ingest-step-by-step.ts
```

### Transform Step-by-Step:

```bash
# Default (etl)
npx tsx scripts/run-transforms-step-by-step.ts

# Target poc_etl2
npx tsx scripts/run-transforms-step-by-step.ts --config appsettings.poc2.json

# Via environment
PROCESSING_SCHEMA=poc_etl2 npx tsx scripts/run-transforms-step-by-step.ts
```

---

## Verify Schema Configuration

**Before running, check which schemas will be used:**

```bash
# The pipeline logs schema configuration at startup
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step

# Output will show:
# 📋 ETL Configuration:
# ════════════════════════════════════════════════════════════════
# {
#   "database": {
#     "schemas": {
#       "source": "poc_etl2",
#       "transition": "poc_etl2",
#       "processing": "poc_etl2",   ← Working schema
#       "production": "dbo"
#     }
#   }
# }
```

---

## Quick Reference

### Use `etl` (default):
```bash
npx tsx scripts/run-pipeline.ts --step-by-step
```

### Use `poc_etl`:
```bash
npx tsx scripts/run-pipeline.ts --config appsettings.poc.json --step-by-step
```

### Use `poc_etl2`:
```bash
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step
```

### Use environment variable:
```bash
PROCESSING_SCHEMA=your_schema npx tsx scripts/run-pipeline.ts --step-by-step
```

---

## Creating Schema in Database

If `poc_etl2` doesn't exist yet, create it first:

```sql
-- Create schema
CREATE SCHEMA poc_etl2;

-- Verify
SELECT name FROM sys.schemas WHERE name = 'poc_etl2';
```

---

## Schema-Specific Operations

### Backup Current `etl` to `poc_etl2`:

```sql
-- Copy all staging tables from etl to poc_etl2
SELECT * INTO [poc_etl2].[stg_brokers] FROM [etl].[stg_brokers];
SELECT * INTO [poc_etl2].[stg_groups] FROM [etl].[stg_groups];
SELECT * INTO [poc_etl2].[stg_schedules] FROM [etl].[stg_schedules];
-- ... etc
```

### Compare Schemas:

```sql
-- Compare table counts between schemas
SELECT 'etl' AS schema_name, COUNT(*) FROM [etl].[stg_brokers]
UNION ALL
SELECT 'poc_etl2', COUNT(*) FROM [poc_etl2].[stg_brokers];
```

---

## Summary

✅ **The pipeline ALREADY supports targeting different schemas!**

**To target `poc_etl2`:**

1. ✅ Config file created: `appsettings.poc2.json`
2. ✅ Use with: `--config appsettings.poc2.json`
3. ✅ Works with: `--step-by-step`, `--transforms-only`, all phase scripts

**Commands:**
```bash
# Step-by-step with poc_etl2
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step

# Transforms only with poc_etl2
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --transforms-only

# Ingest step-by-step with poc_etl2
npx tsx scripts/run-ingest-step-by-step.ts --config appsettings.poc2.json
```
