# ETL Pipeline: Execution Modes & Verification Guide

## Overview

The ETL pipeline supports **two execution modes** with **configurable verification**:

1. **Auto Mode** (Default) - Runs continuously without pauses
2. **Manual-with-Verify Mode** - Pauses between steps for verification

Both modes are available for:
- ✅ Ingest Phase
- ✅ Transform Phase
- ✅ Export Phase

---

## Quick Start: Which Mode Should I Use?

| Scenario | Recommended Mode | Command |
|----------|------------------|---------|
| **First-time setup** | Manual-with-Verify | `--step-by-step` |
| **Testing changes** | Manual-with-Verify | `--step-by-step` |
| **Production runs** | Auto | Default (no flag) |
| **Already validated** | Auto | Default (no flag) |
| **Debugging issues** | Manual-with-Verify | `--step-by-step` |

---

## Mode 1: Auto Mode (Default)

**Usage:**
```bash
npx tsx scripts/run-pipeline.ts
```

**Behavior:**
- ✅ Runs all phases continuously
- ✅ Shows progress logs
- ✅ SQL scripts have built-in verification queries
- ❌ Does NOT pause between steps
- ✅ State management (can resume if fails)

**Best for:**
- Production runs
- Automated CI/CD
- Already validated pipelines

**Example Output:**
```
Phase 2: Data Ingest (2/6)
├─ [1/30] copy-from-poc-etl.sql ✅ (51.3s, 2.8M records)
├─ [2/30] populate-input-tables.sql ✅ (26.4s, 1.5M records)

Phase 3: Data Transforms (3/6)
├─ [3/30] 00-references.sql ✅ (2.1s, 150 records)
├─ [4/30] 01-brokers.sql ✅ (5.8s, 12.2K records)
... (continues without pausing)
```

---

## Mode 2: Manual-with-Verify Mode (Step-by-Step)

### Option A: Unified Pipeline with --step-by-step Flag

**Usage:**
```bash
npx tsx scripts/run-pipeline.ts --step-by-step
```

**Behavior:**
- ✅ Runs one step at a time
- ✅ Shows progress logs + verification results
- ⏸️ **Pauses after each step** with prompt: "Continue? (y/n/q)"
- ✅ Can abort at any step
- ✅ State management (can resume later)

**Example Output:**
```
Phase 2: Data Ingest (2/6)
├─ [1/30] copy-from-poc-etl.sql
   ⏳ Executing...
   ✅ Completed in 51.3s (2.8M records)
   
   📊 Verification: raw_certificate_info = 1,550,752 rows ✅

════════════════════════════════════════════════════════
  Step 1/30 completed: copy-from-poc-etl.sql
════════════════════════════════════════════════════════

Continue to next step? (y/n/q to quit): _
```

### Option B: Separate Phase-Specific Scripts

**For more granular control, use dedicated scripts:**

#### Ingest Only (Step-by-Step):
```bash
npx tsx scripts/run-ingest-step-by-step.ts
```

#### Transform Only (Step-by-Step):
```bash
npx tsx scripts/run-transforms-step-by-step.ts
```

**Benefits:**
- ✅ More detailed verification queries per step
- ✅ Easier to focus on one phase
- ✅ Can skip to specific phase

---

## Command Reference

### Full Pipeline Commands

| Command | Mode | Phases | Description |
|---------|------|--------|-------------|
| `npx tsx scripts/run-pipeline.ts` | Auto | All | Default: runs everything |
| `npx tsx scripts/run-pipeline.ts --step-by-step` | Manual | All | Pauses between ALL steps |
| `npx tsx scripts/run-pipeline.ts --skip-ingest` | Auto | Transform + Export | Skip ingest if data loaded |
| `npx tsx scripts/run-pipeline.ts --step-by-step --skip-ingest` | Manual | Transform + Export | Step-by-step transforms only |
| `npx tsx scripts/run-pipeline.ts --transforms-only` | Auto | Transform only | Skip ingest and export |
| `npx tsx scripts/run-pipeline.ts --step-by-step --transforms-only` | Manual | Transform only | Step-by-step transforms, no export |

### Phase-Specific Commands

| Command | Phase | Mode | Description |
|---------|-------|------|-------------|
| `npx tsx scripts/run-ingest-step-by-step.ts` | Ingest | Manual | Detailed ingest with pauses |
| `npx tsx scripts/run-transforms-step-by-step.ts` | Transform | Manual | Detailed transforms with pauses |
| `npx tsx scripts/copy-poc-etl-to-etl.ts` | Ingest | Auto | Quick ingest without pauses |

---

## Verification Queries Built Into Scripts

Each SQL script already includes verification queries that run at the end. In **Auto Mode**, these show in the logs. In **Manual Mode**, they display before the pause prompt.

### Ingest Phase Verification:

#### `copy-from-poc-etl.sql`:
- Row counts for all raw tables
- Sample data from `raw_certificate_info`

#### `populate-input-tables.sql`:
- Total rows, unique groups, unique schedules
- Sample certificate data

### Transform Phase Verification:

#### `04-schedules.sql` (CRITICAL):
```sql
-- Shows:
- Schedules created: 686 (expected 600+)
- Schedule rates: 10,090
- Heaped vs level-only rates breakdown
- Top schedules by rate count
```

#### `07-hierarchies.sql` (CRITICAL):
```sql
-- Shows:
- Hierarchy count
- Participants with/without ScheduleId
- Schedule linking percentage (target: >95%)
- Top 10 unmatched ScheduleCodes (if any)
```

#### `11-policy-hierarchy-assignments.sql` (CRITICAL):
```sql
-- Shows:
- Total PHA assignments
- Unique policies
- Policies with multiple assignments (expected for multiple earnings)
- PHA participants with ScheduleId percentage
```

#### `99-audit-and-cleanup.sql` (Final):
```sql
-- Shows:
- Final entity counts for all staging tables
- Broker ID population percentage (target: >95%)
- Data quality validation summary
```

---

## Example: Testing a New Transform

Let's say you modified `07-hierarchies.sql` and want to test it:

### Step 1: Run ingest (if not already done)
```bash
npx tsx scripts/run-pipeline.ts --transforms-only
# Skips ingest, runs all transforms, skips export
```

### Step 2: Run transforms step-by-step starting from hierarchy script
```bash
npx tsx scripts/run-transforms-step-by-step.ts
```

When it reaches `07-hierarchies.sql`:
- Review the verification results
- Check ScheduleId linking percentage
- If good: Continue
- If bad: Abort, fix script, re-run

---

## Recommended Workflows

### First-Time Setup (Validate Everything):
```bash
# Step 1: Ingest with verification
npx tsx scripts/run-ingest-step-by-step.ts

# Step 2: Transform with verification
npx tsx scripts/run-transforms-step-by-step.ts

# Step 3: Export (can use auto mode once ingest+transform validated)
npx tsx scripts/run-pipeline.ts --skip-ingest --skip-transform
```

### After Code Changes (Quick Validation):
```bash
# Unified pipeline with pauses
npx tsx scripts/run-pipeline.ts --step-by-step
```

### Production Runs (Fully Automated):
```bash
# No pauses, full automation
npx tsx scripts/run-pipeline.ts
```

### Re-run Single Phase:
```bash
# Ingest only
npx tsx scripts/run-pipeline.ts --skip-transform --skip-export

# Transform only
npx tsx scripts/run-pipeline.ts --skip-ingest --skip-export

# Export only
npx tsx scripts/run-pipeline.ts --skip-ingest --skip-transform
```

---

## Configuration Summary

### Via Command Line Flags:

| Flag | Effect |
|------|--------|
| `--step-by-step` | Enable manual verification mode (pauses between steps) |
| `--config <file>` | Use custom config file (change target schema) |
| `--skip-ingest` | Skip ingest phase |
| `--skip-transform` | Skip transform phase |
| `--skip-export` | Skip export phase |
| `--transforms-only` | Alias for `--skip-ingest --skip-export` |
| `--export-only` | Alias for `--skip-ingest --skip-transform` |
| `--resume` | Resume from last failed run |
| `--debug` | Enable debug mode (record limits) |

### Schema Configuration:

**Default:** All operations target `etl` schema

**To target different schema (e.g., `poc_etl2`):**

```bash
# Method 1: Config file (recommended)
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step

# Method 2: Environment variable
PROCESSING_SCHEMA=poc_etl2 SOURCE_SCHEMA=poc_etl2 \
  npx tsx scripts/run-pipeline.ts --step-by-step
```

**See:** `SCHEMA-CONFIGURATION-GUIDE.md` for full schema configuration details

### Combinations:

```bash
# Step-by-step ingest + transforms, skip export
npx tsx scripts/run-pipeline.ts --step-by-step --skip-export

# Step-by-step transforms only (skip ingest and export)
npx tsx scripts/run-pipeline.ts --step-by-step --transforms-only

# Auto mode, skip ingest
npx tsx scripts/run-pipeline.ts --skip-ingest
```

---

## Verification Checklists

### After Ingest Phase:
- [ ] `raw_certificate_info`: ~1.5M rows ✅
- [ ] `raw_schedule_rates`: ~1.1M rows ✅
- [ ] `input_certificate_info`: ~1.5M rows ✅
- [ ] Unique schedules referenced: ~688 ✅

### After Transform Phase (Critical Checks):
- [ ] `stg_schedules`: 600+ schedules created ✅
- [ ] `stg_schedule_rates`: 10K+ rates ✅
- [ ] `stg_hierarchies`: Active status ✅
- [ ] `stg_hierarchy_participants`: >95% have ScheduleId ✅
- [ ] `stg_proposals`: Broker IDs populated >95% ✅
- [ ] `stg_policy_hierarchy_assignments`: Multiple per policy for multi-earning policies ✅
- [ ] `stg_groups`: PrimaryBrokerId populated >95% ✅

### After Export Phase:
- [ ] Production tables match staging counts ✅
- [ ] No data loss during export ✅
- [ ] Status values correct (Active, Approved) ✅

---

## Files Created

### New Scripts:
- ✅ `scripts/run-ingest-step-by-step.ts` - Ingest phase with verification
- ✅ `scripts/run-transforms-step-by-step.ts` - Transform phase with verification
- ✅ `EXECUTION-MODES-GUIDE.md` - This guide
- ✅ `INGEST-STEP-BY-STEP-GUIDE.md` - Detailed ingest guide

### Modified:
- ✅ `scripts/run-pipeline.ts` - Added `--step-by-step` flag support

### SQL Scripts (Already Have Verification):
- ✅ `sql/ingest/copy-from-poc-etl.sql` - Has verification queries
- ✅ `sql/ingest/populate-input-tables.sql` - Has verification queries
- ✅ `sql/transforms/04-schedules.sql` - Has comprehensive verification
- ✅ `sql/transforms/07-hierarchies.sql` - Has schedule linking verification
- ✅ `sql/transforms/99-audit-and-cleanup.sql` - Final data quality checks

---

## Summary

You now have **complete control** over execution and verification:

### For Testing/Validation:
```bash
# Option 1: Unified pipeline with pauses
npx tsx scripts/run-pipeline.ts --step-by-step

# Option 2: Phase-specific with detailed verification
npx tsx scripts/run-ingest-step-by-step.ts
npx tsx scripts/run-transforms-step-by-step.ts
```

### For Production:
```bash
# Fully automated, no pauses
npx tsx scripts/run-pipeline.ts
```

### Mix and Match:
```bash
# Manual ingest, auto transform, skip export
npx tsx scripts/run-ingest-step-by-step.ts
npx tsx scripts/run-pipeline.ts --skip-ingest --skip-export
```

**Both ingest and transform phases are now configurable for auto or manual-with-verify execution!**
