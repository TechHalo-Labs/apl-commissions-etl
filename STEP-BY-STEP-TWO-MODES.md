# Step-by-Step Execution: Two Modes

## Overview

There are **two ways** to run the ETL pipeline with step-by-step verification:

1. **Main Pipeline** (`run-pipeline.ts --step-by-step`) - Quick pauses with basic info
2. **Dedicated Scripts** (`run-*-step-by-step.ts`) - Rich descriptions and comprehensive test queries

---

## Mode 1: Main Pipeline with `--step-by-step`

### Command:
```bash
npx tsx scripts/run-pipeline.ts --step-by-step
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step
```

### What You Get:
- ✅ Pauses after each step
- ✅ Basic description for key steps
- ✅ One quick test query per step
- ✅ Reference to full testing guide
- ✅ All phases in one run (ingest → transform → export)
- ✅ Progress tracking and state management

### Output Example:
```
════════════════════════════════════════════════════════════
  Step 4/42 completed: copy-from-poc-etl.sql
  📋 ⚠️ CRITICAL: Copies raw data from source schema to ETL working schema
════════════════════════════════════════════════════════════

  💡 Quick Test:
     SELECT COUNT(*) FROM [etl].[raw_certificate_info]; -- expect ~1.5M

  📚 Full test queries: STEP-BY-STEP-TEST-GUIDE.md
  🔧 Dedicated scripts: run-ingest-step-by-step.ts / run-transforms-step-by-step.ts

Continue to next step? (y/n/q to quit):
```

### Best For:
- ✅ Running the complete pipeline start-to-finish
- ✅ Production runs with verification checkpoints
- ✅ Quick validation at key steps
- ✅ When you want all phases in one execution

---

## Mode 2: Dedicated Step-by-Step Scripts

### Commands:
```bash
# Ingest phase only (2 steps)
npx tsx scripts/run-ingest-step-by-step.ts
npx tsx scripts/run-ingest-step-by-step.ts --config appsettings.poc2.json

# Transform phase only (12+ steps)
npx tsx scripts/run-transforms-step-by-step.ts
npx tsx scripts/run-transforms-step-by-step.ts --config appsettings.poc2.json
```

### What You Get:
- ✅ Rich, detailed descriptions for every step
- ✅ **Purpose** - Why the step matters
- ✅ **Expected Results** - Specific metrics to target
- ✅ **3-5 comprehensive test queries** per step
- ✅ Formatted, copy-paste ready SQL
- ✅ Critical step warnings (⚠️ CRITICAL)
- ✅ Focus on one phase at a time

### Output Example:
```
╔════════════════════════════════════════════════════════╗
║  Step 2: Brokers                                       ║
╚════════════════════════════════════════════════════════╝

📋 Description: 
   Transforms broker data from both individual and organization rosters

🎯 Purpose:
   Creates the master broker registry by combining individual agents and 
   broker organizations. Sets ExternalPartyId (UniquePartyId) which is the 
   primary identifier for brokers. Ensures all brokers have Status=Active 
   for commission processing.

✅ Expected Results:
   • ~12,000 total brokers (mix of individuals and organizations)
   • ~95%+ should have ExternalPartyId populated
   • All brokers should have Status=0 (Active)
   • Names should be properly formatted (not empty)

📄 Script: 01-brokers.sql

⏳ Executing...
✅ Step completed in 3.45s
📊 Records affected: 12,341

┌─────────────────────────────────────────┐
│         VERIFICATION RESULTS            │
└─────────────────────────────────────────┘

[Verification tables displayed]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔍 HOW TO TEST RESULTS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Copy and run these queries to verify data quality:

-- Check broker type distribution
SELECT BrokerType, COUNT(*) AS cnt, 
       COUNT(DISTINCT ExternalPartyId) AS unique_ids
FROM [etl].[stg_brokers]
GROUP BY BrokerType;

-- Check for missing critical data
SELECT 
    SUM(CASE WHEN ExternalPartyId IS NULL THEN 1 ELSE 0 END) AS missing_external_id,
    SUM(CASE WHEN Name IS NULL OR Name = '' THEN 1 ELSE 0 END) AS missing_name,
    SUM(CASE WHEN Status != 0 THEN 1 ELSE 0 END) AS inactive_status
FROM [etl].[stg_brokers];

-- Sample brokers
SELECT TOP 10 Id, Name, BrokerType, ExternalPartyId, Status 
FROM [etl].[stg_brokers] 
ORDER BY Id;

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Continue to next step? (y/n):
```

### Best For:
- ✅ Deep investigation of specific phases
- ✅ Learning the ETL process
- ✅ Troubleshooting data quality issues
- ✅ When you need comprehensive test queries
- ✅ Training or documentation purposes
- ✅ Detailed verification of each transform

---

## Comparison Matrix

| Feature | Main Pipeline (`--step-by-step`) | Dedicated Scripts |
|---------|----------------------------------|-------------------|
| **Phases Covered** | All (ingest + transform + export) | One phase at a time |
| **Descriptions** | Basic (key steps only) | Rich & detailed (every step) |
| **Test Queries** | 1 quick query | 3-5 comprehensive queries |
| **Expected Results** | Brief mention | Specific metrics listed |
| **Purpose/Context** | Minimal | Full business context |
| **Progress Tracking** | ✅ Full state management | ❌ Manual phase only |
| **Resume Support** | ✅ Can resume failed runs | ❌ Single phase execution |
| **Best For** | Production runs | Deep dive / learning |

---

## Recommended Workflow

### For First-Time Setup or Learning:
1. **Start with dedicated scripts** to understand each phase:
   ```bash
   # Learn ingest phase
   npx tsx scripts/run-ingest-step-by-step.ts --config appsettings.poc2.json
   
   # Learn transform phase
   npx tsx scripts/run-transforms-step-by-step.ts --config appsettings.poc2.json
   ```

2. **Reference the testing guide** alongside:
   - Open `STEP-BY-STEP-TEST-GUIDE.md` in editor
   - Copy/paste test queries as you go
   - Compare results against expected values

### For Production Runs with Checkpoints:
```bash
# Full pipeline with verification pauses
npx tsx scripts/run-pipeline.ts --config appsettings.poc2.json --step-by-step
```
- Quick validation at each step
- Continue through all phases
- Can reference test guide if issues found

### For Troubleshooting Specific Phase:
```bash
# Run just the phase you're debugging
npx tsx scripts/run-transforms-step-by-step.ts --config appsettings.poc2.json
```
- Detailed analysis of each step
- Comprehensive test queries
- Stop and investigate at any point

---

## Schema Targeting

Both modes support schema configuration:

```bash
# Main pipeline - target poc_etl2
npx tsx scripts/run-pipeline.ts \
  --config appsettings.poc2.json \
  --step-by-step

# Dedicated scripts - target poc_etl2
npx tsx scripts/run-ingest-step-by-step.ts \
  --config appsettings.poc2.json

npx tsx scripts/run-transforms-step-by-step.ts \
  --config appsettings.poc2.json
```

---

## When to Use Each Mode

### Use **Main Pipeline** (`--step-by-step`) When:
- Running complete ETL start-to-finish
- You need all phases in one run
- You want progress tracking and state management
- Quick verification is sufficient
- Production deployment with checkpoints

### Use **Dedicated Scripts** When:
- Learning the ETL process for the first time
- Investigating data quality issues in detail
- Need comprehensive test queries for verification
- Training new team members
- Troubleshooting a specific phase
- Want rich context and business explanations

---

## Quick Command Reference

| Goal | Command |
|------|---------|
| Full pipeline with pauses | `npx tsx scripts/run-pipeline.ts --step-by-step` |
| Ingest phase (detailed) | `npx tsx scripts/run-ingest-step-by-step.ts` |
| Transform phase (detailed) | `npx tsx scripts/run-transforms-step-by-step.ts` |
| Target specific schema | Add `--config appsettings.poc2.json` |
| Skip phases | Add `--skip-ingest`, `--skip-transform`, etc. |

---

## Documentation References

- **STEP-BY-STEP-TEST-GUIDE.md** - Comprehensive testing guide for all steps
- **EXECUTION-MODES-GUIDE.md** - Auto vs manual-with-verify modes
- **QUICK-REFERENCE.md** - Command cheat sheet
- **SCHEMA-CONFIGURATION-GUIDE.md** - Target different schemas
- **STEP-BY-STEP-ENHANCEMENTS-SUMMARY.md** - What was enhanced

---

## Summary

**You have two great options:**

1. **Main pipeline** - Fast, complete runs with basic verification
2. **Dedicated scripts** - Rich, educational, comprehensive testing

**Both are valid!** Choose based on your needs:
- **Production? Need all phases?** → Main pipeline
- **Learning? Troubleshooting? Need details?** → Dedicated scripts

**See full testing guide:** `STEP-BY-STEP-TEST-GUIDE.md`
