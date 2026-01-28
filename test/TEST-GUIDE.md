# Hierarchy Fix - Focused Test Guide

## Overview

This focused test validates that the hierarchy consolidation fix works correctly by:
1. Testing on a **specific problem group** (G16163)
2. Verifying **1-to-1 mapping** between CertSplitSeq and Hierarchies
3. Checking **referential integrity** for all foreign keys
4. Confirming **zero NULL values** in critical fields

**Test Duration:** ~10-15 seconds
**Test Scope:** Single group (G16163) with 10 proposals

---

## Quick Start

### Option 1: Run with Script (Easiest)

```bash
cd /Users/kennpalm/Downloads/source/APL/apl-commissions-etl/test
./run-hierarchy-test.sh
```

### Option 2: Run with sqlcmd Directly

```bash
sqlcmd -S "halo-sql.database.windows.net" \
       -d "halo-sqldb" \
       -U "azadmin" \
       -P 'AzureSQLWSXHjj!jks7600' \
       -C \
       -i test/test-hierarchy-fix.sql
```

---

## What the Test Does

### Step 1: Verify Source Data
- ✅ Checks that raw certificate data exists for G16163
- ✅ Counts unique split sequences
- ✅ Identifies broker structure changes over time

### Step 2: Clear Test Tables
- ✅ Clears work tables and staging tables for G16163 only
- ✅ Non-destructive (only affects test group)

### Step 3: Run Hierarchy Transform
- ✅ Executes the **FIXED** hierarchy logic
- ✅ Creates one hierarchy per CertSplitSeq (no consolidation)
- ✅ Maps split sequences to hierarchies (1-to-1)

### Step 4: Validate Results
Runs 4 validation checks:

#### ✓ Validation 1: CertSplitSeq → Hierarchy Mapping
- Verifies 1-to-1 ratio
- Checks for duplicates (should be 0)

#### ✓ Validation 2: Proposal → Hierarchy Coverage
- Counts proposals for G16163
- Checks how many proposals have hierarchies

#### ✓ Validation 3: Referential Integrity
- GroupId exists in stg_groups
- BrokerId exists in stg_brokers
- ScheduleCode exists in stg_schedules

#### ✓ Validation 4: Data Quality
- No NULL GroupId
- No NULL BrokerId
- No NULL HierarchyId

### Step 5: Detailed Breakdown
- Shows CertSplitSeq → Hierarchy mapping table
- Lists participants per hierarchy
- Displays broker chains

### Step 6: Summary & Test Results
- ✅ **PASS** or ❌ **FAIL** verdict
- Clear indication if fix is working

---

## Expected Results (Before Fix)

```
❌ TEST FAILED

Issues:
- CertSplitSeq to Hierarchy ratio: 1:4 (should be 1:1)
- Expected 4 hierarchies, got 1 hierarchy
- Consolidation by StructureSignature detected
```

---

## Expected Results (After Fix)

```
✅ TEST PASSED: Referential Integrity Validated

🎉 The fix is working correctly!
   - No consolidation by StructureSignature
   - Each CertSplitSeq has its own hierarchy
   - All foreign keys are valid
   - Ready for full ETL run

Summary:
✅ PASS: CertSplitSeq to Hierarchy ratio is 1:1 (4 hierarchies)
✅ PASS: All hierarchies have valid GroupId
✅ PASS: All hierarchies have valid BrokerId
✅ PASS: All hierarchies have valid HierarchyId
```

---

## Sample Output

```
════════════════════════════════════════════════════════════════
FOCUSED TEST: Hierarchy Fix - Referential Integrity Validation
════════════════════════════════════════════════════════════════

Test Configuration:
  Test Group: G16163
  Raw Group ID: 16163

────────────────────────────────────────────────────────────────
STEP 1: Verify Source Data
────────────────────────────────────────────────────────────────
metric                    certificate_count split_seq_count min_date   max_date  
Raw Certificates                         59               4 2013-12-01 2026-02-01

Unique Broker Structures  structure_count
                                        4

Source data verified.

────────────────────────────────────────────────────────────────
STEP 3: Run Hierarchy Transform (Test Group Only)
────────────────────────────────────────────────────────────────
Split participants extracted: 8 rows
Split signatures built: 4 rows
Hierarchy ID map: 4 hierarchies (NO CONSOLIDATION)  ← KEY: Should match split_seq_count
SplitSeq mappings: 4 rows (1-to-1)
Hierarchy data: 4 rows

════════════════════════════════════════════════════════════════
STEP 4: Validation Results
════════════════════════════════════════════════════════════════

✓ Validation 1: CertSplitSeq → Hierarchy Mapping
────────────────────────────────────────────────────────────────
metric                              value
Total CertSplitSeq values               4
Total Hierarchies created               4
Mapping ratio (should be 1:1)        1.00  ← KEY: Should be exactly 1.00
Duplicate CertSplitSeq (should be 0)    0  ← KEY: Should be 0

✓ Validation 2: Proposal → Hierarchy Coverage
────────────────────────────────────────────────────────────────
metric                            value
Proposals for G16163                 10  ← Production proposals
Proposals with at least 1 hierarchy  10  ← All should have hierarchies

✓ Validation 3: Referential Integrity
────────────────────────────────────────────────────────────────
metric                                  value
Hierarchies with valid GroupId              4
Hierarchies with valid BrokerId             4
Split participants with valid BrokerId      8
Split participants with valid Schedule  valid_count total_count pct_valid
                                                  8           8    100.00

✓ Validation 4: Data Quality
────────────────────────────────────────────────────────────────
metric                                  value
Hierarchies with NULL GroupId (0)           0  ← KEY: Must be 0
Hierarchies with NULL BrokerId (0)          0  ← KEY: Must be 0
Hierarchies with NULL HierarchyId (0)       0  ← KEY: Must be 0

════════════════════════════════════════════════════════════════
STEP 5: Detailed Breakdown for G16163
════════════════════════════════════════════════════════════════

▸ CertSplitSeq → Hierarchy Mapping:
CertSplitSeq HierarchyId   MinEffDate   WritingBrokerId
           1 H-G16163-1    2013-12-01             20766
           2 H-G16163-2    2013-12-01             20766
           3 H-G16163-3    2013-12-01             20766
           4 H-G16163-4    2013-12-01             20766

▸ Participants per Hierarchy:
HierarchyId   CertSplitSeq broker_count brokers
H-G16163-1               1            2 20766, 20787
H-G16163-2               2            2 20766, 20787
H-G16163-3               3            2 20766, 20787
H-G16163-4               4            2 20766, 20787

════════════════════════════════════════════════════════════════
TEST SUMMARY
════════════════════════════════════════════════════════════════

✅ PASS: CertSplitSeq to Hierarchy ratio is 1:1 (4 hierarchies)
✅ PASS: All hierarchies have valid GroupId
✅ PASS: All hierarchies have valid BrokerId
✅ PASS: All hierarchies have valid HierarchyId

════════════════════════════════════════════════════════════════
✅ TEST PASSED: Referential Integrity Validated

🎉 The fix is working correctly!
   - No consolidation by StructureSignature
   - Each CertSplitSeq has its own hierarchy
   - All foreign keys are valid
   - Ready for full ETL run
════════════════════════════════════════════════════════════════
```

---

## Key Metrics to Watch

### Critical Pass Criteria

| Metric | Expected | Description |
|--------|----------|-------------|
| **CertSplitSeq count** | 4 (for G16163) | Unique split sequences in source data |
| **Hierarchies created** | 4 | Should match CertSplitSeq count (1:1) |
| **Mapping ratio** | 1.00 | CertSplitSeq to Hierarchy ratio |
| **Duplicate CertSplitSeq** | 0 | No split sequence should map to multiple hierarchies |
| **NULL GroupId** | 0 | All hierarchies must have valid GroupId |
| **NULL BrokerId** | 0 | All hierarchies must have valid BrokerId |
| **NULL HierarchyId** | 0 | All hierarchies must have valid ID |
| **Proposals with hierarchy** | 10 (for G16163) | All proposals should resolve to hierarchies |

---

## Interpreting Results

### ✅ Test Passed

**What it means:**
- Fix is working correctly
- No consolidation by StructureSignature
- 1-to-1 mapping between CertSplitSeq and Hierarchies
- All foreign keys valid
- **SAFE to run full ETL**

**Next steps:**
1. Run full ETL transform: `npx tsx scripts/run-pipeline.ts --transforms-only`
2. Validate across all groups
3. Export to production

---

### ❌ Test Failed

**What it means:**
- Fix may not be applied correctly
- Consolidation still happening
- Foreign key issues detected
- **DO NOT run full ETL yet**

**Next steps:**
1. Review error messages in test output
2. Check if `07-hierarchies.sql` was updated correctly
3. Verify no GROUP BY on line 95
4. Verify JOIN uses CertSplitSeq on line 118
5. Re-run test after corrections

---

## Troubleshooting

### Problem: Test shows consolidation still happening

**Symptoms:**
```
❌ FAIL: CertSplitSeq to Hierarchy ratio is not 1:1
   Expected: 4 hierarchies
   Got: 1 hierarchies
```

**Solution:**
1. Verify `07-hierarchies.sql` line 95 has NO `GROUP BY`
2. Ensure changes were saved
3. Re-run test

---

### Problem: NULL values detected

**Symptoms:**
```
❌ FAIL: Found 4 hierarchies with NULL GroupId
```

**Solution:**
1. Check if `stg_groups` table is populated
2. Verify GroupId format matches (G16163 vs 16163)
3. Run brokers/groups transforms first if needed

---

### Problem: Proposals not resolving to hierarchies

**Symptoms:**
```
Proposals with at least 1 hierarchy: 0
```

**Solution:**
1. Check if `stg_proposals` table is populated
2. Verify date range matching logic
3. Ensure proposals exist for test group

---

## Test Scope

### What is Tested
- ✅ Hierarchy creation logic (no consolidation)
- ✅ CertSplitSeq to Hierarchy mapping (1-to-1)
- ✅ Foreign key validity (GroupId, BrokerId, ScheduleId)
- ✅ Data quality (no NULLs)
- ✅ Specific problem group (G16163)

### What is NOT Tested
- ❌ All groups (only G16163)
- ❌ Proposal exports
- ❌ Commission calculations
- ❌ Production database changes

**This is a FOCUSED test on the specific fix.**

---

## After Test Passes

Once the test passes, you're ready to:

1. **Run Full Transform:**
   ```bash
   cd /Users/kennpalm/Downloads/source/APL/apl-commissions-etl
   npx tsx scripts/run-pipeline.ts --step-by-step --transforms-only
   ```

2. **Validate All Groups:**
   ```sql
   -- Check hierarchy coverage across all groups
   SELECT 
       COUNT(*) AS total_proposals,
       SUM(CASE WHEN EXISTS (
           SELECT 1 FROM Hierarchies h WHERE h.ProposalId = p.Id
       ) THEN 1 ELSE 0 END) AS with_hierarchy,
       CAST(SUM(CASE WHEN EXISTS (
           SELECT 1 FROM Hierarchies h WHERE h.ProposalId = p.Id
       ) THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct
   FROM Proposals p;
   ```
   **Target:** 95%+ proposals with hierarchies

3. **Export to Production:**
   ```bash
   npx tsx scripts/run-pipeline.ts --export-only
   ```

---

## Files Created

1. ✅ `test/test-hierarchy-fix.sql` - SQL test script
2. ✅ `test/run-hierarchy-test.sh` - Shell runner script
3. ✅ `test/TEST-GUIDE.md` - This guide

---

## Summary

This focused test provides **quick validation** that the hierarchy consolidation fix works correctly:

- ⏱️ **Fast:** Runs in ~10-15 seconds
- 🎯 **Focused:** Tests specific problem (G16163)
- ✅ **Comprehensive:** Validates referential integrity
- 🔒 **Safe:** Non-destructive (only affects test group)

**Run this test BEFORE the full ETL to confirm the fix is working!**
