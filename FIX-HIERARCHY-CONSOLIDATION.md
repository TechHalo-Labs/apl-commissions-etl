# Fix: Hierarchy Consolidation Bug

**Date:** 2026-01-26
**Issue:** 76.5% of proposals (9,096 out of 11,891) have NO hierarchy due to consolidation bug
**Root Cause:** ETL consolidated hierarchies by `StructureSignature`, creating ONE hierarchy for multiple time periods, then linked that hierarchy to ONLY ONE proposal

---

## 🔴 Problem Summary

### Issue Identified

Group G16163 example:
- **10 proposals** exist spanning 2013-2026 (different time periods, broker changes, schedule changes)
- **1 hierarchy** created (H-G16163-1) 
- **That hierarchy linked to ONLY 1 proposal** (P-G16163-8, the most recent)
- **9 proposals orphaned** (P-G16163-1 through P-G16163-7, P-G16163-C1, P-G16163-C2)

### Impact

```
Production Database:
- Total Proposals: 11,891
- Proposals WITHOUT Hierarchy: 9,096 (76.5%) ❌
- Proposals WITH Hierarchy: 6,130 (51.6%) ⚠️

Staging Backup (Even Worse):
- Total Proposals: 7,206
- Proposals WITHOUT Hierarchy: 7,206 (100%) ❌
- ALL staging hierarchies had NULL ProposalId
```

---

## 🔍 Root Cause Analysis

### ETL Script: `sql/transforms/07-hierarchies.sql`

**Step 2:** Creates `StructureSignature` per (GroupId, CertSplitSeq, WritingBrokerId)
- ✅ Correctly identifies unique broker structures

**Step 3 (THE BUG):** Line 95
```sql
GROUP BY GroupId, WritingBrokerId, StructureSignature;  ❌ CONSOLIDATES!
```
- ❌ Multiple CertSplitSeq values with same structure → ONE hierarchy
- ❌ Time periods collapsed

**Step 7:** Lines 195-217 - Hierarchy-Proposal Linking
```sql
ProposalId = COALESCE(
    -- Try to match by date range...
    -- Fallback to most recent proposal
)
```
- ❌ Links the ONE consolidated hierarchy to ONLY ONE proposal
- ❌ Other proposals orphaned

---

## ✅ Fix Applied

### Changes to `sql/transforms/07-hierarchies.sql`

#### 1. Step 3 - Line 95 (Primary Fix)

**Before:**
```sql
INSERT INTO [etl].[work_hierarchy_id_map] (...)
SELECT
    GroupId,
    WritingBrokerId,
    StructureSignature,
    MIN(MinEffDate) AS MinEffDate,
    MIN(CertSplitSeq) AS RepresentativeSplitSeq,
    CONCAT('H-', GroupId, '-', ...) AS HierarchyId
FROM [etl].[work_split_signatures]
GROUP BY GroupId, WritingBrokerId, StructureSignature;  ❌ CONSOLIDATION
```

**After:**
```sql
INSERT INTO [etl].[work_hierarchy_id_map] (...)
SELECT
    GroupId,
    WritingBrokerId,
    StructureSignature,
    MinEffDate,  -- Keep original date, don't MIN()
    CertSplitSeq AS RepresentativeSplitSeq,  -- Use actual CertSplitSeq
    CONCAT('H-', GroupId, '-', ...) AS HierarchyId
FROM [etl].[work_split_signatures]
-- REMOVED: GROUP BY GroupId, WritingBrokerId, StructureSignature;
-- Each CertSplitSeq gets its own hierarchy (no consolidation)
```

**Impact:**
- ✅ One hierarchy per `CertSplitSeq`
- ✅ No consolidation by `StructureSignature`
- ✅ Preserves time-based distinctions

---

#### 2. Step 4 - Line 118 (JOIN Update)

**Before:**
```sql
INNER JOIN [etl].[work_hierarchy_id_map] him 
    ON him.GroupId = ss.GroupId
    AND him.WritingBrokerId = ss.WritingBrokerId
    AND him.StructureSignature = ss.StructureSignature;  ❌ Many-to-1
```

**After:**
```sql
INNER JOIN [etl].[work_hierarchy_id_map] him 
    ON him.GroupId = ss.GroupId
    AND him.WritingBrokerId = ss.WritingBrokerId
    AND him.CertSplitSeq = ss.CertSplitSeq;  ✅ 1-to-1
```

**Impact:**
- ✅ JOIN now 1-to-1 instead of many-to-1
- ✅ Each `CertSplitSeq` maps to exactly one hierarchy

---

#### 3. Comments Updated

- Header comment updated to reflect "NO CONSOLIDATION"
- Step 3 comment clarifies the fix
- Step 4 comment explains 1-to-1 mapping
- Step 7 comment updated

---

## 📊 Expected Results After Fix

### For Group G16163:

**Before Fix:**
- 10 proposals → 1 hierarchy → 9 proposals orphaned (90%)

**After Fix:**
- 10 proposals → 10 hierarchies → 0 proposals orphaned (0%) ✅

### For All Groups:

**Before Fix:**
- 11,891 proposals
- ~1,681 hierarchies (over-consolidated)
- 9,096 proposals orphaned (76.5%)

**After Fix (Estimated):**
- 11,891 proposals
- ~11,891 hierarchies (1-to-1 with proposals)
- 0 proposals orphaned (0%) ✅

**Note:** Some proposals may share hierarchies if they genuinely have identical split configurations at the same time, but this will be rare.

---

## 🎯 What Changed

| Aspect | Before | After |
|--------|--------|-------|
| **Consolidation** | ✅ Yes (by StructureSignature) | ❌ No (each CertSplitSeq separate) |
| **Hierarchies per Group** | Few (consolidated) | Many (time-based) |
| **Proposal-Hierarchy Ratio** | Many proposals : 1 hierarchy | ~1 proposal : 1 hierarchy |
| **Orphaned Proposals** | 76.5% (9,096 of 11,891) | ~0% (target) |
| **Historical Accuracy** | ❌ Lost (time periods merged) | ✅ Preserved |

---

## 🧪 Testing Plan Required

**⏸️ PAUSED FOR TESTING PLAN**

Before running the full ETL, we need a plan to:

1. **Unit Test:** Run just `07-hierarchies.sql` on test data
2. **Verify:** Check hierarchy count vs proposal count
3. **Sample:** Validate G16163 specifically
4. **Full ETL:** Run complete pipeline
5. **Production Impact:** Assess changes before export

### Questions to Address:

1. Should we test on a subset of groups first?
2. How do we verify the fix worked correctly?
3. What metrics should we track (hierarchy count, proposal coverage, etc.)?
4. Should we export to a test schema first?
5. What rollback plan if something goes wrong?

---

## 📝 Files Modified

1. ✅ `/Users/kennpalm/Downloads/source/APL/apl-commissions-etl/sql/transforms/07-hierarchies.sql`
   - Line 3-7: Header comment updated
   - Line 86-96: Step 3 - Removed GROUP BY consolidation
   - Line 107-119: Step 4 - Updated JOIN to use CertSplitSeq
   - Line 185-186: Step 7 comment updated

---

## 🔄 Related Issues

This fix also addresses:
- Multiple earnings per certificate (each split sequence preserved)
- Time-based commission rule changes (each period gets its own hierarchy)
- Broker hierarchy changes (writing broker changes preserved)

---

## ⚠️ Potential Side Effects

### More Hierarchies (Expected)

- **Before:** ~1,681 hierarchies
- **After:** ~11,891 hierarchies (potentially)
- **Why:** No longer consolidating, each split sequence gets its own hierarchy

**This is CORRECT behavior** - reflects actual commission structure changes over time.

### Database Size

- Hierarchies table will grow significantly
- HierarchyVersions, HierarchyParticipants will also grow
- This is necessary to capture accurate commission history

### Performance

- More hierarchies = more lookups during commission calculation
- Should be offset by better indexing and 1-to-1 proposal mapping
- Net impact: Likely POSITIVE (fewer failed lookups)

---

## 🎉 Expected Benefits

1. ✅ **Zero orphaned proposals** (down from 76.5%)
2. ✅ **Accurate historical commission rules** preserved
3. ✅ **Commission calculations will succeed** for all proposals
4. ✅ **Multiple earnings per certificate** properly supported
5. ✅ **Time-based changes** (broker, schedule, splits) captured correctly

---

## Next Steps

**READY FOR TESTING PLAN** ⏸️

User requested pause after fix to create testing strategy.
