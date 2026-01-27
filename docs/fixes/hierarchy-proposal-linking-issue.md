# Hierarchy-Proposal Linking Issue - Root Cause & Fix

## Problem Summary

**77.7% of proposals (9,798 out of 12,615) have NO hierarchies configured.**

This causes:
- Policy resolution failures
- Policies falling back to `PolicyHierarchyAssignments` (92,324 policies affected)
- Inability to calculate commissions for these proposals

## Root Cause

The ETL hierarchy creation logic (`07-hierarchies.sql`) only links **ONE proposal per group** to hierarchies:

```sql
-- OLD CODE (lines 185-188)
COALESCE(
    (SELECT TOP 1 m.ProposalId FROM [etl].[stg_proposal_key_mapping] m WHERE m.GroupId = hd.GroupId ORDER BY m.EffectiveYear DESC),
    (SELECT TOP 1 p.Id FROM [etl].[stg_proposals] p WHERE p.GroupId = hd.GroupId AND p.EffectiveDateTo IS NULL ORDER BY p.EffectiveDateFrom DESC)
) AS ProposalId,
```

**Why this fails:**
- Hierarchies are created per `(Group, WritingBroker, SplitStructure)`
- Proposals are created per `(Group, DateRange, Product, Plan, etc.)`
- One group can have **many proposals** (e.g., G9966 has 9 proposals)
- But hierarchies are only linked to **ONE proposal** per group
- All other proposals get **NO hierarchies**

## Example: G9966

| Proposal | Date Range | Hierarchy? | Status |
|----------|------------|------------|--------|
| P-G9966-C1 | 2015-07-01 to 2017-09-30 | ❌ None | Cannot calculate |
| P-G9966-1 | 2017-10-01 to 2018-05-31 | ❌ None | Cannot calculate |
| P-G9966-3 | 2018-06-01 to 2018-06-30 | ❌ None | Cannot calculate |
| P-G9966-2 | 2018-07-01 to 2019-10-31 | ❌ None | Cannot calculate |
| P-G9966-C2 | 2019-11-01 to 2022-07-31 | ✅ H-G9966-1 | **Only one with hierarchy** |
| P-G9966-4 | 2019-11-01 to NULL | ❌ None | Cannot calculate |
| P-G9966-5 | 2022-08-01 to 2023-07-31 | ❌ None | Cannot calculate |
| P-G9966-6 | 2023-08-01 to 2025-07-31 | ❌ None | Cannot calculate |
| P-G9966-7 | 2025-08-01 to NULL | ❌ None | Cannot calculate |

**Result:** Only 1 out of 9 proposals has a hierarchy!

## Fixes Implemented

### 1. ETL Fix: Improved Hierarchy-Proposal Linking

**File:** `sql/transforms/07-hierarchies.sql` (lines 185-200)

**Change:** Updated hierarchy creation to link hierarchies to proposals based on **date range matching**:

```sql
-- NEW CODE: Links to FIRST matching proposal based on date range
COALESCE(
    -- Match 1: Hierarchy date within proposal date range
    (SELECT TOP 1 p.Id 
     FROM [etl].[stg_proposals] p 
     WHERE p.GroupId = hd.GroupId
       AND p.EffectiveDateFrom IS NOT NULL
       AND CAST(hd.MinEffDate AS DATE) >= p.EffectiveDateFrom
       AND (p.EffectiveDateTo IS NULL OR CAST(hd.MinEffDate AS DATE) <= p.EffectiveDateTo)
     ORDER BY p.EffectiveDateFrom DESC),
    -- Match 2: Open-ended proposal where hierarchy date >= proposal start
    (SELECT TOP 1 p.Id 
     FROM [etl].[stg_proposals] p 
     WHERE p.GroupId = hd.GroupId
       AND p.EffectiveDateTo IS NULL
       AND p.EffectiveDateFrom IS NOT NULL
       AND CAST(hd.MinEffDate AS DATE) >= p.EffectiveDateFrom
     ORDER BY p.EffectiveDateFrom DESC),
    -- Match 3: Fallback to most recent proposal
    (SELECT TOP 1 p.Id 
     FROM [etl].[stg_proposals] p 
     WHERE p.GroupId = hd.GroupId
     ORDER BY p.EffectiveDateFrom DESC)
) AS ProposalId,
```

**Impact:** Future ETL runs will link hierarchies to proposals more intelligently, but still only ONE proposal per hierarchy.

### 2. Post-Transform Fix: Create Duplicate Hierarchies

**File:** `sql/fix/link-hierarchies-to-all-proposals.sql`

**Purpose:** After hierarchy creation, create duplicate hierarchy records for proposals that don't have hierarchies.

**Strategy:**
1. Find proposals without hierarchies
2. Find candidate hierarchies (same group, date range matches)
3. Create duplicate hierarchy records linked to those proposals
4. Copy hierarchy versions and participants

**Status:** Script created, INSERT statements commented out for safety.

## Data Fix Scripts

### Fix Existing Production Data

**File:** `sql/fix/fix-hierarchy-proposal-linking.sql`

**Purpose:** Fix existing production data by creating duplicate hierarchies for proposals without hierarchies.

**Findings:**
- 13,707 hierarchy-proposal matches found
- 9,798 proposals without hierarchies
- Most proposals have candidate hierarchies that can be linked

**Status:** Script created, UPDATE statements commented out for safety.

## Recommended Approach

### Option A: Create Duplicate Hierarchies (Recommended for Quick Fix)

1. Run the post-transform fix script after ETL:
   ```bash
   cd /Users/kennpalm/Downloads/source/APL/apl-commissions-etl
   sqlcmd -S halo-sql.database.windows.net -d halo-sqldb -U azadmin -P "..." -i sql/fix/link-hierarchies-to-all-proposals.sql
   ```

2. Uncomment the INSERT blocks in the script
3. Run export to production

**Pros:**
- Quick fix for existing data
- Preserves existing hierarchy structure
- Each proposal gets its own hierarchy

**Cons:**
- Creates duplicate hierarchy records
- More storage required

### Option B: Refactor ETL to Create Hierarchies Per Proposal (Long-term)

Refactor `07-hierarchies.sql` to create hierarchies FOR EACH PROPOSAL instead of one per group.

**Pros:**
- Cleaner data model
- No duplicates
- Each proposal guaranteed to have hierarchy

**Cons:**
- Requires significant ETL refactoring
- More complex hierarchy creation logic

## Verification

After applying fixes, verify:

```sql
-- Should show 0 proposals without hierarchies
SELECT COUNT(*) as ProposalsWithoutHierarchies
FROM [dbo].[Proposals] p
WHERE p.Status = 2
  AND NOT EXISTS (
      SELECT 1 FROM [dbo].[Hierarchies] h 
      WHERE h.ProposalId = p.Id
  );
```

## Impact

**Before Fix:**
- 9,798 proposals without hierarchies (77.7%)
- 92,324 policies in PolicyHierarchyAssignments
- Cannot calculate commissions for these proposals

**After Fix:**
- All proposals should have hierarchies
- Policies can resolve to proposals
- Commissions can be calculated

## Next Steps

1. ✅ ETL fix implemented (improved linking logic)
2. ✅ Post-transform fix script created
3. ✅ Production fix script created
4. ⏳ **TODO:** Test post-transform fix script
5. ⏳ **TODO:** Apply fix to production data
6. ⏳ **TODO:** Verify all proposals have hierarchies
