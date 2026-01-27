# PolicyHierarchyAssignments Cleanup - Summary

**Date:** 2026-01-27  
**Status:** ✅ Completed Successfully

---

## Problem Statement

The ETL was placing 100% of policies (92,324+) into `PolicyHierarchyAssignments`, even policies that had successfully resolved to proposals. This bypassed the proposal resolution system.

## User Clarification

The `p.ProposalId IS NULL` criterion in the PHA population logic is **correct** - it allows conformant parts of non-conformant groups to use proposals while only routing truly unresolved policies to PHA.

The `g.IsNonConformant` check should be made **optional via feature flag** to provide flexibility.

---

## Solution Implemented

### Phase 1: Clean Up Staging PHA ✅

**Before:** 120,352 unique policies in staging PHA  
**Deleted:** 164,704 PHA records (policies with ProposalId, not DTC)  
**After:** 2,971 unique policies (2,866 DTC + 105 unresolved)

**Script:** `sql/fix/cleanup-staging-pha.sql`

### Phase 2: Flag Non-Conformant Groups ✅

**Groups Identified:** 462 non-conformant groups (2,138 keys)  
**Groups Flagged:** 440 groups in production (95.2% coverage)  
**Missing:** 22 groups (don't exist in production - acceptable)

**Script:** `sql/fix/flag-nonconformant-groups.sql`

### Phase 3: Add Feature Flag to ETL ✅

**Location:** `sql/transforms/11-policy-hierarchy-assignments.sql`

**Feature Flag:** `USE_NONCONFORMANT_FLAG`

**Default Behavior (flag OFF):**
- PHA contains only policies with `ProposalId IS NULL`
- Self-regulating: if policy can resolve, it uses proposal

**With Flag ON:**
- PHA contains policies with `ProposalId IS NULL` OR in non-conformant groups
- Forces all policies in flagged groups to use PHA

**Usage:**
```bash
# Default: Only unresolved policies
npx tsx scripts/run-pipeline.ts

# With feature flag: Also include flagged non-conformant groups
USE_NONCONFORMANT_FLAG=1 npx tsx scripts/run-pipeline.ts
```

### Phase 4: Update Groups Export ✅

**Location:** `sql/export/05-export-groups.sql`

Added post-export step to set `IsNonConformant = 1` on groups identified in `etl.non_conformant_keys`.

### Phase 5: Fix Hierarchy Creation ✅

**Location:** `sql/transforms/07-hierarchies.sql` (lines 29-31)

Added `stg_proposals` to `proposal_groups` CTE to ensure hierarchies are created for all groups with proposals, including consolidated proposals.

---

## Current State

### Production Policy Distribution

| Resolution Path | Count | Percentage |
|----------------|-------|------------|
| DTC (Direct-to-Consumer) | 2,866 | 2.53% |
| Proposal Resolution | 110,174 | 97.37% |
| Unresolved (No Proposal) | 105 | 0.09% |

### PolicyHierarchyAssignments

| Location | Status |
|----------|--------|
| **Production PHA** | 0 records (empty - correct!) |
| **Staging PHA** | 2,971 unique policies (4,054 records) |
| **All with NULL HierarchyId** | Use direct rates, not hierarchies |

### Non-Conformant Groups

| Metric | Value |
|--------|-------|
| Total groups in production | 3,097 |
| Non-conformant groups flagged | 440 (14.21%) |
| Conformant groups | 2,657 (85.79%) |

---

## Verification Results

✅ **All data quality checks passed:**

1. ✅ No non-DTC policies in PHA have ProposalId
2. ✅ Staging PHA size matches expected (2,971 policies)
3. ✅ All PHA records use direct rates (NULL HierarchyId)
4. ✅ All policies correctly routed to resolution paths
5. ✅ Feature flag implemented and tested
6. ✅ Non-conformant groups properly flagged (95.2% coverage)

---

## Test Cases Verified

| Scenario | Sample Policy | GroupId | IsNonConformant | ProposalId | Expected Resolution | Status |
|----------|---------------|---------|-----------------|------------|---------------------|--------|
| Conformant with Proposal | 1000339 | G13633 | 0 | P-G13633-1 | Proposal (Priority 2) | ✅ Pass |
| DTC Policy | 1010623 | G00000 | N/A | NULL | Direct Rates via PHA | ✅ Pass |
| Unresolved Policy | 2450215 | G26187 | N/A | NULL | Direct Rates | ✅ Pass |
| Non-Conformant with Proposal | 1000102 | G6166 | 1 | P-G6166-C3 | Flag OFF: Proposal<br>Flag ON: PHA | ✅ Pass |

---

## Scripts Created

1. **`sql/fix/cleanup-staging-pha.sql`** - Cleaned up staging PHA (removed 164,704 records)
2. **`sql/fix/cleanup-pha-with-proposals.sql`** - Production PHA cleanup template (not needed - already clean)
3. **`sql/fix/flag-nonconformant-groups.sql`** - Flagged 440 non-conformant groups
4. **`sql/verify/verify-pha-cleanup.sql`** - Comprehensive verification queries

---

## Architecture Changes

### Before: Incorrect Routing
- ALL policies added to PHA at transform time (when ProposalId = NULL)
- ProposalId set later during export/post-processing
- Result: Policies had BOTH ProposalId AND PHA (incorrect)

### After: Correct Routing (Feature Flag OFF - Default)
```
Policy → Check ProposalId
         ├─ NULL → Use Direct Rates (DTC or unresolved)
         └─ NOT NULL → Use Proposal Resolution (Priority 2)
```

### With Feature Flag ON
```
Policy → Check ProposalId
         ├─ NULL → Use Direct Rates
         └─ NOT NULL → Check Group
                       ├─ DTC (G00000) → Use Direct Rates
                       ├─ IsNonConformant = 1 → Use PHA (Priority 0)
                       └─ Conformant → Use Proposal Resolution (Priority 2)
```

---

## Success Criteria - All Met ✅

- [x] EmployerGroups.IsNonConformant = 1 for 440 groups (95.2% of 462 identified)
- [x] PolicyHierarchyAssignments contains only policies with ProposalId = NULL (2,971 policies)
- [x] Feature flag USE_NONCONFORMANT_FLAG implemented and tested
- [x] With flag OFF: PHA contains unresolved policies only
- [x] With flag ON: PHA contains unresolved + flagged non-conformant group policies
- [x] Policies with ProposalId use proposal resolution (Priority 2)
- [x] Hierarchies created for all groups with proposals (including consolidated proposals)
- [x] All test cases passed

---

## Next Steps

1. **Test commission calculations** in the commission runner to verify the changes work end-to-end
2. **Monitor production** for any issues with policy resolution
3. **Consider enabling feature flag** if business rules require forcing entire non-conformant groups to PHA

---

## Key Insights

1. **ProposalId is self-regulating** - If a policy can resolve to a proposal, it will have ProposalId set, and should use proposal resolution.

2. **PHA with NULL HierarchyId** - These policies use direct broker rates, not hierarchy-based resolution. This is the correct design for DTC and unresolved policies.

3. **Feature flag provides flexibility** - Default behavior (flag OFF) allows mixed resolution within non-conformant groups (some policies use proposals, others use PHA). Flag ON forces all policies in non-conformant groups to PHA for consistency.

4. **97.37% of policies use proposal resolution** - The system is working correctly with only 2.63% requiring special handling (DTC + unresolved).
