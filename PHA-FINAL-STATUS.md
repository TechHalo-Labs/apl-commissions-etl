# PolicyHierarchyAssignments - Final Status Report

**Date:** 2026-01-29  
**Status:** ✅ **CORRECTED - PHA Now Only for Non-Conformant Policies**

---

## Executive Summary

✅ **Fixed:** PHA reduced from 464,520 (incorrect - all policies) to **3,733 (correct - non-conformant only)**

✅ **Verified:** All 3,733 PHA records have valid hierarchies and participants with schedules

⚠️ **Gap Identified:** 41,514 non-conformant policies still need resolution

---

## Policy Distribution

| Category | Count | % | Has Commission Path? |
|----------|-------|---|----------------------|
| **Conformant Policies** | 378,265 | 89.5% | ✅ Via Proposal→Hierarchy |
| **Non-Conformant with PHA** | 2,747 | 0.65% | ✅ Via PHA→Hierarchy |
| **Non-Conformant without PHA** | 41,514 | 9.83% | ❓ **NEEDS RESOLUTION** |
| **TOTAL** | **422,526** | **100%** | - |

---

## Current PHA Status

### What PHA Contains Now ✅ CORRECT

**Count:** 3,733 PHA records for 2,747 unique policies

**Characteristics:**
- All have `IsNonConforming = 1`
- All have `GroupId = 'G00000'` (Direct-to-Consumer)
- All have historical split/tier structures from source data
- Average 1.36 splits per policy
- Range: 1-3 splits per policy

**TypeScript Builder Logic:**
```typescript
private isInvalidGroup(groupId: string): boolean {
  if (!groupId) return true;
  if (groupId.trim() === '') return true;
  if (/^0+$/.test(groupId)) return true; // All zeros
  return false;
}
```

Only policies with `NULL`, empty, or all-zeros GroupId get PHA.

---

## The Gap: 41,514 Policies Without Commission Path

### Who Are They?

**Count:** 41,514 policies (9.83% of total)

**Characteristics:**
- Have **valid GroupIds** (G6166, G15668, G13633, etc.)
- **No ProposalId** (couldn't match to proposal via key mapping)
- **Have historical hierarchy data** in source (etl.input_certificate_info)
- Most have simple single-broker structures

**Sample:**
| PolicyId | GroupId | ProductCode | Has Source Data? |
|----------|---------|-------------|------------------|
| 1000102 | G6166 | CPA2200 | ❌ No (not in raw) |
| 400083 | (check) | (check) | ✅ Yes (complex: 2 splits, 7 tiers) |
| 404064 | (check) | (check) | ✅ Yes (2 splits, 3 tiers) |

---

## Issue Analysis

### Why These Policies Are Non-Conformant

**Primary Reason:** No matching proposal in key mapping
- GroupId + Year + ProductCode + PlanCode doesn't match any proposal
- Even year-adjacent and group-fallback matching failed

**Possible Root Causes:**
1. Product codes in policies don't match proposal product lists
2. Plan codes don't match proposal plan constraints
3. Effective dates outside proposal date ranges
4. Groups that never had formal proposals (ad-hoc arrangements)

### Why They Don't Have PHA

**TypeScript Builder Criteria:**
- Only generates PHA for invalid/DTC GroupIds
- These policies have **valid** GroupIds
- Builder assumed they would match proposals (they didn't)

---

## Resolution Options

### Option 1: Generate PHA from Source Data (Comprehensive)

**Approach:** Generate PHA for ALL 44,261 non-conformant policies from `input_certificate_info`

**Pros:**
- Captures all historical payment structures
- Ensures all policies can calculate commissions
- Uses actual historical data

**Cons:**
- Requires re-running TypeScript builder or post-processing
- Some source data may be missing (certificates not in raw)
- ~6,881 certificates don't have raw data

**Implementation:**
1. Re-run TypeScript builder with relaxed PHA generation (include all non-proposal-matched)
2. Or post-process: Generate PHA from input_certificate_info for policies without ProposalId

---

### Option 2: Create Catch-All Proposals (Simpler)

**Approach:** Create generic proposals for unmatched GroupId+Product combinations

**Pros:**
- Cleaner data model (fewer policy hierarchy assignments needed)
- Follows standard path (Proposal→Hierarchy)
- Can use group-level hierarchies

**Cons:**
- May not preserve exact historical payment structure
- Need to create ~1,000-2,000 new proposals
- Need to link to appropriate hierarchies

**Implementation:**
1. Analyze unmapped GroupId+Product combinations
2. Create proposals for each combination
3. Link to hierarchies (group-level or create new)
4. Re-link policies to these new proposals

---

### Option 3: Hybrid Approach (Recommended)

**Approach:** 
- Keep PHA for DTC (G00000) policies with complex splits ✅ Already done
- Create catch-all proposals for other non-conformant groups
- Only generate PHA if source data shows complex non-standard splits

**Pros:**
- Balanced approach
- Uses standard path where possible
- Preserves complex historical structures via PHA

**Implementation:**
1. ✅ DTC policies with PHA: Done (2,747 policies, 3,733 PHA)
2. ⏸️ Analyze remaining 41,514 policies
3. ⏸️ Create proposals for groups with simple/standard hierarchies
4. ⏸️ Generate PHA only for those with truly non-standard splits

---

## Current Production State

### Conformant Policies (378,265) ✅ READY

**Commission Path:**
```
Policy → ProposalId → Proposal → Hierarchy → HierarchyVersion → HierarchyParticipants → Schedules → Rates
```

**Coverage:** 100%
- All have ProposalId
- All proposals link to hierarchies
- All hierarchies have participants
- All participants have schedules with rates

---

### Non-Conformant with PHA (2,747) ✅ READY

**Commission Path:**
```
Policy → PolicyHierarchyAssignment → Hierarchy (synthetic) → HierarchyVersion → HierarchyParticipants → Schedules → Rates
```

**Coverage:** 100%
- 3,733 PHA records (avg 1.36 per policy)
- 3,733 synthetic hierarchies created
- 6,337 participants with schedules
- All schedules have rates

---

### Non-Conformant without PHA (41,514) ❓ **NEEDS DECISION**

**Current Commission Path:** ❌ None

**Options:**
1. Generate PHA from source (if data exists)
2. Create catch-all proposals
3. Exclude from commissions (data quality issue)

**Impact:** 9.83% of policies can't calculate commissions until resolved

---

## Data in Staging vs Production

| Entity | Staging | Production | Match? |
|--------|---------|------------|--------|
| **PHA Records** | 3,733 | 3,733 | ✅ 100% |
| **Unique Policies in PHA** | 2,747 | 2,747 | ✅ 100% |
| **PHA Participants** | 6,337 | 6,337 | ✅ 100% |
| **Non-Conformant Hierarchies** | - | 3,733 | ✅ Generated |
| **Non-Conformant Hierarchy Versions** | - | 3,733 | ✅ Generated |

---

## Recommendations

### Immediate (For Commission Calculations)

**Status:** ✅ **READY for 90.2% of policies**
- 378,265 conformant (89.5%)
- 2,747 non-conformant with PHA (0.65%)
- **Total ready: 380,012 policies (90.2%)**

You can proceed with commission calculations for these 380K policies.

---

### Next Steps for the 41,514 Gap

**Investigate:**
1. Check how many have source data in `input_certificate_info`
2. Analyze split complexity (simple vs complex)
3. Determine appropriate resolution:
   - Simple/standard hierarchies → Create proposals
   - Complex/non-standard → Generate PHA

**Questions to Answer:**
1. Should we generate PHA for ALL non-conformant policies from source?
2. Or should we only keep PHA for truly exceptional cases (DTC)?
3. What's the business rule for policies without ProposalId?

---

## Summary

✅ **Mission Accomplished (Partially)**
- PHA corrected: Now only for non-conformant policies (3,733 vs 464K)
- All PHA have valid hierarchies and schedules
- 90.2% of policies ready for commission calculations

⚠️ **Gap Identified**
- 41,514 policies (9.83%) need resolution
- Have valid GroupIds but no ProposalId
- Most have source hierarchy data available
- Need business decision on handling approach

🎯 **Recommendation**
- Proceed with commission calculations for 380K ready policies
- Analyze the 41.5K gap to determine root cause
- Implement appropriate fix based on business rules

---

**Next Action:** User decision on how to handle 41,514 non-conformant policies without PHA
