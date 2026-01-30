# Systemic Data Issues Report

**Date:** 2026-01-29  
**Severity:** 🔴 CRITICAL  
**Scope:** Production Database

---

## Issue #1: PolicyHierarchyAssignments - 100% BROKEN REFERENCES

### Severity: 🔴 **CRITICAL - SYSTEMIC**

### Summary
**ALL 468 unique hierarchy references in PolicyHierarchyAssignments are broken.**
- **468 unique hierarchies referenced**
- **0 exist in Hierarchies table**
- **100% missing rate**

### Root Cause
**Format Mismatch Between Tables:**

| Table | HierarchyId Format | Example |
|-------|-------------------|---------|
| **PolicyHierarchyAssignments** (Production) | `H-G00000-280` | From previous ETL run |
| **Hierarchies** (Production - Current) | `H-1`, `H-10`, `H-100` | From TypeScript builder |
| **stg_policy_hierarchy_assignments** (Staging) | `NULL` | Non-conforming policies |

### Impact
- **3,733 policies** cannot resolve their hierarchies
- Commission calculations will fail for these policies
- Primarily affects **Direct-to-Consumer (DTC)** policies (G00000 group)

### Data Breakdown

**Top 10 Missing Hierarchies:**

| HierarchyId | Policy Count | Group |
|-------------|--------------|-------|
| H-G00000-358 | 1,475 | DTC |
| H-G00000-415 | 159 | DTC |
| H-G00000-106 | 76 | DTC |
| H-G00000-280 | 72 | DTC |
| H-G00000-366 | 67 | DTC |
| H-G00000-43 | 59 | DTC |
| H-G00000-124 | 50 | DTC |
| H-G00000-353 | 46 | DTC |
| H-G00000-37 | 46 | DTC |
| H-G00000-299 | 43 | DTC |

**Sample Affected Policies:**

| PolicyId | PolicyNumber | GroupId | GroupName | Missing HierarchyId |
|----------|--------------|---------|-----------|---------------------|
| 1010623 | 1010623 | G00000 | Direct to Consumer | H-G00000-280 |
| 1010625 | 1010625 | G00000 | Direct to Consumer | H-G00000-280 |
| 1121404 | 1121404 | G00000 | Direct to Consumer | H-G00000-358 |
| 113582 | 113582 | G00000 | Direct to Consumer | H-G00000-219 |

### Why This Happened
1. **Previous ETL Run:** PolicyHierarchyAssignments was populated from an earlier ETL that used a different hierarchy ID generation scheme
2. **New ETL Run:** Hierarchies table was cleared and re-exported with NEW IDs from TypeScript builder (`H-1`, `H-10`, etc.)
3. **No Re-export:** PolicyHierarchyAssignments was NOT re-exported, leaving orphaned references

---

## Issue #2: Missing Schedule Rates

### Severity: ⚠️ **MINOR - NOT SYSTEMIC**

### Summary
**Limited to specific products and schedules:**
- Only **1 product** (HI1721LPH) has NO rates at all (affects 4 policies in 1 group)
- Product HI1721L missing rates in **5 specific schedules** (239, 240, 241, 242, 247)

### Data Breakdown

**Overall Statistics:**
- **237 products** in Policies
- **253 products** have schedule rates
- **1 product** (HI1721LPH) with NO rates

**HI1721L Schedule Analysis:**

| ScheduleId | ScheduleName | Rate Count for HI1721L |
|------------|--------------|------------------------|
| 239 | GRPHI - BICD | 0 |
| 240 | GRPHI - BICD3 | 0 |
| 241 | GRPHI - BICE | 0 |
| 242 | GRPHI - BICE3 | 0 |
| 247 | GRPHI - BICJ | 0 |

### Impact
- **Minimal:** Only 4 policies for HI1721LPH
- **Specific:** HI1721L policies using those 5 schedules cannot calculate commissions
- **Not Blocking:** Most policies (99.9%+) can still calculate commissions

### Root Cause
Legacy data migration incomplete for these specific product/schedule combinations.

---

## Recommended Fixes

### Priority 1: Fix PolicyHierarchyAssignments (CRITICAL)

**Option A: Clear and Don't Re-Export (Recommended)**
```sql
-- Since staging has NULL HierarchyIds for non-conforming policies,
-- and these represent data quality issues, we should clear this table
DELETE FROM dbo.PolicyHierarchyAssignments;
```

**Rationale:**
- Staging data has `NULL` HierarchyIds with reason: "Invalid GroupId (null/empty/zeros)"
- These are **non-conforming policies** that need special handling
- Better to have NO assignments than WRONG assignments

**Option B: Export from Staging (If Staging Had Valid Data)**
```sql
-- This would work IF staging had valid HierarchyIds matching production format
INSERT INTO dbo.PolicyHierarchyAssignments (...)
SELECT ... FROM etl.stg_policy_hierarchy_assignments
WHERE HierarchyId IS NOT NULL;
```

**Rationale:**
- Only export policies with valid hierarchy assignments
- Skip non-conforming policies (NULL HierarchyIds)

### Priority 2: Document Missing Schedule Rates (MINOR)

**Action:**
1. Document HI1721LPH and HI1721L as products needing rate data
2. Source rates from legacy system if available
3. Create placeholder rates if needed for testing

---

## Data Quality Analysis

### PolicyHierarchyAssignments Data Quality

**Staging Data Analysis:**
```
Sample staging records show:
- HierarchyId: NULL
- IsNonConforming: 1
- NonConformantReason: "Invalid GroupId (null/empty/zeros)"
```

**Interpretation:**
- These are known data quality issues
- Policies lack proper group assignment
- Cannot be mapped to valid hierarchies
- Should NOT be included in commission calculations

### Conclusion

**Issue #1 (Missing Hierarchies): SYSTEMIC & CRITICAL**
- Affects 100% of PolicyHierarchyAssignments
- Requires immediate fix (clear table)
- Commission calculations blocked for these 3,733 policies

**Issue #2 (Missing Schedule Rates): ISOLATED & MINOR**
- Affects < 0.1% of policies
- Specific to 1-2 products
- Can be addressed later without blocking overall system

---

## Immediate Actions Required

1. ✅ **Clear PolicyHierarchyAssignments**
   ```sql
   DELETE FROM dbo.PolicyHierarchyAssignments;
   ```

2. ⏸️ **Decide on Non-Conforming Policy Handling**
   - Should non-conforming policies (NULL hierarchies) be included?
   - If yes, create a catch-all hierarchy or special handling logic
   - If no, exclude from commission calculations

3. 📝 **Document Missing Rate Products**
   - Add HI1721LPH and HI1721L to known issues list
   - Track as technical debt for rate data import

---

## Testing Recommendations

**After Fixes:**
1. Verify PolicyHierarchyAssignments is empty or has valid references only
2. Run test commission calculations on remaining policies
3. Monitor for errors related to missing hierarchies
4. Generate report of policies excluded due to data quality issues

---

**Status:** 🔴 **AWAITING DECISION ON FIX STRATEGY**  
**Next Steps:** User to decide on handling of non-conforming policies
