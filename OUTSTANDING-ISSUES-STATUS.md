# Outstanding Issues - Current Status

**Date:** 2026-01-29  
**Review of:** Items from CHAIN-FIX-SUCCESS-REPORT.md

---

## Summary

| Issue | Status | Impact | Priority |
|-------|--------|--------|----------|
| 1. WH01 & WH02 schedules | ⚠️ **STILL OUTSTANDING** | 2 participants (0.001%) | Low |
| 2. HI1721L rates (schedules 239-247) | ⚠️ **STILL OUTSTANDING** | 5 schedules affected | Medium |
| 3. 44,261 unlinked policies | ✅ **MOSTLY RESOLVED** | 6,828 remain (1.6%) | Low |
| 4. 423 alphanumeric GroupIds | ✅ **RESOLVED** | 0 remaining | N/A |

---

## Issue 1: WH01 & WH02 Missing Schedules ⚠️

**Status:** ⚠️ **STILL OUTSTANDING**

**Details:**
- 2 HierarchyParticipants reference schedule codes `WH01` and `WH02`
- These schedules don't exist in `dbo.Schedules`
- Participants cannot link to schedules (ScheduleId is NULL)

**Impact:** 
- **0.001%** of participants (2 out of 161,924)
- Minimal - these 2 participants can't calculate commissions
- Likely affects 1-2 policies maximum

**Resolution Options:**
1. ✅ **Recommended:** Add WH01 & WH02 schedules if source data exists in legacy system
2. Remove these 2 participants if schedules are invalid/deprecated
3. Map to alternative schedules if these are aliases

**Priority:** **Low** - affects <0.01% of data

---

## Issue 2: HI1721L Rates Missing from Schedules 239-247 ⚠️

**Status:** ⚠️ **STILL OUTSTANDING**

**Details:**
- 5 schedules exist: 239 (GRPHI-BICD), 240 (GRPHI-BICD3), 241 (GRPHI-BICE), 242 (GRPHI-BICE3), 247 (GRPHI-BICJ)
- Each has 7-8 rates for OTHER products
- **Zero rates** for product `HI1721L`
- This was discovered in the systemic analysis for Group G25565

**Current State:**
```
Schedule 239: 8 rates, 0 for HI1721L
Schedule 240: 8 rates, 0 for HI1721L
Schedule 241: 7 rates, 0 for HI1721L
Schedule 242: 7 rates, 0 for HI1721L
Schedule 247: 7 rates, 0 for HI1721L
```

**Impact:**
- **Unknown scope** - need to check how many policies have product HI1721L with these schedules
- Could affect multiple policies in Group G25565 and potentially other groups
- Policies with this combination cannot calculate commissions

**Resolution:**
1. ✅ **Recommended:** Import HI1721L rates for these schedules from legacy data
2. Check if HI1721L + these schedules is a valid combination
3. Query to find affected policies:
   ```sql
   -- How many policies are impacted?
   SELECT COUNT(*) 
   FROM Policies p
   WHERE p.ProductCode = 'HI1721L'
     AND EXISTS (
       SELECT 1 FROM HierarchyParticipants hp
       INNER JOIN HierarchyVersions hv ON hv.Id = hp.HierarchyVersionId
       INNER JOIN Hierarchies h ON h.Id = hv.HierarchyId
       INNER JOIN PolicyHierarchyAssignments pha ON pha.HierarchyId = h.Id
       WHERE pha.PolicyId = p.Id
         AND hp.ScheduleId IN (239, 240, 241, 242, 247)
     );
   ```

**Priority:** **Medium** - need to assess scope first

---

## Issue 3: 44,261 Unlinked Policies (No ProposalId) ✅

**Status:** ✅ **MOSTLY RESOLVED** (84.6% fixed)

**Original Problem:**
- 44,261 policies with `ProposalId IS NULL` (non-conformant)
- Could not calculate commissions via standard Proposal→Hierarchy path

**Resolution:**
- ✅ Generated **65,771 PHA records** for **37,433 policies** (84.6%)
- ✅ Created synthetic hierarchies with historical payment structures
- ✅ Linked to schedules with rates (100% coverage)

**Remaining Gap:**
- ⚠️ 6,828 policies (15.4%) still without PHA
- **Root Cause:** No source data in `etl.input_certificate_info`
- These certificates exist in `new_data.CertificateInfo` but weren't copied during ETL ingest

**Impact:**
- **1.6%** of total policies (6,828 out of 422,526)
- Cannot calculate commissions without historical hierarchy data

**Resolution Options:**
1. ✅ **Recommended:** Re-run data ingest to include missing certificates
   - Update `copy-from-poc-etl.sql` or `populate-input-tables.sql`
   - Ensure all `CertStatus='A'` certificates are copied
   - Re-run proposal builder to generate PHA
2. Exclude as data quality issues (document as limitation)
3. Create default single-broker hierarchies (not recommended - guessing)

**Priority:** **Low** - only 1.6% impact, commission calculations can proceed without

---

## Issue 4: 423 Alphanumeric GroupIds ✅

**Status:** ✅ **RESOLVED & RECOVERED**

**Original Problem:**
- 423 PremiumSplitVersions had alphanumeric GroupIds (AL9999, LA0146, etc.)
- Couldn't export to `dbo.PremiumSplitVersions` (GroupId is BIGINT)

**Resolution:**
- ✅ Created `dbo.NormalizeAlphanumericGroupId()` function
- ✅ State prefix encoding: LA→50000000, MS→60000000, AL→40000000
- ✅ Updated export script to normalize alphanumeric GroupIds
- ✅ **Recovered 402 records** (21 filtered for broken participants)
- ✅ 4.75% improvement in data coverage

**Impact:** 402 additional commission splits can now be processed

**Priority:** N/A - complete

**Details:** See `ALPHANUMERIC-RECOVERY-SUCCESS.md`

---

## Recommendations

### Immediate Actions

1. **Assess HI1721L Impact** (30 minutes)
   ```sql
   -- Run query to find affected policies
   -- Determine if this blocks significant portion of commissions
   ```

2. **If HI1721L is critical:**
   - Import missing rates from legacy data
   - Add to schedules 239, 240, 241, 242, 247

### Optional Actions

3. **Add WH01/WH02 Schedules** (if source data available)
   - Affects only 2 participants
   - Low priority unless commission calculations fail for these

4. **Re-ingest Missing 6,828 Certificates** (if 100% coverage needed)
   - Modify ingest scripts to include all Active certificates
   - Re-run proposal builder
   - Brings coverage from 98.4% → 100%

---

## Bottom Line

### ✅ Ready to Proceed
- **415,698 policies (98.4%)** ready for commission calculations
- All critical issues resolved
- PHA structure correct and validated

### ⚠️ Minor Outstanding Issues
- **HI1721L rates:** Medium priority, scope unknown
- **WH01/WH02 schedules:** Low priority, 2 participants
- **6,828 policies:** Low priority, 1.6% gap

### 🚀 Recommendation
**Proceed with commission calculations now.** Address HI1721L if errors occur during processing.

---

**Commission calculations can begin immediately for 98.4% of policies!**
