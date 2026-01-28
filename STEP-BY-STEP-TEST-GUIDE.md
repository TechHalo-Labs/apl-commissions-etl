# ETL Transform Steps - Testing & Verification Guide

## Purpose

This guide provides detailed descriptions and test queries for each ETL transform step.
Use this when running `--step-by-step` mode to understand what each step does and how to verify results.

---

## Step 1: References (00-references.sql)

### 📋 Description
Creates foundational reference data for states and products.

### 🎯 Purpose
Establishes lookup tables used throughout the ETL for data validation and enrichment. States are needed for situs state validation, products for policy categorization.

### ✅ Expected Results
- ~50 states/territories in `stg_states`
- ~100-200 product definitions in `stg_products`
- All states should have proper codes (e.g., FL, TX, CA)

### 🔍 Test Queries
```sql
-- Check state data
SELECT Code, Name, Country 
FROM [etl].[stg_states] 
ORDER BY Code;

-- Check product data  
SELECT Code, Name, Category 
FROM [etl].[stg_products] 
ORDER BY Code;

-- Verify no duplicates
SELECT Code, COUNT(*) AS cnt 
FROM [etl].[stg_states] 
GROUP BY Code 
HAVING COUNT(*) > 1;
```

---

## Step 2: Brokers (01-brokers.sql)

### 📋 Description
Transforms broker data from both individual and organization rosters.

### 🎯 Purpose
Creates the master broker registry by combining individual agents and broker organizations. Sets `ExternalPartyId` (`UniquePartyId`) which is the primary identifier for brokers. Ensures all brokers have `Status=Active` for commission processing.

### ✅ Expected Results
- ~12,000 total brokers (mix of individuals and organizations)
- ~95%+ should have `ExternalPartyId` populated
- All brokers should have `Status=0` (Active)
- Names should be properly formatted (not empty)

### 🔍 Test Queries
```sql
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
```

---

## Step 3: Groups (02-groups.sql)

### 📋 Description
Transforms employer groups from PerfGroupModel with primary broker assignment.

### 🎯 Purpose
Creates employer group records with proper names (not generic), group sizes, and links to primary brokers via `BrokerUniqueId` from `raw_perf_groups`. `PrimaryBrokerId` is critical for proposal broker assignment.

### ✅ Expected Results
- ~3,000-3,500 employer groups
- ~95%+ should have `PrimaryBrokerId` populated (from perf groups)
- GroupNames should be real names, not "Group XXXXX"
- `GroupSize` should be > 0 for most groups

### 🔍 Test Queries
```sql
-- Check group data quality
SELECT 
    COUNT(*) AS total_groups,
    SUM(CASE WHEN PrimaryBrokerId IS NOT NULL THEN 1 ELSE 0 END) AS with_primary_broker,
    SUM(CASE WHEN GroupSize > 0 THEN 1 ELSE 0 END) AS with_group_size,
    SUM(CASE WHEN Name NOT LIKE 'Group %' THEN 1 ELSE 0 END) AS with_real_names
FROM [etl].[stg_groups];

-- Check situs state distribution
SELECT SitusState, COUNT(*) AS cnt
FROM [etl].[stg_groups]
GROUP BY SitusState
ORDER BY cnt DESC;

-- Sample groups with broker info
SELECT TOP 10 Id, Code, Name, GroupSize, PrimaryBrokerId, SitusState
FROM [etl].[stg_groups]
WHERE PrimaryBrokerId IS NOT NULL
ORDER BY GroupSize DESC;

-- Groups with missing primary broker (should be rare)
SELECT TOP 20 Id, Code, Name, GroupSize, SitusState
FROM [etl].[stg_groups]
WHERE PrimaryBrokerId IS NULL
ORDER BY GroupSize DESC;
```

---

## Step 4: Products (03-products.sql)

### 📋 Description
Transforms product definitions and categories.

### 🎯 Purpose
Creates product catalog used for policy classification and commission rate lookup. Products link to schedules for commission calculation.

### ✅ Expected Results
- ~100-200 product records
- Products categorized (Dental, Vision, Life, etc.)
- All products should have valid codes

### 🔍 Test Queries
```sql
-- Check product categories
SELECT Category, COUNT(*) AS cnt
FROM [etl].[stg_products]
GROUP BY Category
ORDER BY cnt DESC;

-- Sample products
SELECT TOP 20 Code, Name, Category 
FROM [etl].[stg_products] 
ORDER BY Code;

-- Check for missing data
SELECT 
    SUM(CASE WHEN Code IS NULL OR Code = '' THEN 1 ELSE 0 END) AS missing_code,
    SUM(CASE WHEN Name IS NULL OR Name = '' THEN 1 ELSE 0 END) AS missing_name,
    SUM(CASE WHEN Category IS NULL OR Category = '' THEN 1 ELSE 0 END) AS missing_category
FROM [etl].[stg_products];
```

---

## Step 5: Schedules ⚠️ CRITICAL (04-schedules.sql)

### 📋 Description
⚠️ **CRITICAL**: Transforms commission schedules and rates (must succeed!)

### 🎯 Purpose
Creates commission rate schedules from `raw_schedule_rates`. This step **MUST** find schedules in input data, or downstream steps will fail. Uses permanent work tables to avoid sqlcmd batching issues. Schedule rates define commission percentages for first-year and renewal commissions.

### ✅ Expected Results
- ~600-700 unique schedules
- ~10,000+ schedule rates (first-year + renewal)
- **CRITICAL**: If schedules = 0, ETL has failed - check raw data exists
- Rates should have `FirstYearRate` and `RenewalRate` populated

### 🔍 Test Queries
```sql
-- ⚠️ CRITICAL: Check schedule count (should be > 0!)
SELECT COUNT(*) AS total_schedules 
FROM [etl].[stg_schedules];

-- If 0 schedules, check if raw data exists
SELECT COUNT(*) AS raw_schedule_rates_count 
FROM [etl].[raw_schedule_rates];

SELECT COUNT(*) AS input_certificate_info_count 
FROM [etl].[input_certificate_info];

-- Check schedule rate distribution
SELECT 
    COUNT(*) AS total_rates,
    SUM(CASE WHEN FirstYearRate > 0 THEN 1 ELSE 0 END) AS with_first_year,
    SUM(CASE WHEN RenewalRate > 0 THEN 1 ELSE 0 END) AS with_renewal,
    AVG(CAST(FirstYearRate AS FLOAT)) AS avg_first_year_rate,
    AVG(CAST(RenewalRate AS FLOAT)) AS avg_renewal_rate,
    MIN(FirstYearRate) AS min_first_year,
    MAX(FirstYearRate) AS max_first_year
FROM [etl].[stg_schedule_rates];

-- Sample schedules with rate counts
SELECT TOP 10 s.Id, s.Name, s.ExternalId, COUNT(sr.Id) AS rate_count
FROM [etl].[stg_schedules] s
LEFT JOIN [etl].[stg_schedule_rates] sr ON sr.ScheduleId = s.Id
GROUP BY s.Id, s.Name, s.ExternalId
ORDER BY rate_count DESC;

-- Check for schedules with no rates (should be rare)
SELECT s.Id, s.Name, s.ExternalId
FROM [etl].[stg_schedules] s
LEFT JOIN [etl].[stg_schedule_rates] sr ON sr.ScheduleId = s.Id
WHERE sr.Id IS NULL;
```

---

## Step 6a-g: Proposals (06a-06g series)

### 📋 Description
Multi-step proposal generation using tiered approach.

### 🎯 Purpose
Creates commission agreement proposals for groups using a tiered approach:
1. **Simple Groups** - One proposal per group+product
2. **Non-Conformant** - Groups that don't fit standard patterns
3. **Plan-Differentiated** - Separate proposals per plan code
4. **Year-Differentiated** - Separate proposals per certificate effective year
5. **Granular** - Most granular level (every unique combination)
6. **Consolidation** - Merges where applicable
7. **Normalization** - Sets proper date ranges

### ✅ Expected Results
- ~12,000 total proposals after all steps
- All proposals should have `GroupId`, `BrokerId`, `SitusState`
- Proposals should have proper effective date ranges
- BrokerName should be populated from brokers table

### 🔍 Test Queries
```sql
-- Check proposal counts by source/tier
SELECT 
    [Source],
    COUNT(*) AS proposal_count,
    MIN(ProposedEffectiveDate) AS earliest_effective,
    MAX(ProposedEffectiveDate) AS latest_effective
FROM [etl].[stg_proposals]
GROUP BY [Source]
ORDER BY proposal_count DESC;

-- Check data quality
SELECT 
    COUNT(*) AS total_proposals,
    SUM(CASE WHEN BrokerId IS NOT NULL THEN 1 ELSE 0 END) AS with_broker_id,
    SUM(CASE WHEN BrokerName IS NOT NULL AND BrokerName != '' THEN 1 ELSE 0 END) AS with_broker_name,
    SUM(CASE WHEN GroupId IS NOT NULL AND GroupId != '' THEN 1 ELSE 0 END) AS with_group_id,
    SUM(CASE WHEN SitusState IS NOT NULL THEN 1 ELSE 0 END) AS with_situs_state,
    SUM(CASE WHEN EffectiveDateTo IS NULL THEN 1 ELSE 0 END) AS open_ended
FROM [etl].[stg_proposals];

-- Sample proposals
SELECT TOP 10 
    Id, GroupId, BrokerId, BrokerName, SitusState,
    ProposedEffectiveDate, EffectiveDateTo, [Source]
FROM [etl].[stg_proposals]
ORDER BY ProposedEffectiveDate DESC;

-- Check for issues
SELECT 
    'Missing BrokerId' AS issue,
    COUNT(*) AS count
FROM [etl].[stg_proposals]
WHERE BrokerId IS NULL
UNION ALL
SELECT 
    'Missing GroupId',
    COUNT(*)
FROM [etl].[stg_proposals]
WHERE GroupId IS NULL OR GroupId = ''
UNION ALL
SELECT 
    'Missing SitusState',
    COUNT(*)
FROM [etl].[stg_proposals]
WHERE SitusState IS NULL;
```

---

## Step 7: Hierarchies ⚠️ CRITICAL (07-hierarchies.sql)

### 📋 Description
⚠️ **CRITICAL**: Creates hierarchies with premium splits - must NOT consolidate by `StructureSignature`!

### 🎯 Purpose
Creates commission hierarchies that define how commission dollars are split among brokers. Each `CertSplitSeq` should get its own hierarchy (not consolidated). Hierarchy participants must have `ScheduleId` linked correctly (~95%+ success rate).

### ✅ Expected Results
- ~8,000-10,000 hierarchies (one per unique Group+CertSplitSeq+WritingBroker)
- All hierarchies should have `Status=Active`
- ~95%+ of hierarchy participants should have `ScheduleId` populated
- Each hierarchy should have 1-5 participants (brokers at different tiers)

### 🔍 Test Queries
```sql
-- Check hierarchy counts
SELECT 
    COUNT(*) AS total_hierarchies,
    COUNT(DISTINCT ProposalId) AS unique_proposals,
    SUM(CASE WHEN Status = 'Active' THEN 1 ELSE 0 END) AS active_count
FROM [etl].[stg_hierarchies];

-- ⚠️ CRITICAL: Check schedule linking (should be 95%+)
SELECT 
    COUNT(*) AS total_participants,
    SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) AS with_schedule,
    SUM(CASE WHEN ScheduleId IS NULL THEN 1 ELSE 0 END) AS without_schedule,
    CAST(SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS schedule_link_pct
FROM [etl].[stg_hierarchy_participants];

-- Check participant distribution (should have 1-5 levels typically)
SELECT 
    hp.Level,
    COUNT(*) AS participant_count,
    AVG(CAST(hp.SplitPercent AS FLOAT)) AS avg_split_percent
FROM [etl].[stg_hierarchy_participants] hp
GROUP BY hp.Level
ORDER BY hp.Level;

-- Sample hierarchies with participant counts
SELECT TOP 10 
    h.Id AS HierarchyId,
    h.ProposalId,
    h.BrokerId AS WritingBrokerId,
    h.Status,
    COUNT(hp.Id) AS participant_count,
    SUM(hp.SplitPercent) AS total_split_percent
FROM [etl].[stg_hierarchies] h
LEFT JOIN [etl].[stg_hierarchy_participants] hp ON hp.HierarchyVersionId = h.Id + '-V1'
GROUP BY h.Id, h.ProposalId, h.BrokerId, h.Status
ORDER BY participant_count DESC;

-- Find hierarchies with NULL schedules (for debugging)
SELECT TOP 20 
    hp.HierarchyVersionId,
    hp.EntityId AS BrokerId,
    hp.ScheduleCode,
    hp.ScheduleId
FROM [etl].[stg_hierarchy_participants] hp
WHERE hp.ScheduleId IS NULL
ORDER BY hp.HierarchyVersionId;

-- Verify split percents add up to 100 per hierarchy
SELECT 
    hp.HierarchyVersionId,
    SUM(hp.SplitPercent) AS total_split
FROM [etl].[stg_hierarchy_participants] hp
GROUP BY hp.HierarchyVersionId
HAVING ABS(SUM(hp.SplitPercent) - 100.0) > 1.0;
```

---

## Step 8: Hierarchy Splits (08-hierarchy-splits.sql)

### 📋 Description
Creates premium split versions and participants.

### 🎯 Purpose
Translates hierarchy split configurations into `PremiumSplitVersions` and `PremiumSplitParticipants` tables. These define how premiums are divided (e.g., 50%/50%, 100%, 30%/70%).

### ✅ Expected Results
- ~10,000 split versions
- ~20,000-30,000 split participants
- Total split percent should = 100 for each version

### 🔍 Test Queries
```sql
-- Check split counts
SELECT 
    COUNT(*) AS split_versions,
    COUNT(DISTINCT ProposalId) AS unique_proposals
FROM [etl].[stg_premium_split_versions];

SELECT COUNT(*) AS split_participants 
FROM [etl].[stg_premium_split_participants];

-- Check split distribution
SELECT 
    psv.TotalSplitPercent,
    COUNT(*) AS version_count
FROM [etl].[stg_premium_split_versions] psv
GROUP BY psv.TotalSplitPercent
ORDER BY version_count DESC;

-- Sample splits with participant details
SELECT TOP 10 
    psv.Id AS SplitVersionId,
    psv.ProposalId,
    psv.TotalSplitPercent,
    COUNT(psp.Id) AS participant_count
FROM [etl].[stg_premium_split_versions] psv
LEFT JOIN [etl].[stg_premium_split_participants] psp ON psp.SplitVersionId = psv.Id
GROUP BY psv.Id, psv.ProposalId, psv.TotalSplitPercent
ORDER BY psv.TotalSplitPercent DESC;

-- Find splits with invalid totals (should be rare)
SELECT 
    Id, ProposalId, TotalSplitPercent
FROM [etl].[stg_premium_split_versions]
WHERE ABS(TotalSplitPercent - 100.0) > 1.0
  AND ABS(TotalSplitPercent - 200.0) > 1.0;  -- 200% is valid (two 100% splits)
```

---

## Step 9: Policies (09-policies.sql)

### 📋 Description
Transforms policies/certificates from raw certificate info.

### 🎯 Purpose
Creates policy records from certificate data. Each policy represents an enrolled member with effective dates, product, group, and premium information.

### ✅ Expected Results
- ~1.5M policies
- All policies should have `groupId`, `productId`, `effectiveDate`
- Most policies should have `ProposalId` (from direct matching)

### 🔍 Test Queries
```sql
-- Check policy counts
SELECT 
    COUNT(*) AS total_policies,
    COUNT(DISTINCT groupId) AS unique_groups,
    COUNT(DISTINCT productId) AS unique_products,
    SUM(CASE WHEN ProposalId IS NOT NULL THEN 1 ELSE 0 END) AS with_proposal_link
FROM [etl].[stg_policies];

-- Check policy effective date range
SELECT 
    MIN(effectiveDate) AS earliest_effective,
    MAX(effectiveDate) AS latest_effective,
    COUNT(DISTINCT YEAR(effectiveDate)) AS years_covered
FROM [etl].[stg_policies];

-- Sample policies
SELECT TOP 10 
    Id, groupId, productId, effectiveDate, ProposalId
FROM [etl].[stg_policies]
ORDER BY effectiveDate DESC;

-- Group with most policies
SELECT TOP 10 
    groupId,
    COUNT(*) AS policy_count,
    COUNT(DISTINCT productId) AS product_count
FROM [etl].[stg_policies]
GROUP BY groupId
ORDER BY policy_count DESC;
```

---

## Step 10: Policy Hierarchy Assignments (10-policy-hierarchy-assignments.sql)

### 📋 Description
Creates policy-hierarchy assignments for non-conformant/DTC policies.

### 🎯 Purpose
Links policies that cannot be matched to proposals (non-conformant groups, DTC policies) to specific hierarchies via `PolicyHierarchyAssignments`. Allows commission calculation for policies outside standard proposal flow.

### ✅ Expected Results
- ~92,000 PHA records (only for policies that need custom hierarchy assignment)
- Each PHA should have valid `HierarchyId` and `PolicyId`
- PHA participants should have `ScheduleId` populated

### 🔍 Test Queries
```sql
-- Check PHA counts
SELECT 
    COUNT(*) AS total_assignments,
    COUNT(DISTINCT PolicyId) AS unique_policies,
    COUNT(DISTINCT HierarchyId) AS unique_hierarchies
FROM [etl].[stg_policy_hierarchy_assignments];

-- Check PHA participants
SELECT 
    COUNT(*) AS total_participants,
    SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) AS with_schedule
FROM [etl].[stg_policy_hierarchy_participants];

-- Sample PHAs
SELECT TOP 10 
    pha.Id,
    pha.PolicyId,
    pha.HierarchyId,
    pha.SplitPercent,
    COUNT(phap.Id) AS participant_count
FROM [etl].[stg_policy_hierarchy_assignments] pha
LEFT JOIN [etl].[stg_policy_hierarchy_participants] phap ON phap.PolicyHierarchyAssignmentId = pha.Id
GROUP BY pha.Id, pha.PolicyId, pha.HierarchyId, pha.SplitPercent
ORDER BY participant_count DESC;

-- Check for NULL hierarchies (should be 0)
SELECT COUNT(*) AS pha_with_null_hierarchy
FROM [etl].[stg_policy_hierarchy_assignments]
WHERE HierarchyId IS NULL;
```

---

## Step 11: Special Schedule Rates (11-special-schedule-rates.sql)

### 📋 Description
Creates special year-specific rates when renewal rates differ by year.

### 🎯 Purpose
Populates `special_schedule_rates` table for policies where commission rates change over time (Year 2 != Year 3, Year 3 != Year 4, etc.). Allows commission calculation with year-specific rates.

### ✅ Expected Results
- Variable count depending on schedule complexity
- Each special rate should have `ScheduleId`, `Year`, `Rate`

### 🔍 Test Queries
```sql
-- Check special rate counts
SELECT 
    COUNT(*) AS total_special_rates,
    COUNT(DISTINCT ScheduleId) AS schedules_with_special_rates
FROM [etl].[special_schedule_rates];

-- Sample special rates
SELECT TOP 20 
    ScheduleId,
    Year,
    Rate,
    ProductCode,
    State
FROM [etl].[special_schedule_rates]
ORDER BY ScheduleId, Year;

-- Schedules with most year variations
SELECT 
    ScheduleId,
    COUNT(DISTINCT Year) AS year_variations,
    MIN(Year) AS min_year,
    MAX(Year) AS max_year
FROM [etl].[special_schedule_rates]
GROUP BY ScheduleId
ORDER BY year_variations DESC;
```

---

## Step 12: Audit & Cleanup ⚠️ IMPORTANT (99-audit-and-cleanup.sql)

### 📋 Description
⚠️ **IMPORTANT**: Final data quality audit and cleanup pass.

### 🎯 Purpose
Post-transform audit that validates data quality and fixes any remaining issues:
1. Populates missing `PrimaryBrokerId` on groups (from `raw_perf_groups`)
2. Populates missing `BrokerUniquePartyId`/`BrokerId` on proposals
3. Fixes NULL `ScheduleId` on hierarchy participants (fallback matching)
4. Validates referential integrity

### ✅ Expected Results
- All fixable data quality issues resolved
- ~95%+ completion rates on critical fields
- Detailed audit report showing fixes applied

### 🔍 Test Queries
```sql
-- Final data quality summary
SELECT 
    'Groups' AS entity,
    COUNT(*) AS total,
    SUM(CASE WHEN PrimaryBrokerId IS NOT NULL THEN 1 ELSE 0 END) AS with_primary_broker
FROM [etl].[stg_groups]
UNION ALL
SELECT 
    'Proposals',
    COUNT(*),
    SUM(CASE WHEN BrokerId IS NOT NULL THEN 1 ELSE 0 END)
FROM [etl].[stg_proposals]
UNION ALL
SELECT 
    'Hierarchy Participants',
    COUNT(*),
    SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END)
FROM [etl].[stg_hierarchy_participants];

-- Check for remaining issues
SELECT 
    'Groups missing PrimaryBrokerId' AS issue,
    COUNT(*) AS count
FROM [etl].[stg_groups]
WHERE PrimaryBrokerId IS NULL
UNION ALL
SELECT 
    'Proposals missing BrokerId',
    COUNT(*)
FROM [etl].[stg_proposals]
WHERE BrokerId IS NULL
UNION ALL
SELECT 
    'Hierarchy Participants missing ScheduleId',
    COUNT(*)
FROM [etl].[stg_hierarchy_participants]
WHERE ScheduleId IS NULL;
```

---

## Summary Verification Query

### After All Steps Complete

```sql
-- Complete ETL Summary
SELECT 
    'Brokers' AS entity,
    COUNT(*) AS record_count,
    MIN(CreationTime) AS first_created,
    MAX(CreationTime) AS last_created
FROM [etl].[stg_brokers]
UNION ALL
SELECT 'Groups', COUNT(*), MIN(CreationTime), MAX(CreationTime)
FROM [etl].[stg_groups]
UNION ALL
SELECT 'Schedules', COUNT(*), MIN(CreationTime), MAX(CreationTime)
FROM [etl].[stg_schedules]
UNION ALL
SELECT 'Schedule Rates', COUNT(*), MIN(CreationTime), MAX(CreationTime)
FROM [etl].[stg_schedule_rates]
UNION ALL
SELECT 'Proposals', COUNT(*), MIN(CreationTime), MAX(CreationTime)
FROM [etl].[stg_proposals]
UNION ALL
SELECT 'Hierarchies', COUNT(*), MIN(CreationTime), MAX(CreationTime)
FROM [etl].[stg_hierarchies]
UNION ALL
SELECT 'Hierarchy Participants', COUNT(*), MIN(CreationTime), MAX(CreationTime)
FROM [etl].[stg_hierarchy_participants]
UNION ALL
SELECT 'Policies', COUNT(*), MIN(CreationTime), MAX(CreationTime)
FROM [etl].[stg_policies]
UNION ALL
SELECT 'Policy Hierarchy Assignments', COUNT(*), MIN(CreationTime), MAX(CreationTime)
FROM [etl].[stg_policy_hierarchy_assignments]
ORDER BY entity;
```

---

## Critical Success Criteria

✅ **Must Pass:**
1. Schedules > 500 (ideally 600-700)
2. Schedule linking on hierarchies ≥ 95%
3. Groups with PrimaryBrokerId ≥ 95%
4. Proposals with BrokerId ≥ 95%
5. All hierarchies Status = Active
6. Split percentages sum to 100 (or 200 for multi-split)

⚠️ **If Any Fail:**
- Check raw data ingest completed successfully
- Review audit script output for specific issues
- Check transformation logs for errors
- Verify source schema has data (new_data or poc_etl)

---

## Quick Reference: Common Issues

| Issue | Symptom | Fix |
|-------|---------|-----|
| **No Schedules** | `stg_schedules` = 0 | Check `raw_schedule_rates` populated |
| **NULL ScheduleId** | < 95% linked | Audit script should fix with fallback matching |
| **Missing PrimaryBrokerId** | Groups without broker | Audit script populates from `raw_perf_groups` |
| **NULL ProposalId** | Policies can't resolve | Normal for non-conformant - use PHA instead |
| **Inactive Status** | Status != 0 | All should be Active (0) after transforms |

---

## Using This Guide

1. **Before Running:** Review expected results for each step
2. **During Execution:** Check verification output matches expectations
3. **If Step Fails:** Run test queries to diagnose the issue
4. **After Completion:** Run summary query to verify entire ETL

**See Also:**
- `EXECUTION-MODES-GUIDE.md` - How to run step-by-step mode
- `QUICK-REFERENCE.md` - Command cheat sheet
- `INGEST-STEP-BY-STEP-GUIDE.md` - Ingest phase testing
