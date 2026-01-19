-- =============================================================================
-- Transform: Policy Hierarchy Assignments (T-SQL)
-- Creates hierarchy assignments for non-conformant policies (DTC + non-conformant groups)
-- Captures the actual hierarchy structure from raw certificate data
-- Usage: sqlcmd -S server -d database -i sql/transforms/11-policy-hierarchy-assignments.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: Policy Hierarchy Assignments';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Identify non-conformant policies
-- DTC (GroupId = G00000) + policies in non-conformant groups
-- =============================================================================
PRINT 'Step 1: Identifying non-conformant policies...';

DROP TABLE IF EXISTS #tmp_nonconformant_policies;

SELECT 
    p.Id AS PolicyId,
    TRY_CAST(p.Id AS BIGINT) AS CertificateId,
    p.GroupId,
    g.IsNonConformant,
    CASE 
        WHEN p.GroupId = 'G00000' THEN 'DTC-NoGroup'
        WHEN g.IsNonConformant = 1 THEN 'NonConformant-SplitMismatch'
        WHEN p.ProposalId IS NULL THEN 'NoProposal'
        ELSE 'Unknown'
    END AS NonConformantReason
INTO #tmp_nonconformant_policies
FROM [etl].[stg_policies] p
LEFT JOIN [etl].[stg_groups] g ON g.Id = p.GroupId
WHERE p.GroupId = 'G00000'  -- DTC policies
   OR g.IsNonConformant = 1  -- Non-conformant groups
   OR p.ProposalId IS NULL;  -- Any policy without a proposal

DECLARE @nonconf_count INT = @@ROWCOUNT;
PRINT 'Non-conformant policies identified: ' + CAST(@nonconf_count AS VARCHAR);

-- =============================================================================
-- Step 2: Extract hierarchy assignments from raw certificate data
-- One row per (CertificateId, CertSplitSeq, WritingBrokerId)
-- =============================================================================
PRINT '';
PRINT 'Step 2: Extracting hierarchy assignments from raw certificate data...';

DROP TABLE IF EXISTS #tmp_hierarchy_assignments;

SELECT DISTINCT
    CONCAT('PHA-', ci.CertificateId, '-', ci.CertSplitSeq, '-', REPLACE(ci.WritingBrokerID, 'P', '')) AS Id,
    CAST(ci.CertificateId AS NVARCHAR(100)) AS PolicyId,
    TRY_CAST(ci.CertificateId AS BIGINT) AS CertificateId,
    ci.CertSplitSeq AS SplitSequence,
    ci.CertSplitPercent AS SplitPercent,
    TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) AS WritingBrokerId,
    ncp.NonConformantReason
INTO #tmp_hierarchy_assignments
FROM [etl].[input_certificate_info] ci
INNER JOIN #tmp_nonconformant_policies ncp ON ncp.CertificateId = TRY_CAST(ci.CertificateId AS BIGINT)
WHERE ci.SplitBrokerSeq = 1  -- Get the assignment level (level 1 participant defines the assignment)
  AND ci.WritingBrokerID IS NOT NULL 
  AND ci.WritingBrokerID <> ''
  AND ci.RecStatus = 'A'  -- Only active split configurations
  AND TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) IS NOT NULL;

DECLARE @assign_count INT = @@ROWCOUNT;
PRINT 'Hierarchy assignments extracted: ' + CAST(@assign_count AS VARCHAR);

-- =============================================================================
-- Step 3: Truncate and populate stg_policy_hierarchy_assignments
-- =============================================================================
PRINT '';
PRINT 'Step 3: Populating stg_policy_hierarchy_assignments...';

TRUNCATE TABLE [etl].[stg_policy_hierarchy_assignments];

-- Use CTE to deduplicate and pick best matching hierarchy
;WITH assignments_with_hierarchy AS (
    SELECT
        ha.Id,
        ha.PolicyId,
        ha.CertificateId,
        h.Id AS HierarchyId,
        COALESCE(ha.SplitPercent, 0) AS SplitPercent,
        ha.WritingBrokerId,
        ha.SplitSequence,
        ha.NonConformantReason,
        ROW_NUMBER() OVER (PARTITION BY ha.Id ORDER BY h.Id) AS rn
    FROM #tmp_hierarchy_assignments ha
    LEFT JOIN [etl].[stg_policies] p ON p.Id = ha.PolicyId
    LEFT JOIN [etl].[stg_hierarchies] h ON h.GroupId = p.GroupId AND h.BrokerId = ha.WritingBrokerId
)
INSERT INTO [etl].[stg_policy_hierarchy_assignments] (
    Id, PolicyId, CertificateId, HierarchyId, SplitPercent, WritingBrokerId,
    SplitSequence, IsNonConforming, NonConformantReason, CreationTime, IsDeleted
)
SELECT
    Id,
    PolicyId,
    CertificateId,
    HierarchyId,
    SplitPercent,
    WritingBrokerId,
    SplitSequence,
    1 AS IsNonConforming,
    NonConformantReason,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM assignments_with_hierarchy
WHERE rn = 1;

PRINT 'Policy hierarchy assignments staged: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 4: Extract hierarchy participants for each assignment
-- One row per (Assignment, SplitBrokerSeq)
-- =============================================================================
PRINT '';
PRINT 'Step 4: Extracting hierarchy participants...';

DROP TABLE IF EXISTS #tmp_participants;

SELECT
    CONCAT(pha.Id, '-L', ci.SplitBrokerSeq) AS Id,
    pha.Id AS PolicyHierarchyAssignmentId,
    TRY_CAST(REPLACE(ci.SplitBrokerId, 'P', '') AS BIGINT) AS BrokerId,
    b.Name AS BrokerName,
    ci.SplitBrokerSeq AS [Level],
    -- Get commission rate from commission details if available
    COALESCE(cd.RealCommissionRate, cd.CommissionRate, 0) AS CommissionRate,
    ci.CommissionsSchedule AS ScheduleCode,
    ci.ReassignedType,
    TRY_CAST(REPLACE(ci.PaidBrokerId, 'P', '') AS BIGINT) AS PaidBrokerId
INTO #tmp_participants
FROM [etl].[stg_policy_hierarchy_assignments] pha
INNER JOIN [etl].[input_certificate_info] ci 
    ON TRY_CAST(ci.CertificateId AS BIGINT) = pha.CertificateId
    AND ci.CertSplitSeq = pha.SplitSequence
    AND TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) = pha.WritingBrokerId
LEFT JOIN [etl].[stg_brokers] b ON b.Id = TRY_CAST(REPLACE(ci.SplitBrokerId, 'P', '') AS BIGINT)
LEFT JOIN [etl].[input_commission_details] cd 
    ON cd.CertificateId = ci.CertificateId
    AND cd.SplitBrokerId = ci.SplitBrokerId
WHERE ci.SplitBrokerId IS NOT NULL 
  AND ci.SplitBrokerId <> ''
  AND TRY_CAST(REPLACE(ci.SplitBrokerId, 'P', '') AS BIGINT) IS NOT NULL;

DECLARE @part_count INT = @@ROWCOUNT;
PRINT 'Participants extracted: ' + CAST(@part_count AS VARCHAR);

-- =============================================================================
-- Step 5: Truncate and populate stg_policy_hierarchy_participants
-- =============================================================================
PRINT '';
PRINT 'Step 5: Populating stg_policy_hierarchy_participants...';

TRUNCATE TABLE [etl].[stg_policy_hierarchy_participants];

-- Deduplicate participants (same broker can appear in multiple commission detail rows)
-- First aggregate, then deduplicate
;WITH aggregated_participants AS (
    SELECT 
        Id,
        PolicyHierarchyAssignmentId,
        BrokerId,
        BrokerName,
        [Level],
        MAX(CommissionRate) AS CommissionRate,
        MAX(ScheduleCode) AS ScheduleCode,
        MAX(ReassignedType) AS ReassignedType,
        MAX(PaidBrokerId) AS PaidBrokerId
    FROM #tmp_participants
    GROUP BY Id, PolicyHierarchyAssignmentId, BrokerId, BrokerName, [Level]
),
deduped_participants AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY Id ORDER BY CommissionRate DESC) AS rn
    FROM aggregated_participants
)
INSERT INTO [etl].[stg_policy_hierarchy_participants] (
    Id, PolicyHierarchyAssignmentId, BrokerId, BrokerName, [Level],
    CommissionRate, ScheduleCode, ScheduleId, ReassignedType, PaidBrokerId,
    CreationTime, IsDeleted
)
SELECT
    dp.Id,
    dp.PolicyHierarchyAssignmentId,
    dp.BrokerId,
    dp.BrokerName,
    dp.[Level],
    dp.CommissionRate,
    dp.ScheduleCode,
    s.Id AS ScheduleId,  -- Link to schedule via ExternalId
    dp.ReassignedType,
    dp.PaidBrokerId,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM deduped_participants dp
LEFT JOIN [etl].[stg_schedules] s ON s.ExternalId = dp.ScheduleCode
WHERE dp.rn = 1;

PRINT 'Policy hierarchy participants staged: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 6: Create State Rules for Policy Hierarchy Assignments
-- For each linked hierarchy, create state rules with ALL states
-- =============================================================================
PRINT '';
PRINT 'Step 6: Creating state rules for policy hierarchy assignments...';

-- Get all US states
DROP TABLE IF EXISTS #tmp_all_states;
SELECT StateCode INTO #tmp_all_states
FROM (VALUES
    ('AL'),('AK'),('AZ'),('AR'),('CA'),('CO'),('CT'),('DE'),('FL'),('GA'),
    ('HI'),('ID'),('IL'),('IN'),('IA'),('KS'),('KY'),('LA'),('ME'),('MD'),
    ('MA'),('MI'),('MN'),('MS'),('MO'),('MT'),('NE'),('NV'),('NH'),('NJ'),
    ('NM'),('NY'),('NC'),('ND'),('OH'),('OK'),('OR'),('PA'),('RI'),('SC'),
    ('SD'),('TN'),('TX'),('UT'),('VT'),('VA'),('WA'),('WV'),('WI'),('WY'),('DC')
) AS States(StateCode);

-- Get unique (HierarchyVersionId) for assignments that have a linked hierarchy
DROP TABLE IF EXISTS #tmp_pha_hierarchy_versions;
SELECT DISTINCT
    pha.HierarchyId,
    hv.Id AS HierarchyVersionId
INTO #tmp_pha_hierarchy_versions
FROM [etl].[stg_policy_hierarchy_assignments] pha
INNER JOIN [etl].[stg_hierarchy_versions] hv ON hv.HierarchyId = pha.HierarchyId
WHERE pha.HierarchyId IS NOT NULL;

-- Create state rules for all states (if not already exists)
INSERT INTO [etl].[stg_state_rules] (
    Id, HierarchyVersionId, ShortName, Name, [Description], [Type], SortOrder,
    CreationTime, IsDeleted
)
SELECT
    CONCAT('SR-', hv.HierarchyVersionId, '-', s.StateCode) AS Id,
    hv.HierarchyVersionId,
    s.StateCode AS ShortName,
    s.StateCode AS Name,
    CONCAT('State rule for ', s.StateCode, ' (PHA-generated)') AS [Description],
    0 AS [Type],  -- 0=Include
    ROW_NUMBER() OVER (PARTITION BY hv.HierarchyVersionId ORDER BY s.StateCode) AS SortOrder,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #tmp_pha_hierarchy_versions hv
CROSS JOIN #tmp_all_states s
WHERE NOT EXISTS (
    SELECT 1 FROM [etl].[stg_state_rules] sr
    WHERE sr.Id = CONCAT('SR-', hv.HierarchyVersionId, '-', s.StateCode)
);

DECLARE @pha_state_rules_count INT = @@ROWCOUNT;
PRINT 'State rules created for PHA hierarchies: ' + CAST(@pha_state_rules_count AS VARCHAR);

-- Create corresponding state rule states
INSERT INTO [etl].[stg_state_rule_states] (
    Id, StateRuleId, StateCode, StateName, CreationTime, IsDeleted
)
SELECT
    CONCAT(sr.Id, '-', sr.ShortName) AS Id,
    sr.Id AS StateRuleId,
    sr.ShortName AS StateCode,
    sr.ShortName AS StateName,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[stg_state_rules] sr
WHERE sr.[Description] LIKE '%PHA-generated%'
  AND NOT EXISTS (
    SELECT 1 FROM [etl].[stg_state_rule_states] srs
    WHERE srs.Id = CONCAT(sr.Id, '-', sr.ShortName)
);

DECLARE @pha_state_rule_states_count INT = @@ROWCOUNT;
PRINT 'State rule states created for PHA hierarchies: ' + CAST(@pha_state_rule_states_count AS VARCHAR);

-- =============================================================================
-- Step 7: Create Hierarchy Splits for each policy's product code
-- Uses the schedule from raw_certificate_info
-- =============================================================================
PRINT '';
PRINT 'Step 7: Creating hierarchy splits for policy products...';

-- Get unique (HierarchyVersionId, ProductCode, ScheduleCode) from PHA policies
DROP TABLE IF EXISTS #tmp_pha_products;
SELECT DISTINCT
    pha.HierarchyId,
    hv.Id AS HierarchyVersionId,
    p.ProductCode,
    p.[State] AS PolicyState,
    ci.CommissionsSchedule AS ScheduleCode
INTO #tmp_pha_products
FROM [etl].[stg_policy_hierarchy_assignments] pha
INNER JOIN [etl].[stg_policies] p ON p.Id = pha.PolicyId
INNER JOIN [etl].[stg_hierarchy_versions] hv ON hv.HierarchyId = pha.HierarchyId
INNER JOIN [etl].[input_certificate_info] ci 
    ON ci.CertificateId = pha.PolicyId
    AND ci.CertSplitSeq = pha.SplitSequence
WHERE pha.HierarchyId IS NOT NULL
  AND p.ProductCode IS NOT NULL
  AND p.ProductCode <> '';

-- Create hierarchy splits for each product in ALL state rules
-- This ensures the product is available for any state
INSERT INTO [etl].[stg_hierarchy_splits] (
    Id, StateRuleId, ProductId, ProductCode, ProductName, 
    SortOrder, CreationTime, IsDeleted
)
SELECT DISTINCT
    CONCAT(sr.Id, '-', pp.ProductCode) AS Id,
    sr.Id AS StateRuleId,
    pp.ProductCode AS ProductId,
    pp.ProductCode AS ProductCode,
    COALESCE(prod.ProductName, pp.ProductCode) AS ProductName,
    1 AS SortOrder,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #tmp_pha_products pp
INNER JOIN [etl].[stg_state_rules] sr 
    ON sr.HierarchyVersionId = pp.HierarchyVersionId
LEFT JOIN [etl].[stg_products] prod ON prod.ProductCode = pp.ProductCode
WHERE NOT EXISTS (
    SELECT 1 FROM [etl].[stg_hierarchy_splits] hs
    WHERE hs.Id = CONCAT(sr.Id, '-', pp.ProductCode)
);

DECLARE @pha_splits_count INT = @@ROWCOUNT;
PRINT 'Hierarchy splits created for PHA products: ' + CAST(@pha_splits_count AS VARCHAR);

-- Cleanup temp tables
DROP TABLE IF EXISTS #tmp_all_states;
DROP TABLE IF EXISTS #tmp_pha_hierarchy_versions;
DROP TABLE IF EXISTS #tmp_pha_products;

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'Policy Hierarchy Assignments' AS entity, COUNT(*) AS cnt 
FROM [etl].[stg_policy_hierarchy_assignments];

SELECT 'Policy Hierarchy Participants' AS entity, COUNT(*) AS cnt 
FROM [etl].[stg_policy_hierarchy_participants];

-- Breakdown by non-conformant reason
SELECT 'Assignments by reason' AS metric, NonConformantReason, COUNT(*) AS cnt
FROM [etl].[stg_policy_hierarchy_assignments]
GROUP BY NonConformantReason
ORDER BY cnt DESC;

-- Assignments with linked hierarchies
SELECT 'Assignments with linked hierarchy' AS metric,
    SUM(CASE WHEN HierarchyId IS NOT NULL THEN 1 ELSE 0 END) AS with_hierarchy,
    SUM(CASE WHEN HierarchyId IS NULL THEN 1 ELSE 0 END) AS without_hierarchy
FROM [etl].[stg_policy_hierarchy_assignments];

-- Participants with schedules
SELECT 'Participants with schedule' AS metric,
    SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) AS with_schedule,
    SUM(CASE WHEN ScheduleId IS NULL THEN 1 ELSE 0 END) AS without_schedule
FROM [etl].[stg_policy_hierarchy_participants];

-- State rules created for PHA
SELECT 'State rules (PHA-generated)' AS metric, COUNT(*) AS cnt
FROM [etl].[stg_state_rules]
WHERE [Description] LIKE '%PHA-generated%';

-- Hierarchy splits count
SELECT 'Total Hierarchy Splits' AS metric, COUNT(*) AS cnt
FROM [etl].[stg_hierarchy_splits];

-- Cleanup temp tables
DROP TABLE IF EXISTS #tmp_nonconformant_policies;
DROP TABLE IF EXISTS #tmp_hierarchy_assignments;
DROP TABLE IF EXISTS #tmp_participants;

PRINT '';
PRINT '============================================================';
PRINT 'POLICY HIERARCHY ASSIGNMENTS TRANSFORM COMPLETED';
PRINT '============================================================';

GO

