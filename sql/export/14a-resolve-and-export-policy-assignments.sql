-- =============================================================================
-- Resolve Hierarchies and Export Policy Assignments
-- Resolves HierarchyId based on (GroupId, WritingBrokerId) and exports
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'RESOLVE AND EXPORT: Policy Hierarchy Assignments';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Resolve HierarchyId for staging assignments
-- Match on: GroupId + WritingBrokerId → HierarchyId
-- =============================================================================
PRINT 'Step 1: Resolving HierarchyIds...';

-- Create temp table with resolved hierarchies
IF OBJECT_ID('tempdb..#ResolvedAssignments') IS NOT NULL DROP TABLE #ResolvedAssignments;

SELECT
    pha.PolicyId,
    p.GroupId,
    pha.WritingBrokerId,
    pha.SplitPercent,
    pha.IsNonConforming,
    -- Resolve HierarchyId: Find hierarchy for this group + writing broker
    h.Id AS ResolvedHierarchyId
INTO #ResolvedAssignments
FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments] pha
JOIN [$(PRODUCTION_SCHEMA)].[Policies] p ON p.Id = pha.PolicyId  -- Get GroupId from exported policy
LEFT JOIN [$(PRODUCTION_SCHEMA)].[Hierarchies] h ON (
        -- Try exact match first
        h.GroupId = p.GroupId
        OR
        -- Handle leading zero padding differences: G00000 vs G0000
        -- Convert both to numeric and compare (use TRY_CAST for safety)
        (TRY_CAST(REPLACE(h.GroupId, 'G', '') AS BIGINT) IS NOT NULL
         AND TRY_CAST(REPLACE(p.GroupId, 'G', '') AS BIGINT) IS NOT NULL
         AND TRY_CAST(REPLACE(h.GroupId, 'G', '') AS BIGINT) = TRY_CAST(REPLACE(p.GroupId, 'G', '') AS BIGINT))
    )
    AND h.BrokerId = CAST(pha.WritingBrokerId AS BIGINT)  -- Match on broker
    AND h.Status = 1  -- Only active hierarchies
    AND h.IsDeleted = 0
WHERE pha.PolicyId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Policies])  -- Only for exported policies
  AND p.GroupId IN (SELECT Id FROM [$(ETL_SCHEMA)].[stg_included_groups]);  -- Only included groups

DECLARE @resolved_count INT = @@ROWCOUNT;
PRINT 'Assignments prepared: ' + CAST(@resolved_count AS VARCHAR);

-- Check resolution rate
DECLARE @with_hierarchy INT = (SELECT COUNT(*) FROM #ResolvedAssignments WHERE ResolvedHierarchyId IS NOT NULL);
DECLARE @without_hierarchy INT = (SELECT COUNT(*) FROM #ResolvedAssignments WHERE ResolvedHierarchyId IS NULL);

PRINT 'Resolved with hierarchy: ' + CAST(@with_hierarchy AS VARCHAR);
IF @without_hierarchy > 0
    PRINT 'WARNING: ' + CAST(@without_hierarchy AS VARCHAR) + ' assignments could not resolve hierarchy';

-- =============================================================================
-- Step 2: Export aggregated assignments
-- Aggregate by (PolicyId, HierarchyId, WritingBrokerId) - production unique index
-- =============================================================================
PRINT '';
PRINT 'Step 2: Exporting aggregated assignments...';

;WITH AggregatedAssignments AS (
    SELECT
        CONCAT('PHA-', PolicyId, '-', 
               REPLACE(ResolvedHierarchyId, 'H-', ''), '-', 
               WritingBrokerId) AS Id,
        PolicyId,
        TRY_CAST(PolicyId AS BIGINT) AS CertificateId,
        ResolvedHierarchyId AS HierarchyId,
        SUM(SplitPercent) AS SplitPercent,
        TRY_CAST(WritingBrokerId AS BIGINT) AS WritingBrokerId,
        MAX(CASE WHEN IsNonConforming = 1 THEN 1 ELSE 0 END) AS IsNonConforming
    FROM #ResolvedAssignments
    WHERE ResolvedHierarchyId IS NOT NULL  -- Only export if we found a hierarchy
    GROUP BY PolicyId, ResolvedHierarchyId, WritingBrokerId
)
INSERT INTO [$(PRODUCTION_SCHEMA)].[PolicyHierarchyAssignments] (
    Id,
    PolicyId,
    CertificateId,
    HierarchyId,
    SplitPercent,
    WritingBrokerId,
    IsNonConforming,
    CreationTime,
    IsDeleted
)
SELECT
    aa.Id,
    aa.PolicyId,
    aa.CertificateId,
    aa.HierarchyId,
    aa.SplitPercent,
    aa.WritingBrokerId,
    aa.IsNonConforming,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM AggregatedAssignments aa
WHERE aa.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[PolicyHierarchyAssignments])
  -- Avoid unique constraint violation
  AND NOT EXISTS (
      SELECT 1 FROM [$(PRODUCTION_SCHEMA)].[PolicyHierarchyAssignments] existing
      WHERE existing.PolicyId = aa.PolicyId
        AND existing.HierarchyId = aa.HierarchyId
        AND existing.WritingBrokerId = aa.WritingBrokerId
  );

DECLARE @exported INT = @@ROWCOUNT;
PRINT 'PolicyHierarchyAssignments exported: ' + CAST(@exported AS VARCHAR);

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'Production Total' AS Metric, COUNT(*) AS Count
FROM [$(PRODUCTION_SCHEMA)].[PolicyHierarchyAssignments];

SELECT 'By IsNonConforming' AS Metric,
    SUM(CASE WHEN IsNonConforming = 1 THEN 1 ELSE 0 END) AS NonConforming,
    SUM(CASE WHEN IsNonConforming = 0 OR IsNonConforming IS NULL THEN 1 ELSE 0 END) AS Conforming
FROM [$(PRODUCTION_SCHEMA)].[PolicyHierarchyAssignments];

-- Sample by group
SELECT TOP 10
    'By Group' AS Metric,
    p.GroupId,
    g.GroupName,
    COUNT(*) AS AssignmentCount
FROM [$(PRODUCTION_SCHEMA)].[PolicyHierarchyAssignments] pha
JOIN [$(PRODUCTION_SCHEMA)].[Policies] p ON p.Id = pha.PolicyId
JOIN [$(PRODUCTION_SCHEMA)].[EmployerGroups] g ON g.Id = p.GroupId
GROUP BY p.GroupId, g.GroupName
ORDER BY COUNT(*) DESC;

PRINT '';
PRINT '============================================================';
PRINT 'POLICY HIERARCHY ASSIGNMENTS EXPORT COMPLETED';
PRINT '============================================================';

DROP TABLE IF EXISTS #ResolvedAssignments;
GO
