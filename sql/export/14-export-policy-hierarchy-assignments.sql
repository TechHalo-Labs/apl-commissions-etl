-- =============================================================================
-- Export: Policy Hierarchy Assignments
-- Exports stg_policy_hierarchy_assignments to dbo.PolicyHierarchyAssignments
-- Uses additive INSERT - does not update existing records
-- Production HierarchyId is NOT NULL - must filter out records without hierarchy
-- Aggregates multiple splits for same (PolicyId, HierarchyId, WritingBrokerId)
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'EXPORT: Policy Hierarchy Assignments';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 0: Ensure all PHA hierarchies exist in production (skip this step for now)
-- =============================================================================
PRINT 'Step 0: Creating missing PHA hierarchies...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[Hierarchies] (
    Id, Name, Status, EffectiveDate, CreationTime, IsDeleted,
    Type, HasOverrides, DeviationCount
)
SELECT DISTINCT
    h.Id,
    h.Name,
    h.Status,
    h.EffectiveDate,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted,
    0 AS Type,  -- Default type
    0 AS HasOverrides,  -- Default value
    0 AS DeviationCount  -- Default value
FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments] pha
INNER JOIN [$(ETL_SCHEMA)].[stg_hierarchies] h ON h.Id = pha.HierarchyId
WHERE pha.HierarchyId NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Hierarchies]);

DECLARE @created_hierarchies INT = @@ROWCOUNT;
PRINT 'Created ' + CAST(@created_hierarchies AS VARCHAR) + ' missing PHA hierarchies';

-- Also create missing hierarchy versions
INSERT INTO [$(PRODUCTION_SCHEMA)].[HierarchyVersions] (
    Id, HierarchyId, Version, Status, EffectiveFrom, EffectiveTo, CreationTime, IsDeleted
)
SELECT DISTINCT
    hv.Id,
    hv.HierarchyId,
    hv.Version,
    hv.Status,
    hv.EffectiveFrom,
    hv.EffectiveTo,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments] pha
INNER JOIN [$(ETL_SCHEMA)].[stg_hierarchy_versions] hv ON hv.HierarchyId = pha.HierarchyId
WHERE hv.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[HierarchyVersions]);

DECLARE @created_versions INT = @@ROWCOUNT;
PRINT 'Created ' + CAST(@created_versions AS VARCHAR) + ' missing hierarchy versions';

-- Also create missing hierarchy participants
INSERT INTO [$(PRODUCTION_SCHEMA)].[HierarchyParticipants] (
    Id, HierarchyVersionId, EntityId, EntityName, Level, SortOrder, ScheduleCode, ScheduleId, CreationTime, IsDeleted
)
SELECT DISTINCT
    hp.Id,
    hp.HierarchyVersionId,
    hp.EntityId,
    hp.EntityName,
    hp.Level,
    1 AS SortOrder,  -- Default sort order
    hp.ScheduleCode,
    hp.ScheduleId,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments] pha
INNER JOIN [$(ETL_SCHEMA)].[stg_hierarchy_participants] hp ON hp.HierarchyVersionId IN (
    SELECT hv.Id FROM [$(ETL_SCHEMA)].[stg_hierarchy_versions] hv WHERE hv.HierarchyId = pha.HierarchyId
)
WHERE hp.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[HierarchyParticipants]);

DECLARE @created_participants INT = @@ROWCOUNT;
PRINT 'Created ' + CAST(@created_participants AS VARCHAR) + ' missing hierarchy participants';

PRINT '';

-- =============================================================================
-- Step 1: Export PolicyHierarchyAssignments
-- Production has unique index on (PolicyId, HierarchyId, WritingBrokerId)
-- so we aggregate multiple split sequences into one row per combination
-- =============================================================================
PRINT 'Step 1: Exporting PolicyHierarchyAssignments (aggregated by unique key)...';

;WITH AggregatedAssignments AS (
    SELECT
        -- Generate unique Id based on the combination
        CONCAT('PHA-', PolicyId, '-', 
               REPLACE(HierarchyId, 'H-', ''), '-', 
               WritingBrokerId) AS Id,
        PolicyId,
        TRY_CAST(PolicyId AS BIGINT) AS CertificateId,
        HierarchyId,
        -- Sum split percentages for duplicate combinations
        SUM(SplitPercent) AS SplitPercent,
        TRY_CAST(WritingBrokerId AS BIGINT) AS WritingBrokerId,
        MAX(CASE WHEN IsNonConforming = 1 THEN 1 ELSE 0 END) AS IsNonConforming,
        MAX(COALESCE(EntryType, 0)) AS EntryType
    FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments] pha
    INNER JOIN [$(ETL_SCHEMA)].[stg_policies] p ON p.Id = pha.PolicyId
    WHERE pha.HierarchyId IS NOT NULL
      -- Exclude groups flagged in stg_excluded_groups
      AND (
        p.GroupId IS NULL 
        OR p.GroupId = ''
        OR CONCAT('G', p.GroupId) NOT IN (SELECT GroupId FROM [$(ETL_SCHEMA)].[stg_excluded_groups])
      )
    GROUP BY pha.PolicyId, pha.HierarchyId, pha.WritingBrokerId
)
INSERT INTO [$(PRODUCTION_SCHEMA)].[PolicyHierarchyAssignments] (
    Id,
    PolicyId,
    CertificateId,
    HierarchyId,
    SplitPercent,
    WritingBrokerId,
    IsNonConforming,
    EntryType,
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
    aa.EntryType,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM AggregatedAssignments aa
WHERE aa.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[PolicyHierarchyAssignments])
  -- HierarchyId must exist in production
  AND aa.HierarchyId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Hierarchies])
  -- PolicyId must exist in production Policies table
  AND aa.PolicyId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Policies])
  -- WritingBrokerId must exist in production Brokers table
  AND aa.WritingBrokerId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Brokers])
  -- Avoid the unique constraint violation
  AND NOT EXISTS (
      SELECT 1 FROM [$(PRODUCTION_SCHEMA)].[PolicyHierarchyAssignments] existing
      WHERE existing.PolicyId = aa.PolicyId
        AND existing.HierarchyId = aa.HierarchyId
        AND existing.WritingBrokerId = aa.WritingBrokerId
  );

DECLARE @pha_count INT = @@ROWCOUNT;
PRINT 'PolicyHierarchyAssignments exported: ' + CAST(@pha_count AS VARCHAR);

-- Report staging counts
DECLARE @staging_total INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments]);
PRINT 'Staging total: ' + CAST(@staging_total AS VARCHAR);

-- Report skipped records due to NULL HierarchyId
DECLARE @skipped_null_hierarchy INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments] pha
    WHERE pha.HierarchyId IS NULL
);
IF @skipped_null_hierarchy > 0
    PRINT 'INFO: ' + CAST(@skipped_null_hierarchy AS VARCHAR) + ' assignments skipped (NULL HierarchyId - policy uses direct rates)';

-- Report skipped records due to missing Hierarchy
DECLARE @skipped_hierarchy INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments] pha
    WHERE pha.HierarchyId IS NOT NULL
      AND pha.HierarchyId NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Hierarchies])
);
IF @skipped_hierarchy > 0
    PRINT 'WARNING: ' + CAST(@skipped_hierarchy AS VARCHAR) + ' assignments skipped (HierarchyId not in production)';

-- Report skipped records due to missing Policy
DECLARE @skipped_policy INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments] pha
    WHERE pha.PolicyId NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Policies])
);
IF @skipped_policy > 0
    PRINT 'WARNING: ' + CAST(@skipped_policy AS VARCHAR) + ' assignments skipped (PolicyId not in production)';

-- Report skipped records due to missing Broker
DECLARE @skipped_broker INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments] pha
    WHERE TRY_CAST(pha.WritingBrokerId AS BIGINT) NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Brokers])
      AND pha.PolicyId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Policies])
      AND pha.HierarchyId IS NOT NULL
);
IF @skipped_broker > 0
    PRINT 'WARNING: ' + CAST(@skipped_broker AS VARCHAR) + ' assignments skipped (WritingBrokerId not in production)';

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'Production PolicyHierarchyAssignments' AS tbl, COUNT(*) AS cnt 
FROM [$(PRODUCTION_SCHEMA)].[PolicyHierarchyAssignments];

SELECT 'By IsNonConforming' AS metric,
    SUM(CASE WHEN IsNonConforming = 1 THEN 1 ELSE 0 END) AS nonconforming,
    SUM(CASE WHEN IsNonConforming = 0 THEN 1 ELSE 0 END) AS conforming
FROM [$(PRODUCTION_SCHEMA)].[PolicyHierarchyAssignments];

PRINT '';
PRINT '============================================================';
PRINT 'POLICY HIERARCHY ASSIGNMENTS EXPORT COMPLETED';
PRINT '============================================================';

GO
