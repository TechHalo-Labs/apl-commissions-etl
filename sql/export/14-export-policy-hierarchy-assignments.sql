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
        MAX(CASE WHEN IsNonConforming = 1 THEN 1 ELSE 0 END) AS IsNonConforming
    FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments]
    WHERE HierarchyId IS NOT NULL
    GROUP BY PolicyId, HierarchyId, WritingBrokerId
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
