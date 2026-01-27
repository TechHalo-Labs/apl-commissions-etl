-- =============================================================================
-- Post-Transform Fix: Link Hierarchies to ALL Matching Proposals
-- 
-- Problem: ETL creates hierarchies but only links them to ONE proposal per group.
-- This causes 77.7% of proposals (9,798 out of 12,615) to have NO hierarchies.
--
-- Solution: After hierarchy creation, create additional hierarchy records
-- for proposals that don't have hierarchies but have matching date ranges.
--
-- Strategy:
-- 1. Find proposals without hierarchies
-- 2. Find candidate hierarchies (same group, date range matches)
-- 3. Create duplicate hierarchy records linked to those proposals
-- 4. Copy hierarchy versions and participants
-- =============================================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;

PRINT '============================================================';
PRINT 'POST-TRANSFORM FIX: Link Hierarchies to ALL Matching Proposals';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Identify proposals without hierarchies that have candidate hierarchies
-- =============================================================================
PRINT 'Step 1: Identifying proposals without hierarchies...';

DROP TABLE IF EXISTS #proposals_needing_hierarchies;

SELECT 
    p.Id AS ProposalId,
    p.ProposalNumber,
    p.GroupId,
    p.EffectiveDateFrom,
    p.EffectiveDateTo,
    -- Find best matching hierarchy for this proposal
    (SELECT TOP 1 h.Id 
     FROM [etl].[stg_hierarchies] h 
     WHERE h.GroupId = p.GroupId
       AND h.EffectiveDate IS NOT NULL
       AND (
           -- Hierarchy date within proposal range
           (p.EffectiveDateFrom IS NOT NULL 
            AND (p.EffectiveDateTo IS NULL OR p.EffectiveDateTo >= h.EffectiveDate)
            AND p.EffectiveDateFrom <= h.EffectiveDate)
           OR
           -- Open-ended proposal: hierarchy date >= proposal start
           (p.EffectiveDateTo IS NULL 
            AND p.EffectiveDateFrom IS NOT NULL
            AND h.EffectiveDate >= p.EffectiveDateFrom)
       )
     ORDER BY h.EffectiveDate DESC) AS SourceHierarchyId
INTO #proposals_needing_hierarchies
FROM [etl].[stg_proposals] p
WHERE NOT EXISTS (
    SELECT 1 FROM [etl].[stg_hierarchies] h 
    WHERE h.ProposalId = p.Id
);

DECLARE @proposals_needing_hierarchies_count INT = (SELECT COUNT(*) FROM #proposals_needing_hierarchies WHERE SourceHierarchyId IS NOT NULL);
PRINT 'Found ' + CAST(@proposals_needing_hierarchies_count AS VARCHAR) + ' proposals that can be linked to existing hierarchies';
PRINT '';

-- =============================================================================
-- Step 2: Create duplicate hierarchies for proposals (COMMENTED OUT)
-- =============================================================================
PRINT 'Step 2: Creating duplicate hierarchies for proposals...';
PRINT '';
PRINT '⚠️  WARNING: The INSERT statements below are COMMENTED OUT for safety.';
PRINT '    Review the preview above, then uncomment to execute.';
PRINT '';

/*
-- Create new hierarchy records for proposals without hierarchies
INSERT INTO [etl].[stg_hierarchies] (
    Id, Name, [Description], [Type], [Status], ProposalId, ProposalNumber, GroupId, GroupName,
    GroupNumber, BrokerId, BrokerName, BrokerLevel, ContractId, SourceType,
    SitusState, EffectiveDate, CurrentVersionId, CurrentVersionNumber, CreationTime, IsDeleted
)
SELECT 
    CONCAT(sh.Id, '-P', REPLACE(pnh.ProposalId, 'P-', '')) AS Id,  -- Unique ID: H-G123-1-P-G123-C2
    sh.Name,
    sh.[Description],
    sh.[Type],
    sh.[Status],
    pnh.ProposalId AS ProposalId,
    pnh.ProposalNumber AS ProposalNumber,
    sh.GroupId,
    sh.GroupName,
    sh.GroupNumber,
    sh.BrokerId,
    sh.BrokerName,
    sh.BrokerLevel,
    sh.ContractId,
    'Migration-Duplicate' AS SourceType,
    sh.SitusState,
    sh.EffectiveDate,
    CONCAT(sh.Id, '-P', REPLACE(pnh.ProposalId, 'P-', ''), '-V1') AS CurrentVersionId,
    sh.CurrentVersionNumber,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[stg_hierarchies] sh
INNER JOIN #proposals_needing_hierarchies pnh ON pnh.SourceHierarchyId = sh.Id
WHERE pnh.SourceHierarchyId IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM [etl].[stg_hierarchies] h2 
      WHERE h2.ProposalId = pnh.ProposalId
  );

DECLARE @new_hierarchies_created INT = @@ROWCOUNT;
PRINT 'Created ' + CAST(@new_hierarchies_created AS VARCHAR) + ' duplicate hierarchy records';
PRINT '';

-- Copy hierarchy versions for new hierarchies
INSERT INTO [etl].[stg_hierarchy_versions] (
    Id, HierarchyId, [Version], [Status], EffectiveFrom, EffectiveTo, ChangeReason, CreationTime, IsDeleted
)
SELECT 
    CONCAT(nh.Id, '-V1') AS Id,
    nh.Id AS HierarchyId,
    shv.[Version],
    shv.[Status],
    shv.EffectiveFrom,
    shv.EffectiveTo,
    'Copied from ' + shv.HierarchyId AS ChangeReason,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[stg_hierarchies] nh
INNER JOIN [etl].[stg_hierarchies] sh ON sh.Id = nh.Id
INNER JOIN [etl].[stg_hierarchy_versions] shv ON shv.HierarchyId = sh.Id
WHERE nh.SourceType = 'Migration-Duplicate'
  AND NOT EXISTS (
      SELECT 1 FROM [etl].[stg_hierarchy_versions] hv2 
      WHERE hv2.HierarchyId = nh.Id
  );

PRINT 'Created hierarchy versions for duplicate hierarchies';
PRINT '';

-- Copy hierarchy participants for new hierarchies
INSERT INTO [etl].[stg_hierarchy_participants] (
    Id, HierarchyVersionId, EntityId, EntityName, [Level], SortOrder,
    ScheduleCode, ScheduleId, CommissionRate, PaidBrokerId, CreationTime, IsDeleted
)
SELECT 
    CONCAT(nhv.Id, '-P', CAST(shp.EntityId AS VARCHAR), '-L', CAST(shp.[Level] AS VARCHAR)) AS Id,
    nhv.Id AS HierarchyVersionId,
    shp.EntityId,
    shp.EntityName,
    shp.[Level],
    shp.SortOrder,
    shp.ScheduleCode,
    shp.ScheduleId,
    shp.CommissionRate,
    shp.PaidBrokerId,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[stg_hierarchies] nh
INNER JOIN [etl].[stg_hierarchy_versions] nhv ON nhv.HierarchyId = nh.Id
INNER JOIN [etl].[stg_hierarchies] sh ON sh.Id = nh.Id
INNER JOIN [etl].[stg_hierarchy_versions] shv ON shv.HierarchyId = sh.Id
INNER JOIN [etl].[stg_hierarchy_participants] shp ON shp.HierarchyVersionId = shv.Id
WHERE nh.SourceType = 'Migration-Duplicate'
  AND NOT EXISTS (
      SELECT 1 FROM [etl].[stg_hierarchy_participants] hp2 
      WHERE hp2.HierarchyVersionId = nhv.Id
  );

PRINT 'Created hierarchy participants for duplicate hierarchies';
PRINT '';
*/

-- Cleanup
DROP TABLE IF EXISTS #proposals_needing_hierarchies;

PRINT '';
PRINT '============================================================';
PRINT 'FIX SCRIPT COMPLETE';
PRINT '============================================================';
PRINT '';
PRINT 'Next steps:';
PRINT '1. Review the preview above';
PRINT '2. Verify proposals that can be linked';
PRINT '3. Uncomment the INSERT blocks to create duplicate hierarchies';
PRINT '4. Run export to apply to production';
PRINT '';

GO
