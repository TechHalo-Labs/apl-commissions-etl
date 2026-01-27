-- =============================================================================
-- Fix: Link Hierarchies to ALL Matching Proposals (Not Just One)
-- 
-- Problem: Current ETL only links ONE proposal per group to hierarchies.
-- This causes 77.7% of proposals (9,798 out of 12,615) to have NO hierarchies.
--
-- Solution: Link hierarchies to ALL proposals where:
-- 1. Hierarchy EffectiveDate falls within proposal's date range, OR
-- 2. Proposal has no end date and hierarchy EffectiveDate >= proposal EffectiveDateFrom
--
-- Strategy:
-- 1. Create a mapping table of (HierarchyId, ProposalId) pairs
-- 2. Update existing hierarchies to link to all matching proposals
-- 3. Create additional hierarchy records for proposals that don't have hierarchies
-- =============================================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;

PRINT '============================================================';
PRINT 'FIX: Link Hierarchies to ALL Matching Proposals';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Create mapping of hierarchies to ALL matching proposals
-- =============================================================================
PRINT 'Step 1: Creating hierarchy-to-proposal mapping...';

DROP TABLE IF EXISTS #hierarchy_proposal_matches;

SELECT 
    h.Id AS HierarchyId,
    h.GroupId,
    h.EffectiveDate AS HierarchyEffectiveDate,
    p.Id AS ProposalId,
    p.ProposalNumber,
    p.EffectiveDateFrom AS ProposalEffectiveDateFrom,
    p.EffectiveDateTo AS ProposalEffectiveDateTo,
    CASE 
        -- Hierarchy date falls within proposal date range
        WHEN p.EffectiveDateFrom IS NOT NULL 
            AND (p.EffectiveDateTo IS NULL OR p.EffectiveDateTo >= h.EffectiveDate)
            AND p.EffectiveDateFrom <= h.EffectiveDate
        THEN 'DATE_MATCH'
        -- Proposal has no end date and hierarchy date >= proposal start
        WHEN p.EffectiveDateTo IS NULL 
            AND p.EffectiveDateFrom IS NOT NULL
            AND h.EffectiveDate >= p.EffectiveDateFrom
        THEN 'OPEN_ENDED_MATCH'
        ELSE 'NO_MATCH'
    END AS MatchType
INTO #hierarchy_proposal_matches
FROM [dbo].[Hierarchies] h
INNER JOIN [dbo].[Proposals] p ON p.GroupId = h.GroupId
WHERE p.Status = 2  -- Approved proposals only
  AND h.EffectiveDate IS NOT NULL
  AND (
      -- Date range match: hierarchy date within proposal range
      (p.EffectiveDateFrom IS NOT NULL 
       AND (p.EffectiveDateTo IS NULL OR p.EffectiveDateTo >= h.EffectiveDate)
       AND p.EffectiveDateFrom <= h.EffectiveDate)
      OR
      -- Open-ended proposal: hierarchy date >= proposal start
      (p.EffectiveDateTo IS NULL 
       AND p.EffectiveDateFrom IS NOT NULL
       AND h.EffectiveDate >= p.EffectiveDateFrom)
  );

DECLARE @match_count INT = (SELECT COUNT(*) FROM #hierarchy_proposal_matches);
PRINT 'Found ' + CAST(@match_count AS VARCHAR) + ' hierarchy-proposal matches';
PRINT '';

-- =============================================================================
-- Step 2: Identify proposals without hierarchies
-- =============================================================================
PRINT 'Step 2: Identifying proposals without hierarchies...';

DROP TABLE IF EXISTS #proposals_without_hierarchies;

SELECT 
    p.Id AS ProposalId,
    p.ProposalNumber,
    p.GroupId,
    p.EffectiveDateFrom,
    p.EffectiveDateTo,
    -- Find a hierarchy for this group that could be linked
    (SELECT TOP 1 h.Id 
     FROM [dbo].[Hierarchies] h 
     WHERE h.GroupId = p.GroupId
       AND h.EffectiveDate IS NOT NULL
       AND (
           (p.EffectiveDateFrom IS NOT NULL 
            AND (p.EffectiveDateTo IS NULL OR p.EffectiveDateTo >= h.EffectiveDate)
            AND p.EffectiveDateFrom <= h.EffectiveDate)
           OR
           (p.EffectiveDateTo IS NULL 
            AND p.EffectiveDateFrom IS NOT NULL
            AND h.EffectiveDate >= p.EffectiveDateFrom)
       )
     ORDER BY h.EffectiveDate DESC) AS CandidateHierarchyId
INTO #proposals_without_hierarchies
FROM [dbo].[Proposals] p
WHERE p.Status = 2  -- Approved
  AND NOT EXISTS (
      SELECT 1 FROM [dbo].[Hierarchies] h 
      WHERE h.ProposalId = p.Id
  );

DECLARE @proposals_without_hierarchies_count INT = (SELECT COUNT(*) FROM #proposals_without_hierarchies);
PRINT 'Found ' + CAST(@proposals_without_hierarchies_count AS VARCHAR) + ' proposals without hierarchies';
PRINT '';

-- Show sample
IF @proposals_without_hierarchies_count > 0
BEGIN
    PRINT 'Sample proposals without hierarchies:';
    SELECT TOP 10
        ProposalId,
        ProposalNumber,
        GroupId,
        EffectiveDateFrom,
        EffectiveDateTo,
        CandidateHierarchyId,
        CASE WHEN CandidateHierarchyId IS NOT NULL THEN 'CAN_LINK' ELSE 'NO_CANDIDATE' END AS Status
    FROM #proposals_without_hierarchies
    ORDER BY GroupId, EffectiveDateFrom;
    PRINT '';
END

-- =============================================================================
-- Step 3: Preview changes (DRY RUN)
-- =============================================================================
PRINT 'Step 3: Preview of changes (DRY RUN)...';
PRINT '';

-- Show current state
SELECT 
    'Current State' AS Phase,
    COUNT(DISTINCT p.Id) AS TotalProposals,
    COUNT(DISTINCT h.ProposalId) AS ProposalsWithHierarchies,
    COUNT(DISTINCT p.Id) - COUNT(DISTINCT h.ProposalId) AS ProposalsWithoutHierarchies
FROM [dbo].[Proposals] p
LEFT JOIN [dbo].[Hierarchies] h ON h.ProposalId = p.Id
WHERE p.Status = 2;

-- Show what will be linked
SELECT 
    'After Fix' AS Phase,
    COUNT(DISTINCT hpm.ProposalId) AS ProposalsThatWillHaveHierarchies,
    COUNT(DISTINCT hpm.HierarchyId) AS HierarchiesThatWillBeLinked
FROM #hierarchy_proposal_matches hpm;

PRINT '';
PRINT '⚠️  WARNING: The UPDATE statements below are COMMENTED OUT for safety.';
PRINT '    Review the preview above, then uncomment to execute.';
PRINT '';

-- =============================================================================
-- Step 4: Link hierarchies to ALL matching proposals (COMMENTED OUT)
-- =============================================================================
PRINT 'Step 4: Linking hierarchies to proposals...';
PRINT '';

/*
-- Option A: Update existing hierarchies to link to additional proposals
-- This creates duplicate hierarchy records (one per proposal)
-- Note: This approach requires creating new hierarchy records since ProposalId is a single field

-- First, create new hierarchy records for proposals that don't have hierarchies
INSERT INTO [dbo].[Hierarchies] (
    Id, Name, [Description], [Type], [Status], ProposalId, ProposalNumber,
    GroupId, GroupName, GroupNumber, BrokerId, BrokerName, BrokerLevel,
    SourceType, HasOverrides, DeviationCount, SitusState, EffectiveDate,
    CurrentVersionId, CurrentVersionNumber, CreationTime, IsDeleted
)
SELECT 
    CONCAT(h.Id, '-P', REPLACE(pwh.ProposalId, 'P-', '')) AS Id,  -- Unique ID per proposal
    h.Name,
    h.[Description],
    h.[Type],
    h.[Status],
    pwh.ProposalId AS ProposalId,
    pwh.ProposalNumber AS ProposalNumber,
    h.GroupId,
    h.GroupName,
    h.GroupNumber,
    h.BrokerId,
    h.BrokerName,
    h.BrokerLevel,
    'Migration-Fix' AS SourceType,
    h.HasOverrides,
    h.DeviationCount,
    h.SitusState,
    h.EffectiveDate,
    h.CurrentVersionId,
    h.CurrentVersionNumber,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [dbo].[Hierarchies] h
INNER JOIN #proposals_without_hierarchies pwh ON pwh.CandidateHierarchyId = h.Id
WHERE pwh.CandidateHierarchyId IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM [dbo].[Hierarchies] h2 
      WHERE h2.ProposalId = pwh.ProposalId
  );

DECLARE @new_hierarchies_created INT = @@ROWCOUNT;
PRINT 'Created ' + CAST(@new_hierarchies_created AS VARCHAR) + ' new hierarchy records for proposals';
PRINT '';

-- Copy hierarchy versions for new hierarchies
INSERT INTO [dbo].[HierarchyVersions] (
    Id, HierarchyId, [Version], [Status], EffectiveFrom, EffectiveTo,
    ChangeReason, CreationTime, IsDeleted
)
SELECT 
    CONCAT(nh.Id, '-V1') AS Id,
    nh.Id AS HierarchyId,
    hv.[Version],
    hv.[Status],
    hv.EffectiveFrom,
    hv.EffectiveTo,
    hv.ChangeReason,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [dbo].[Hierarchies] nh
INNER JOIN [dbo].[Hierarchies] oh ON oh.Id = nh.Id
INNER JOIN [dbo].[HierarchyVersions] hv ON hv.HierarchyId = oh.Id
WHERE nh.SourceType = 'Migration-Fix'
  AND NOT EXISTS (
      SELECT 1 FROM [dbo].[HierarchyVersions] hv2 
      WHERE hv2.HierarchyId = nh.Id
  );

PRINT 'Created hierarchy versions for new hierarchies';
PRINT '';

-- Copy hierarchy participants for new hierarchies
INSERT INTO [dbo].[HierarchyParticipants] (
    Id, HierarchyVersionId, EntityId, EntityName, [Level], SortOrder,
    ScheduleCode, ScheduleId, CommissionRate, CreationTime, IsDeleted
)
SELECT 
    CONCAT(nhv.Id, '-P', CAST(hp.EntityId AS VARCHAR), '-L', CAST(hp.[Level] AS VARCHAR)) AS Id,
    nhv.Id AS HierarchyVersionId,
    hp.EntityId,
    hp.EntityName,
    hp.[Level],
    hp.SortOrder,
    hp.ScheduleCode,
    hp.ScheduleId,
    hp.CommissionRate,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [dbo].[Hierarchies] nh
INNER JOIN [dbo].[HierarchyVersions] nhv ON nhv.HierarchyId = nh.Id
INNER JOIN [dbo].[Hierarchies] oh ON oh.Id = nh.Id
INNER JOIN [dbo].[HierarchyVersions] ohv ON ohv.HierarchyId = oh.Id
INNER JOIN [dbo].[HierarchyParticipants] hp ON hp.HierarchyVersionId = ohv.Id
WHERE nh.SourceType = 'Migration-Fix'
  AND NOT EXISTS (
      SELECT 1 FROM [dbo].[HierarchyParticipants] hp2 
      WHERE hp2.HierarchyVersionId = nhv.Id
  );

PRINT 'Created hierarchy participants for new hierarchies';
PRINT '';
*/

-- =============================================================================
-- Step 5: Verification Query
-- =============================================================================
PRINT 'Step 5: Verification (after fix)...';
PRINT '';

SELECT 
    p.Id AS ProposalId,
    p.ProposalNumber,
    p.GroupId,
    COUNT(h.Id) AS HierarchyCount,
    CASE 
        WHEN COUNT(h.Id) = 0 THEN '❌ NO HIERARCHY'
        WHEN COUNT(h.Id) = 1 THEN '✅ HAS HIERARCHY'
        ELSE '⚠️ MULTIPLE HIERARCHIES'
    END AS Status
FROM [dbo].[Proposals] p
LEFT JOIN [dbo].[Hierarchies] h ON h.ProposalId = p.Id
WHERE p.Status = 2
GROUP BY p.Id, p.ProposalNumber, p.GroupId, p.EffectiveDateFrom
HAVING COUNT(h.Id) = 0
ORDER BY p.GroupId, p.EffectiveDateFrom;

-- Cleanup
DROP TABLE IF EXISTS #hierarchy_proposal_matches;
DROP TABLE IF EXISTS #proposals_without_hierarchies;

PRINT '';
PRINT '============================================================';
PRINT 'FIX SCRIPT COMPLETE';
PRINT '============================================================';
PRINT '';
PRINT 'Next steps:';
PRINT '1. Review the preview above';
PRINT '2. Verify the hierarchy-proposal matches';
PRINT '3. Uncomment the INSERT blocks to create new hierarchy records';
PRINT '4. Run verification query to confirm fixes';
PRINT '';

GO
