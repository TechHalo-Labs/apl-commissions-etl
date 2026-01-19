-- =====================================================
-- Export PremiumSplitVersions and PremiumSplitParticipants
-- Only exports records that don't already exist
-- Handles schema differences between staging and production:
-- - GroupId: staging is nvarchar with 'G' prefix, production is bigint
-- - TotalSplitPercent: production is decimal(5,2), max 999.99
-- - PremiumSplitParticipants: production doesn't have Sequence, 
--   WritingBrokerId, CreationTime, IsDeleted columns
-- =====================================================

PRINT 'Exporting missing PremiumSplitVersions...';

-- Helper function to extract numeric portion from GroupId
-- E.g., 'GT17624' -> 17624, 'G0006' -> 6, 'GAL0017' -> 17
INSERT INTO [dbo].[PremiumSplitVersions] (
    Id, GroupId, GroupName, ProposalId, VersionNumber,
    EffectiveFrom, EffectiveTo, ChangeDescription, TotalSplitPercent,
    [Status], [Source], HubspotDealId, CreationTime, IsDeleted
)
SELECT 
    spsv.Id,
    -- Extract only numeric characters from GroupId
    TRY_CAST(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            spsv.GroupId,
            'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),
            'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),
            'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),'U',''),
            'V',''),'W',''),'X',''),'Y',''),'Z','')
        AS BIGINT
    ) AS GroupId,
    spsv.GroupName,
    spsv.ProposalId,
    spsv.VersionNumber,
    spsv.EffectiveFrom,
    spsv.EffectiveTo,
    spsv.ChangeDescription,
    -- Cap TotalSplitPercent at 999.99 (production is decimal(5,2))
    CASE 
        WHEN spsv.TotalSplitPercent > 999.99 THEN 999.99 
        ELSE CAST(spsv.TotalSplitPercent AS DECIMAL(5,2)) 
    END AS TotalSplitPercent,
    COALESCE(spsv.[Status], 1) AS [Status],  -- 1 = Active
    COALESCE(spsv.[Source], 0) AS [Source],
    spsv.HubspotDealId,
    spsv.CreationTime,
    COALESCE(spsv.IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_premium_split_versions] spsv
WHERE spsv.Id NOT IN (SELECT Id FROM [dbo].[PremiumSplitVersions])
  -- Extract only numeric characters and ensure it's a valid number
  AND TRY_CAST(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            spsv.GroupId,
            'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),
            'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),
            'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),'U',''),
            'V',''),'W',''),'X',''),'Y',''),'Z','')
        AS BIGINT
      ) IS NOT NULL;

DECLARE @psvCount INT;
SELECT @psvCount = @@ROWCOUNT;
PRINT 'PremiumSplitVersions exported: ' + CAST(@psvCount AS VARCHAR);
GO

PRINT 'Exporting missing PremiumSplitParticipants...';

-- Note: Production PremiumSplitParticipants schema does NOT have:
-- Sequence, WritingBrokerId, CreationTime, IsDeleted
-- These columns exist in staging but not in production

INSERT INTO [dbo].[PremiumSplitParticipants] (
    Id, VersionId, BrokerId, BrokerName, BrokerNPN, SplitPercent,
    IsWritingAgent, HierarchyId, HierarchyName, TemplateId, TemplateName,
    EffectiveFrom, EffectiveTo, Notes
)
SELECT 
    spsp.Id,
    spsp.VersionId,
    spsp.BrokerId,
    spsp.BrokerName,
    spsp.BrokerNPN,
    spsp.SplitPercent,
    COALESCE(spsp.IsWritingAgent, 0) AS IsWritingAgent,
    spsp.HierarchyId,
    spsp.HierarchyName,
    spsp.TemplateId,
    spsp.TemplateName,
    spsp.EffectiveFrom,
    spsp.EffectiveTo,
    spsp.Notes
FROM [etl].[stg_premium_split_participants] spsp
WHERE spsp.Id NOT IN (SELECT Id FROM [dbo].[PremiumSplitParticipants])
  AND spsp.VersionId IN (SELECT Id FROM [dbo].[PremiumSplitVersions]);

DECLARE @pspCount INT;
SELECT @pspCount = @@ROWCOUNT;
PRINT 'PremiumSplitParticipants exported: ' + CAST(@pspCount AS VARCHAR);
GO

PRINT '=== Split Export Complete ===';
