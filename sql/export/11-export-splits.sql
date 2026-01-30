-- =====================================================
-- Export PremiumSplitVersions and PremiumSplitParticipants
-- Only exports records that don't already exist
-- Handles schema differences between staging and production:
-- - GroupId: staging is nvarchar, production is bigint
--   * Numeric GroupIds: direct cast
--   * Alphanumeric GroupIds: normalized with state prefix encoding
--     (LA0146 -> 50000146, MS0059 -> 60000059, AL9999 -> 40009999)
-- - TotalSplitPercent: production is decimal(5,2), max 999.99
-- - PremiumSplitParticipants: production doesn't have Sequence, 
--   WritingBrokerId, CreationTime, IsDeleted columns
-- =====================================================

PRINT 'Exporting missing PremiumSplitVersions...';

-- Helper function to extract numeric portion from GroupId
-- E.g., 'GT17624' -> 17624, 'G0006' -> 6, 'GAL0017' -> 17
INSERT INTO [$(PRODUCTION_SCHEMA)].[PremiumSplitVersions] (
    Id, GroupId, GroupName, ProposalId, VersionNumber,
    EffectiveFrom, EffectiveTo, ChangeDescription, TotalSplitPercent,
    [Status], [Source], HubspotDealId, CreationTime, IsDeleted
)
SELECT 
    spsv.Id,
    -- Normalize GroupId: use function for alphanumeric, direct cast for numeric
    CASE 
        WHEN TRY_CAST(spsv.GroupId AS BIGINT) IS NOT NULL 
            THEN CAST(spsv.GroupId AS BIGINT)
        ELSE dbo.NormalizeAlphanumericGroupId(spsv.GroupId)
    END AS GroupId,
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
FROM [$(ETL_SCHEMA)].[stg_premium_split_versions] spsv
WHERE spsv.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[PremiumSplitVersions])
  -- Exclude groups flagged in stg_excluded_groups
  AND spsv.GroupId NOT IN (SELECT GroupId FROM [$(ETL_SCHEMA)].[stg_excluded_groups])
  -- REFERENTIAL INTEGRITY: Only export splits for exported proposals
  AND spsv.ProposalId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Proposals])
  -- Accept both numeric and alphanumeric GroupIds (now normalized)
  AND (
    TRY_CAST(spsv.GroupId AS BIGINT) IS NOT NULL 
    OR dbo.NormalizeAlphanumericGroupId(spsv.GroupId) IS NOT NULL
  )
  -- EXCLUDE broken split versions: versions that have participants without HierarchyId
  AND NOT EXISTS (
    SELECT 1 
    FROM [$(ETL_SCHEMA)].[stg_premium_split_participants] spsp
    WHERE spsp.VersionId = spsv.Id
      AND spsp.HierarchyId IS NULL
  );

DECLARE @psvCount INT;
SELECT @psvCount = @@ROWCOUNT;
PRINT 'PremiumSplitVersions exported: ' + CAST(@psvCount AS VARCHAR);
GO

PRINT 'Exporting missing PremiumSplitParticipants...';

-- Note: Production PremiumSplitParticipants schema does NOT have:
-- Sequence, WritingBrokerId, CreationTime, IsDeleted
-- These columns exist in staging but not in production

INSERT INTO [$(PRODUCTION_SCHEMA)].[PremiumSplitParticipants] (
    Id, VersionId, BrokerId, BrokerName, BrokerNPN, SplitPercent,
    IsWritingAgent, HierarchyId, HierarchyName, TemplateId, TemplateName,
    EffectiveFrom, EffectiveTo, Notes
)
SELECT 
    spsp.Id,
    spsp.VersionId,
    spsp.BrokerId,  -- Use BrokerId from staging (maps to production BrokerId)
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
FROM [$(ETL_SCHEMA)].[stg_premium_split_participants] spsp
INNER JOIN [$(ETL_SCHEMA)].[stg_premium_split_versions] spsv ON spsv.Id = spsp.VersionId
WHERE spsp.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[PremiumSplitParticipants])
  -- REFERENTIAL INTEGRITY: Only export participants for exported split versions
  AND spsp.VersionId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[PremiumSplitVersions])
  -- REFERENTIAL INTEGRITY: Only export if hierarchy exists in production
  AND (spsp.HierarchyId IS NULL OR spsp.HierarchyId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Hierarchies]))
  AND spsp.BrokerUniquePartyId IS NOT NULL  -- Only export if broker reference is valid
  AND spsp.HierarchyId IS NOT NULL;  -- EXCLUDE broken participants: only export participants with HierarchyId

DECLARE @pspCount INT;
SELECT @pspCount = @@ROWCOUNT;
PRINT 'PremiumSplitParticipants exported: ' + CAST(@pspCount AS VARCHAR);
GO

PRINT '=== Split Export Complete ===';
