-- =====================================================
-- Export Hierarchies, HierarchyVersions, HierarchyParticipants
-- Only exports records that don't already exist
-- Fixed column mappings for production schema:
-- - HierarchyVersions: uses Version/EffectiveFrom/EffectiveTo (not VersionNumber/EffectiveDate/EndDate/ApprovedBy/ApprovedAt)
-- =====================================================

PRINT 'Exporting missing Hierarchies...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[Hierarchies] (
    Id, Name, [Description], [Type], [Status], ProposalId, ProposalNumber,
    GroupId, GroupName, GroupNumber, BrokerId, BrokerName, BrokerLevel,
    SourceType, HasOverrides, DeviationCount, SitusState, EffectiveDate,
    CurrentVersionId, CurrentVersionNumber, CreationTime, IsDeleted
)
SELECT 
    sh.Id,
    sh.Name,
    sh.[Description],
    COALESCE(TRY_CAST(sh.[Type] AS INT), 0) AS [Type],
    COALESCE(TRY_CAST(sh.[Status] AS INT), 1) AS [Status],  -- 1 = Active
    sh.ProposalId,
    sh.ProposalNumber,
    sh.GroupId,
    sh.GroupName,
    sh.GroupNumber,
    sh.BrokerId,
    sh.BrokerName,
    sh.BrokerLevel,
    sh.SourceType,
    0 AS HasOverrides,
    0 AS DeviationCount,
    sh.SitusState,
    sh.EffectiveDate,
    sh.CurrentVersionId,
    COALESCE(sh.CurrentVersionNumber, 1) AS CurrentVersionNumber,
    COALESCE(sh.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(sh.IsDeleted, 0) AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_hierarchies] sh
WHERE sh.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Hierarchies]);

DECLARE @hCount INT;
SELECT @hCount = @@ROWCOUNT;
PRINT 'Hierarchies exported: ' + CAST(@hCount AS VARCHAR);
GO

PRINT 'Exporting missing HierarchyVersions...';

-- Production HierarchyVersions schema:
-- Id, HierarchyId, Version (int), Status (int), EffectiveFrom, EffectiveTo, ChangeReason

INSERT INTO [$(PRODUCTION_SCHEMA)].[HierarchyVersions] (
    Id, HierarchyId, [Version], [Status], EffectiveFrom, EffectiveTo,
    ChangeReason, CreationTime, IsDeleted
)
SELECT 
    shv.Id,
    shv.HierarchyId,
    COALESCE(TRY_CAST(shv.[Version] AS INT), 1) AS [Version],
    COALESCE(TRY_CAST(shv.[Status] AS INT), 1) AS [Status],  -- 1 = Active
    COALESCE(shv.EffectiveFrom, GETUTCDATE()) AS EffectiveFrom,
    shv.EffectiveTo,
    shv.ChangeReason,
    COALESCE(shv.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(shv.IsDeleted, 0) AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_hierarchy_versions] shv
WHERE shv.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[HierarchyVersions]);

DECLARE @hvCount INT;
SELECT @hvCount = @@ROWCOUNT;
PRINT 'HierarchyVersions exported: ' + CAST(@hvCount AS VARCHAR);
GO

PRINT 'Exporting missing HierarchyParticipants...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[HierarchyParticipants] (
    Id, HierarchyVersionId, EntityId, EntityName, [Level], SortOrder,
    ScheduleCode, ScheduleId, CommissionRate, CreationTime, IsDeleted
)
SELECT 
    shp.Id,
    shp.HierarchyVersionId,
    shp.EntityId,
    shp.EntityName,
    shp.[Level],
    shp.SortOrder,
    shp.ScheduleCode,
    shp.ScheduleId,
    shp.CommissionRate,
    COALESCE(shp.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(shp.IsDeleted, 0) AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_hierarchy_participants] shp
WHERE shp.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[HierarchyParticipants]);

DECLARE @hpCount INT;
SELECT @hpCount = @@ROWCOUNT;
PRINT 'HierarchyParticipants exported: ' + CAST(@hpCount AS VARCHAR);
GO

PRINT 'Exporting missing StateRules...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[StateRules] (
    Id, HierarchyVersionId, ShortName, Name, [Description], [Type], SortOrder,
    CreationTime, IsDeleted
)
SELECT 
    sr.Id,
    sr.HierarchyVersionId,
    sr.ShortName,
    sr.Name,
    sr.[Description],
    COALESCE(sr.[Type], 0) AS [Type],
    COALESCE(sr.SortOrder, 0) AS SortOrder,
    COALESCE(sr.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(sr.IsDeleted, 0) AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_state_rules] sr
WHERE sr.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[StateRules])
  AND sr.HierarchyVersionId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[HierarchyVersions]);

DECLARE @srCount INT;
SELECT @srCount = @@ROWCOUNT;
PRINT 'StateRules exported: ' + CAST(@srCount AS VARCHAR);

-- Update existing state rules that were converted to catch-all
PRINT 'Updating existing StateRules converted to catch-all...';

UPDATE dbo_sr
SET 
    dbo_sr.ShortName = stg_sr.ShortName,
    dbo_sr.Name = stg_sr.Name,
    dbo_sr.[Description] = stg_sr.[Description],
    dbo_sr.[Type] = stg_sr.[Type],
    dbo_sr.LastModificationTime = GETUTCDATE()
FROM [$(PRODUCTION_SCHEMA)].[StateRules] dbo_sr
INNER JOIN [$(ETL_SCHEMA)].[stg_state_rules] stg_sr ON dbo_sr.Id = stg_sr.Id
WHERE stg_sr.[Type] = 1  -- Catch-all
  AND stg_sr.ShortName = 'ALL'
  AND (
      dbo_sr.ShortName <> stg_sr.ShortName
      OR dbo_sr.Name <> stg_sr.Name
      OR dbo_sr.[Type] <> stg_sr.[Type]
  );

DECLARE @srUpdatedCount INT;
SELECT @srUpdatedCount = @@ROWCOUNT;
PRINT 'StateRules updated to catch-all: ' + CAST(@srUpdatedCount AS VARCHAR);
GO

PRINT 'Exporting missing StateRuleStates...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[StateRuleStates] (
    Id, StateRuleId, StateCode, StateName, CreationTime, IsDeleted
)
SELECT 
    srs.Id,
    srs.StateRuleId,
    srs.StateCode,
    srs.StateName,
    COALESCE(srs.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(srs.IsDeleted, 0) AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_state_rule_states] srs
WHERE srs.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[StateRuleStates])
  AND srs.StateRuleId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[StateRules]);

DECLARE @srsCount INT;
SELECT @srsCount = @@ROWCOUNT;
PRINT 'StateRuleStates exported: ' + CAST(@srsCount AS VARCHAR);

-- Delete state rule states for catch-all rules (catch-all has no states)
PRINT 'Deleting StateRuleStates for catch-all rules...';

DELETE dbo_srs
FROM [$(PRODUCTION_SCHEMA)].[StateRuleStates] dbo_srs
INNER JOIN [$(PRODUCTION_SCHEMA)].[StateRules] dbo_sr ON dbo_srs.StateRuleId = dbo_sr.Id
WHERE dbo_sr.[Type] = 1  -- Catch-all
  AND dbo_sr.ShortName = 'ALL'
  AND dbo_srs.IsDeleted = 0;

DECLARE @srsDeletedCount INT;
SELECT @srsDeletedCount = @@ROWCOUNT;
PRINT 'StateRuleStates deleted for catch-all rules: ' + CAST(@srsDeletedCount AS VARCHAR);
GO

PRINT 'Exporting missing HierarchySplits...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[HierarchySplits] (
    Id, StateRuleId, ProductId, ProductCode, ProductName, SortOrder,
    CreationTime, IsDeleted
)
SELECT 
    hs.Id,
    hs.StateRuleId,
    hs.ProductId,
    hs.ProductCode,
    hs.ProductName,
    COALESCE(hs.SortOrder, 0) AS SortOrder,
    COALESCE(hs.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(hs.IsDeleted, 0) AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_hierarchy_splits] hs
WHERE hs.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[HierarchySplits])
  AND hs.StateRuleId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[StateRules]);

DECLARE @hsCount INT;
SELECT @hsCount = @@ROWCOUNT;
PRINT 'HierarchySplits exported: ' + CAST(@hsCount AS VARCHAR);
GO

PRINT 'Exporting missing SplitDistributions...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[SplitDistributions] (
    Id, HierarchySplitId, HierarchyParticipantId, ParticipantEntityId,
    Percentage, ScheduleId, ScheduleName, CreationTime, IsDeleted
)
SELECT 
    sd.Id,
    sd.HierarchySplitId,
    sd.HierarchyParticipantId,
    sd.ParticipantEntityId,
    COALESCE(sd.Percentage, 100) AS Percentage,
    sd.ScheduleId,
    sd.ScheduleName,
    COALESCE(sd.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(sd.IsDeleted, 0) AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_split_distributions] sd
WHERE sd.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[SplitDistributions])
  AND sd.HierarchySplitId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[HierarchySplits])
  AND sd.HierarchyParticipantId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[HierarchyParticipants]);

DECLARE @sdCount INT;
SELECT @sdCount = @@ROWCOUNT;
PRINT 'SplitDistributions exported: ' + CAST(@sdCount AS VARCHAR);
GO

PRINT '=== Hierarchy Export Complete ===';
