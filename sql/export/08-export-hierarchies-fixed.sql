-- =====================================================
-- Export Hierarchies (All Components) - FIXED VERSION
-- =====================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT 'Exporting Hierarchies...';

INSERT INTO [dbo].[Hierarchies] (
    Id, Name, Description, Type, Status, ProposalId, ProposalNumber,
    GroupId, GroupName, GroupNumber, BrokerId, BrokerName, BrokerLevel,
    ContractId, ContractNumber, ContractType, ContractStatus,
    SourceType, HasOverrides, DeviationCount, SitusState,
    EffectiveDate, CurrentVersionId, CurrentVersionNumber,
    TemplateId, TemplateVersion, TemplateSyncStatus,
    CreationTime, IsDeleted
)
SELECT 
    h.Id, h.Name, h.Description, h.Type, h.Status, h.ProposalId, h.ProposalNumber,
    h.GroupId, h.GroupName, h.GroupNumber, h.BrokerId, h.BrokerName, h.BrokerLevel,
    h.ContractId, h.ContractNumber, h.ContractType, h.ContractStatus,
    h.SourceType, h.HasOverrides, h.DeviationCount, h.SitusState,
    h.EffectiveDate, h.CurrentVersionId, h.CurrentVersionNumber,
    h.TemplateId, h.TemplateVersion, h.TemplateSyncStatus,
    h.CreationTime, h.IsDeleted
FROM [etl].[stg_hierarchies] h;

PRINT '  ✓ Hierarchies exported: ' + CAST(@@ROWCOUNT AS VARCHAR);
PRINT '';

PRINT 'Exporting HierarchyVersions...';

INSERT INTO [dbo].[HierarchyVersions] (
    Id, HierarchyId, Version, Status, EffectiveFrom, EffectiveTo,
    ChangeReason, CreationTime, IsDeleted
)
SELECT 
    Id, HierarchyId, Version, Status, EffectiveFrom, EffectiveTo,
    ChangeReason, CreationTime, IsDeleted
FROM [etl].[stg_hierarchy_versions]
WHERE EXISTS (SELECT 1 FROM [dbo].[Hierarchies] WHERE Id = stg_hierarchy_versions.HierarchyId);

PRINT '  ✓ HierarchyVersions exported: ' + CAST(@@ROWCOUNT AS VARCHAR);
PRINT '';

PRINT 'Exporting HierarchyParticipants...';

INSERT INTO [dbo].[HierarchyParticipants] (
    Id, HierarchyVersionId, EntityId, EntityName, Level, SortOrder,
    ScheduleCode, ScheduleId, CommissionRate, CreationTime, IsDeleted
)
SELECT 
    Id, HierarchyVersionId, EntityId, EntityName, Level, SortOrder,
    ScheduleCode, ScheduleId, CommissionRate, CreationTime, IsDeleted
FROM [etl].[stg_hierarchy_participants]
WHERE EXISTS (SELECT 1 FROM [dbo].[HierarchyVersions] WHERE Id = stg_hierarchy_participants.HierarchyVersionId);

PRINT '  ✓ HierarchyParticipants exported: ' + CAST(@@ROWCOUNT AS VARCHAR);
PRINT '';

PRINT 'Exporting StateRules...';

INSERT INTO [dbo].[StateRules] (
    Id, HierarchyVersionId, ShortName, Name, Description,
    Type, SortOrder, CreationTime, IsDeleted
)
SELECT 
    Id, HierarchyVersionId, ShortName, Name, Description,
    Type, SortOrder, CreationTime, IsDeleted
FROM [etl].[stg_state_rules]
WHERE EXISTS (SELECT 1 FROM [dbo].[HierarchyVersions] WHERE Id = stg_state_rules.HierarchyVersionId);

PRINT '  ✓ StateRules exported: ' + CAST(@@ROWCOUNT AS VARCHAR);
PRINT '';

PRINT 'Exporting StateRuleStates...';

INSERT INTO [dbo].[StateRuleStates] (
    Id, StateRuleId, StateCode, StateName, CreationTime, IsDeleted
)
SELECT 
    Id, StateRuleId, StateCode, StateName, CreationTime, IsDeleted
FROM [etl].[stg_state_rule_states]
WHERE EXISTS (SELECT 1 FROM [dbo].[StateRules] WHERE Id = stg_state_rule_states.StateRuleId);

PRINT '  ✓ StateRuleStates exported: ' + CAST(@@ROWCOUNT AS VARCHAR);
PRINT '';

PRINT 'Exporting HierarchySplits with ProductId lookup...';

INSERT INTO [dbo].[HierarchySplits] (
    Id, StateRuleId, ProductId, ProductCode, ProductName,
    SortOrder, CreationTime, IsDeleted
)
SELECT 
    hs.Id, 
    hs.StateRuleId, 
    COALESCE(p.Id, hs.ProductCode) AS ProductId,  -- Lookup ProductId from Products table
    hs.ProductCode, 
    hs.ProductName,
    hs.SortOrder, 
    hs.CreationTime, 
    hs.IsDeleted
FROM [etl].[stg_hierarchy_splits] hs
LEFT JOIN [dbo].[Products] p ON p.ProductCode = hs.ProductCode
WHERE EXISTS (SELECT 1 FROM [dbo].[StateRules] WHERE Id = hs.StateRuleId);

PRINT '  ✓ HierarchySplits exported: ' + CAST(@@ROWCOUNT AS VARCHAR);
PRINT '';

PRINT 'Exporting SplitDistributions...';

INSERT INTO [dbo].[SplitDistributions] (
    Id, HierarchySplitId, HierarchyParticipantId, ParticipantEntityId,
    Percentage, ScheduleId, ScheduleName, CreationTime, IsDeleted
)
SELECT 
    Id, HierarchySplitId, HierarchyParticipantId, ParticipantEntityId,
    Percentage, ScheduleId, ScheduleName, CreationTime, IsDeleted
FROM [etl].[stg_split_distributions]
WHERE EXISTS (SELECT 1 FROM [dbo].[HierarchySplits] WHERE Id = stg_split_distributions.HierarchySplitId)
  AND EXISTS (SELECT 1 FROM [dbo].[HierarchyParticipants] WHERE Id = stg_split_distributions.HierarchyParticipantId);

PRINT '  ✓ SplitDistributions exported: ' + CAST(@@ROWCOUNT AS VARCHAR);
PRINT '';

PRINT '=== Hierarchy Export Complete ===';

GO
