-- ================================================================
-- Simple Direct Export: Staging → Production
-- ================================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '================================================================';
PRINT 'SIMPLE EXPORT: Staging → Production';
PRINT '================================================================';
PRINT '';

-- ================================================================
-- Export PremiumSplitVersions
-- ================================================================

PRINT 'Exporting PremiumSplitVersions...';

INSERT INTO [dbo].[PremiumSplitVersions] (
    Id,
    GroupId,
    GroupName,
    ProposalId,
    VersionNumber,
    EffectiveFrom,
    EffectiveTo,
    ChangeDescription,
    TotalSplitPercent,
    [Status],
    [Source],
    HubspotDealId
)
SELECT 
    Id,
    TRY_CAST(GroupId AS BIGINT) AS GroupId,  -- Production is BIGINT
    GroupName,
    ProposalId,
    VersionNumber,
    EffectiveFrom,
    EffectiveTo,
    ChangeDescription,
    TotalSplitPercent,
    [Status],
    [Source],
    HubspotDealId
FROM [etl].[stg_premium_split_versions]
WHERE TRY_CAST(GroupId AS BIGINT) IS NOT NULL;  -- Only numeric GroupIds

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' PremiumSplitVersions';
PRINT '';
GO

-- ================================================================
-- Export PremiumSplitParticipants
-- ================================================================

PRINT 'Exporting PremiumSplitParticipants...';

INSERT INTO [dbo].[PremiumSplitParticipants] (
    Id,
    VersionId,
    BrokerId,
    BrokerName,
    BrokerNPN,
    SplitPercent,
    IsWritingAgent,
    HierarchyId,
    HierarchyName,
    TemplateId,
    TemplateName,
    EffectiveFrom,
    EffectiveTo,
    Notes
)
SELECT 
    Id,
    VersionId,
    BrokerId,
    BrokerName,
    BrokerNPN,
    SplitPercent,
    IsWritingAgent,
    HierarchyId,
    HierarchyName,
    TemplateId,
    TemplateName,
    EffectiveFrom,
    EffectiveTo,
    Notes
FROM [etl].[stg_premium_split_participants]
WHERE HierarchyId IS NOT NULL;  -- Only valid hierarchies

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' PremiumSplitParticipants';
PRINT '';
GO

-- ================================================================
-- Export Hierarchies
-- ================================================================

PRINT 'Exporting Hierarchies...';

INSERT INTO [dbo].[Hierarchies] (
    Id,
    Name,
    GroupId,
    GroupName,
    BrokerId,
    BrokerName,
    ProposalId,
    SitusState,
    CurrentVersionId,
    [Status]
)
SELECT 
    Id,
    Name,
    TRY_CAST(GroupId AS BIGINT) AS GroupId,  -- Production is BIGINT
    GroupName,
    BrokerId,
    BrokerName,
    ProposalId,
    SitusState,
    VersionNumber AS CurrentVersionId,
    [Status]
FROM [etl].[stg_hierarchies]
WHERE TRY_CAST(GroupId AS BIGINT) IS NOT NULL;  -- Only numeric GroupIds

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Hierarchies';
PRINT '';
GO

-- ================================================================
-- Export Hierarchy Versions
-- ================================================================

PRINT 'Exporting HierarchyVersions...';

INSERT INTO [dbo].[HierarchyVersions] (
    Id,
    HierarchyId,
    VersionNumber,
    EffectiveFrom,
    EffectiveTo,
    [Status]
)
SELECT 
    Id,
    HierarchyId,
    VersionNumber,
    EffectiveFrom,
    EffectiveTo,
    [Status]
FROM [etl].[stg_hierarchy_versions]
WHERE EXISTS (SELECT 1 FROM [dbo].[Hierarchies] WHERE Id = stg_hierarchy_versions.HierarchyId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' HierarchyVersions';
PRINT '';
GO

-- ================================================================
-- Export Hierarchy Participants
-- ================================================================

PRINT 'Exporting HierarchyParticipants...';

INSERT INTO [dbo].[HierarchyParticipants] (
    Id,
    HierarchyVersionId,
    EntityId,
    EntityName,
    Level,
    SortOrder,
    CommissionRate,
    ScheduleCode,
    ScheduleId
)
SELECT 
    Id,
    HierarchyVersionId,
    EntityId,
    EntityName,
    Level,
    SortOrder,
    CommissionRate,
    ScheduleCode,
    ScheduleId
FROM [etl].[stg_hierarchy_participants]
WHERE EXISTS (SELECT 1 FROM [dbo].[HierarchyVersions] WHERE Id = stg_hierarchy_participants.HierarchyVersionId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' HierarchyParticipants';
PRINT '';
GO

-- ================================================================
-- Export Policies
-- ================================================================

PRINT 'Exporting Policies...';

INSERT INTO [dbo].[Policies] (
    Id,
    GroupId,
    CertificateNumber,
    PremiumAmount,
    ProductCode,
    PlanCode,
    EffectiveDate,
    State,
    Status,
    SplitConfigHash,
    ProposalId
)
SELECT 
    PolicyId AS Id,
    TRY_CAST(GroupId AS BIGINT) AS GroupId,
    CertificateNumber,
    PremiumAmount,
    ProductCode,
    PlanCode,
    CertEffectiveDate AS EffectiveDate,
    CertIssuedState AS State,
    CertStatus AS Status,
    SplitConfigHash,
    (SELECT TOP 1 ProposalId 
     FROM [etl].[stg_proposal_key_mapping] pkm
     WHERE pkm.GroupId = p.GroupId
       AND pkm.EffectiveYear = YEAR(p.CertEffectiveDate)
       AND pkm.ProductCode = p.ProductCode
       AND pkm.PlanCode = p.PlanCode) AS ProposalId
FROM [etl].[stg_policies] p
WHERE TRY_CAST(GroupId AS BIGINT) IS NOT NULL;

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Policies';
PRINT '';
GO

-- ================================================================
-- Export PolicyHierarchyAssignments
-- ================================================================

PRINT 'Exporting PolicyHierarchyAssignments...';

INSERT INTO [dbo].[PolicyHierarchyAssignments] (
    Id,
    PolicyId,
    WritingBrokerId,
    SplitSequence,
    SplitPercent,
    NonConformantReason
)
SELECT 
    CONCAT('PHA-', ROW_NUMBER() OVER (ORDER BY PolicyId, SplitSequence)),
    PolicyId,
    WritingBrokerId,
    SplitSequence,
    SplitPercent,
    NonConformantReason
FROM [etl].[stg_policy_hierarchy_assignments]
WHERE EXISTS (SELECT 1 FROM [dbo].[Policies] WHERE Id = stg_policy_hierarchy_assignments.PolicyId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' PolicyHierarchyAssignments';
PRINT '';
GO

-- ================================================================
-- Verification
-- ================================================================

PRINT '================================================================';
PRINT 'VERIFICATION';
PRINT '================================================================';

SELECT 
    'Proposals' as [Table],
    COUNT(*) as [Count]
FROM [dbo].[Proposals]
UNION ALL
SELECT 'PremiumSplitVersions', COUNT(*) FROM [dbo].[PremiumSplitVersions]
UNION ALL
SELECT 'PremiumSplitParticipants', COUNT(*) FROM [dbo].[PremiumSplitParticipants]
UNION ALL
SELECT 'Hierarchies', COUNT(*) FROM [dbo].[Hierarchies]
UNION ALL
SELECT 'HierarchyVersions', COUNT(*) FROM [dbo].[HierarchyVersions]
UNION ALL
SELECT 'HierarchyParticipants', COUNT(*) FROM [dbo].[HierarchyParticipants]
UNION ALL
SELECT 'Policies', COUNT(*) FROM [dbo].[Policies]
UNION ALL
SELECT 'PolicyHierarchyAssignments', COUNT(*) FROM [dbo].[PolicyHierarchyAssignments]
UNION ALL
SELECT 'CommissionAssignmentVersions', COUNT(*) FROM [dbo].[CommissionAssignmentVersions]
UNION ALL
SELECT 'CommissionAssignmentRecipients', COUNT(*) FROM [dbo].[CommissionAssignmentRecipients]
ORDER BY 1;

PRINT '';
PRINT '================================================================';
PRINT '✅ EXPORT COMPLETE!';
PRINT '================================================================';
GO
