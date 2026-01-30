-- ================================================================
-- Fixed Export: Hierarchies, Versions, Participants, Policies, PHA
-- ================================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '================================================================';
PRINT 'EXPORTING: Hierarchies, Policies, PHA';
PRINT '================================================================';
PRINT '';

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
    h.Id,
    h.Name,
    TRY_CAST(h.GroupId AS BIGINT) AS GroupId,
    h.GroupName,
    h.BrokerId,
    h.BrokerName,
    h.ProposalId,
    h.SitusState,
    (SELECT TOP 1 Id FROM [etl].[stg_hierarchy_versions] WHERE HierarchyId = h.Id ORDER BY EffectiveFrom DESC) AS CurrentVersionId,
    h.[Status]
FROM [etl].[stg_hierarchies] h
WHERE TRY_CAST(h.GroupId AS BIGINT) IS NOT NULL;

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
    Version AS VersionNumber,  -- Column is called 'Version' in staging
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
    EntityType,
    Level,
    SortOrder,
    SplitPercent,
    CommissionRate,
    ScheduleCode,
    ScheduleId
)
SELECT 
    Id,
    HierarchyVersionId,
    EntityId,
    EntityName,
    1 AS EntityType,  -- Broker type (staging doesn't have this column)
    Level,
    SortOrder,
    SplitPercent,
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
    p.Id,  -- Column is 'Id' not 'PolicyId'
    TRY_CAST(p.GroupId AS BIGINT) AS GroupId,
    p.CertificateNumber,
    p.Premium AS PremiumAmount,  -- Column is 'Premium' not 'PremiumAmount'
    p.ProductCode,
    p.PlanCode,
    p.EffectiveDate,  -- Column is already named 'EffectiveDate'
    p.State,  -- Column is 'State' not 'CertIssuedState'
    p.Status,  -- Column is 'Status' not 'CertStatus'
    NULL AS SplitConfigHash,  -- Not in staging, will be NULL
    p.ProposalId  -- Already populated by proposal builder
FROM [etl].[stg_policies] p
WHERE TRY_CAST(p.GroupId AS BIGINT) IS NOT NULL
  AND p.Id IS NOT NULL;

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
    CONCAT('PHA-', ROW_NUMBER() OVER (ORDER BY p.Id, pha.SplitSequence)),
    pha.PolicyId,
    pha.WritingBrokerId,
    pha.SplitSequence,  -- Column exists
    pha.SplitPercent,
    pha.NonConformantReason  -- Column exists
FROM [etl].[stg_policy_hierarchy_assignments] pha
INNER JOIN [dbo].[Policies] p ON p.Id = pha.PolicyId;

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
