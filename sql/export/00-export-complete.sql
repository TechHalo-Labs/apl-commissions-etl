-- ================================================================
-- Complete Export: All Proposal Data with Required Defaults
-- ================================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '================================================================';
PRINT 'COMPLETE EXPORT: Hierarchies, Policies, PHA';
PRINT '================================================================';
PRINT '';

-- ================================================================
-- Export Hierarchies
-- ================================================================

PRINT 'Exporting Hierarchies...';

INSERT INTO [dbo].[Hierarchies] (
    Id,
    Name,
    Description,
    Type,
    Status,
    ProposalId,
    ProposalNumber,
    GroupId,
    GroupName,
    GroupNumber,
    BrokerId,
    BrokerName,
    BrokerLevel,
    ContractId,
    ContractNumber,
    ContractType,
    ContractStatus,
    SourceType,
    HasOverrides,
    DeviationCount,
    SitusState,
    EffectiveDate,
    CurrentVersionId,
    CurrentVersionNumber,
    TemplateId,
    TemplateVersion,
    TemplateSyncStatus,
    CreationTime,
    IsDeleted
)
SELECT 
    h.Id,
    h.Name,
    h.Description,
    COALESCE(h.Type, 1) AS Type,  -- 1 = Standard
    h.[Status],
    h.ProposalId,
    NULL AS ProposalNumber,
    CASE WHEN TRY_CAST(h.GroupId AS BIGINT) IS NOT NULL 
         THEN CONCAT('G', CAST(TRY_CAST(h.GroupId AS BIGINT) AS VARCHAR))
         ELSE h.GroupId 
    END AS GroupId,  -- Add G prefix
    h.GroupName,
    NULL AS GroupNumber,
    h.BrokerId,
    h.BrokerName,
    NULL AS BrokerLevel,
    NULL AS ContractId,
    NULL AS ContractNumber,
    NULL AS ContractType,
    NULL AS ContractStatus,
    'ETL' AS SourceType,
    0 AS HasOverrides,
    0 AS DeviationCount,
    h.SitusState,
    GETUTCDATE() AS EffectiveDate,
    (SELECT TOP 1 Id FROM [etl].[stg_hierarchy_versions] WHERE HierarchyId = h.Id ORDER BY EffectiveFrom DESC) AS CurrentVersionId,
    1 AS CurrentVersionNumber,
    NULL AS TemplateId,
    NULL AS TemplateVersion,
    NULL AS TemplateSyncStatus,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[stg_hierarchies] h;

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
    Status,
    CreationTime,
    IsDeleted
)
SELECT 
    Id,
    HierarchyId,
    TRY_CAST(Version AS INT) AS VersionNumber,  -- Column is 'Version' in staging
    EffectiveFrom,
    EffectiveTo,
    [Status],
    GETUTCDATE(),
    0
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
    ScheduleCode,
    ScheduleId,
    CommissionRate,
    CreationTime,
    IsDeleted
)
SELECT 
    Id,
    HierarchyVersionId,
    EntityId,
    EntityName,
    Level,
    SortOrder,
    ScheduleCode,
    ScheduleId,
    CommissionRate,
    GETUTCDATE(),
    0
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
    PolicyNumber,
    CertificateNumber,
    OldPolicyNumber,
    PolicyType,
    Status,
    StatusDate,
    BrokerId,
    ContractId,
    GroupId,
    CarrierName,
    CarrierId,
    ProductCode,
    ProductName,
    PlanCode,
    PlanName,
    MasterCategory,
    Category,
    InsuredName,
    InsuredFirstName,
    InsuredLastName,
    Premium,
    FaceAmount,
    PayMode,
    Frequency,
    EffectiveDate,
    IssueDate,
    ExpirationDate,
    State,
    Division,
    CompanyCode,
    LionRecordNumber,
    PaidThroughDate,
    ProposalId,
    ProposalAssignedAt,
    ProposalAssignmentSource,
    CreationTime,
    IsDeleted
)
SELECT 
    p.Id,
    COALESCE(p.PolicyNumber, p.Id) AS PolicyNumber,
    p.CertificateNumber,
    p.OldPolicyNumber,
    COALESCE(p.PolicyType, 1) AS PolicyType,  -- 1 = Standard
    COALESCE(TRY_CAST(p.Status AS INT), 1) AS Status,
    p.StatusDate,
    p.BrokerId,
    p.ContractId,
    CASE WHEN TRY_CAST(p.GroupId AS BIGINT) IS NOT NULL 
         THEN CONCAT('G', CAST(TRY_CAST(p.GroupId AS BIGINT) AS VARCHAR))
         ELSE p.GroupId 
    END AS GroupId,  -- Add G prefix
    COALESCE(p.CarrierName, 'Unknown') AS CarrierName,
    p.CarrierId,
    p.ProductCode,
    COALESCE(p.ProductName, p.ProductCode) AS ProductName,
    p.PlanCode,
    p.PlanName,
    p.MasterCategory,
    p.Category,
    COALESCE(p.InsuredName, 'Unknown') AS InsuredName,
    p.InsuredFirstName,
    p.InsuredLastName,
    COALESCE(p.Premium, 0) AS Premium,
    COALESCE(p.FaceAmount, 0) AS FaceAmount,
    p.PayMode,
    p.Frequency,
    COALESCE(p.EffectiveDate, GETUTCDATE()) AS EffectiveDate,
    p.IssueDate,
    p.ExpirationDate,
    p.State,
    p.Division,
    p.CompanyCode,
    p.LionRecordNumber,
    p.PaidThroughDate,
    p.ProposalId,
    p.ProposalAssignedAt,
    p.ProposalAssignmentSource,
    GETUTCDATE(),
    0
FROM [etl].[stg_policies] p;

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Policies';
PRINT '';
GO

-- ================================================================
-- Export PolicyHierarchyAssignments (PHA)
-- ================================================================

PRINT 'Exporting PolicyHierarchyAssignments...';

INSERT INTO [dbo].[PolicyHierarchyAssignments] (
    Id,
    PolicyId,
    WritingBrokerId,
    HierarchyId,
    VersionId,
    ParticipantId,
    SplitSequence,
    SplitPercent,
    NonConformantReason,
    Source,
    CreationTime,
    IsDeleted
)
SELECT 
    CONCAT('PHA-', ROW_NUMBER() OVER (ORDER BY pha.PolicyId, pha.SplitSequence)),
    pha.PolicyId,
    pha.WritingBrokerId,
    NULL AS HierarchyId,  -- Will be NULL for non-conformant policies
    NULL AS VersionId,
    NULL AS ParticipantId,
    pha.SplitSequence,
    pha.SplitPercent,
    pha.NonConformantReason,
    'ETL' AS Source,
    GETUTCDATE(),
    0
FROM [etl].[stg_policy_hierarchy_assignments] pha
WHERE EXISTS (SELECT 1 FROM [dbo].[Policies] WHERE Id = pha.PolicyId);

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
PRINT 'Sample Commission Assignments:';
SELECT TOP 5
    v.ProposalId,
    p.GroupId,
    bs.Name as SourceBroker,
    br.Name as RecipientBroker
FROM [dbo].[CommissionAssignmentVersions] v
INNER JOIN [dbo].[CommissionAssignmentRecipients] r ON r.VersionId = v.Id
LEFT JOIN [dbo].[Proposals] p ON p.Id = v.ProposalId
LEFT JOIN [dbo].[Brokers] bs ON bs.Id = v.BrokerId
LEFT JOIN [dbo].[Brokers] br ON br.Id = r.RecipientBrokerId
ORDER BY v.ProposalId;

PRINT '';
PRINT '================================================================';
PRINT '✅ EXPORT COMPLETE!';
PRINT '================================================================';
GO
