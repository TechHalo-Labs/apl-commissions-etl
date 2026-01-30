-- ================================================================
-- Final Export: Match Production Schemas Exactly
-- ================================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '================================================================';
PRINT 'FINAL EXPORT: Hierarchies, Versions, Participants, Policies, PHA';
PRINT '================================================================';
PRINT '';

-- ================================================================
-- Export Hierarchy Versions
-- ================================================================

PRINT 'Exporting HierarchyVersions...';

INSERT INTO [dbo].[HierarchyVersions] (
    Id,
    HierarchyId,
    Version,  -- Column is 'Version' not 'VersionNumber'
    Status,
    EffectiveFrom,
    EffectiveTo,
    ChangeReason,
    CreationTime,
    IsDeleted
)
SELECT 
    Id,
    HierarchyId,
    TRY_CAST(Version AS INT) AS Version,
    [Status],
    EffectiveFrom,
    EffectiveTo,
    ChangeReason,
    GETUTCDATE(),
    0
FROM [etl].[stg_hierarchy_versions]
WHERE EXISTS (SELECT 1 FROM [dbo].[Hierarchies] WHERE Id = stg_hierarchy_versions.HierarchyId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' HierarchyVersions';
PRINT '';
GO

-- ================================================================
-- Export Hierarchy Participants (NO SplitPercent in production!)
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
-- Export Policies (matching production schema)
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
    COALESCE(p.PolicyType, 1) AS PolicyType,
    COALESCE(TRY_CAST(p.Status AS INT), 1) AS Status,
    p.StatusDate,
    COALESCE(p.BrokerId, 0) AS BrokerId,
    p.ContractId,
    CASE 
        WHEN p.GroupId IS NULL OR p.GroupId = '' THEN NULL
        WHEN TRY_CAST(p.GroupId AS BIGINT) IS NOT NULL THEN CONCAT('G', CAST(TRY_CAST(p.GroupId AS BIGINT) AS VARCHAR))
        ELSE p.GroupId 
    END AS GroupId,
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
FROM [etl].[stg_policies] p
LEFT JOIN [dbo].[EmployerGroups] g ON g.Id = CASE 
    WHEN TRY_CAST(p.GroupId AS BIGINT) IS NOT NULL THEN CONCAT('G', CAST(TRY_CAST(p.GroupId AS BIGINT) AS VARCHAR))
    ELSE p.GroupId 
END
WHERE (p.GroupId IS NULL OR p.GroupId = '' OR g.Id IS NOT NULL);  -- Only if group exists or is NULL

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Policies';
PRINT '';
GO

-- ================================================================
-- Export PolicyHierarchyAssignments (matching production schema)
-- ================================================================

PRINT 'Exporting PolicyHierarchyAssignments...';

INSERT INTO [dbo].[PolicyHierarchyAssignments] (
    Id,
    PolicyId,
    CertificateId,
    HierarchyId,
    SplitPercent,
    WritingBrokerId,
    IsNonConforming,
    SourceTraceabilityReportId,
    CreationTime,
    IsDeleted
)
SELECT 
    CONCAT('PHA-', ROW_NUMBER() OVER (ORDER BY pha.PolicyId, pha.SplitSequence)),
    pha.PolicyId,
    NULL AS CertificateId,
    NULL AS HierarchyId,
    pha.SplitPercent,
    pha.WritingBrokerId,
    1 AS IsNonConforming,  -- These are non-conformant by definition
    NULL AS SourceTraceabilityReportId,
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
PRINT 'FINAL VERIFICATION';
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
PRINT '✅ ALL PROPOSAL DATA EXPORTED TO PRODUCTION!';
GO
