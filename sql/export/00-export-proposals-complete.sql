-- ================================================================
-- Complete Proposal Export: Clear and Export All Proposal Data
-- ================================================================
-- Exports proposals, splits, hierarchies, assignments, and policies
-- No conformance filtering - exports ALL data
-- ================================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '================================================================';
PRINT 'EXPORT PROPOSALS, SPLITS, HIERARCHIES, ASSIGNMENTS';
PRINT '================================================================';
PRINT '';

-- ================================================================
-- Step 1: Clear production data (FK order - children first)
-- ================================================================

PRINT 'Step 1: Clearing production proposal data...';
PRINT '';

DELETE FROM [dbo].[CommissionAssignmentRecipients];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' CommissionAssignmentRecipients';

DELETE FROM [dbo].[CommissionAssignmentVersions];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' CommissionAssignmentVersions';

DELETE FROM [dbo].[PolicyHierarchyAssignments];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' PolicyHierarchyAssignments';

DELETE FROM [dbo].[PremiumTransactions];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' PremiumTransactions';

DELETE FROM [dbo].[Policies];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Policies';

DELETE FROM [dbo].[PremiumSplitParticipants];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' PremiumSplitParticipants';

DELETE FROM [dbo].[PremiumSplitVersions];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' PremiumSplitVersions';

DELETE FROM [dbo].[HierarchyParticipants];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' HierarchyParticipants';

DELETE FROM [dbo].[HierarchyVersions];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' HierarchyVersions';

DELETE FROM [dbo].[Hierarchies];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Hierarchies';

DELETE FROM [dbo].[ProposalProducts];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' ProposalProducts';

DELETE FROM [dbo].[Proposals];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Proposals';

PRINT '';
GO

-- ================================================================
-- Step 2: Export Proposals
-- ================================================================

PRINT 'Step 2: Exporting Proposals...';

INSERT INTO [dbo].[Proposals] (
    Id,
    ProposalNumber,
    [Status],
    SubmittedDate,
    ProposedEffectiveDate,
    SpecialCase,
    SpecialCaseCode,
    SitusState,
    BrokerUniquePartyId,
    BrokerName,
    GroupId,
    GroupName,
    EffectiveDateFrom,
    EffectiveDateTo,
    EnablePlanCodeFiltering,
    EnableEffectiveDateFiltering,
    ConstrainingEffectiveDateFrom,
    ConstrainingEffectiveDateTo,
    CreationTime,
    IsDeleted
)
SELECT 
    sp.Id,
    sp.ProposalNumber,
    COALESCE(sp.[Status], 1) AS [Status],
    COALESCE(sp.SubmittedDate, sp.EffectiveDateFrom, GETUTCDATE()) AS SubmittedDate,
    COALESCE(sp.ProposedEffectiveDate, sp.EffectiveDateFrom, GETUTCDATE()) AS ProposedEffectiveDate,
    0 AS SpecialCase,
    0 AS SpecialCaseCode,
    sp.SitusState,
    NULL AS BrokerUniquePartyId,  -- Will be populated later if needed
    NULL AS BrokerName,
    CONCAT('G', sp.GroupId) AS GroupId,  -- Add G prefix for production
    sp.GroupName,
    sp.EffectiveDateFrom,
    sp.EffectiveDateTo,
    0 AS EnablePlanCodeFiltering,
    0 AS EnableEffectiveDateFiltering,
    NULL AS ConstrainingEffectiveDateFrom,
    NULL AS ConstrainingEffectiveDateTo,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[stg_proposals] sp;

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Proposals';
PRINT '';
GO

-- ================================================================
-- Step 3: Export Premium Split Versions
-- ================================================================

PRINT 'Step 3: Exporting Premium Split Versions...';

INSERT INTO [dbo].[PremiumSplitVersions] (
    Id,
    GroupId,
    GroupName,
    ProposalId,
    ProposalNumber,
    VersionNumber,
    EffectiveFrom,
    EffectiveTo,
    TotalSplitPercent,
    [Status],
    CreationTime,
    IsDeleted
)
SELECT 
    Id,
    CONCAT('G', GroupId) AS GroupId,  -- Add G prefix
    GroupName,
    ProposalId,
    ProposalNumber,
    VersionNumber,
    EffectiveFrom,
    EffectiveTo,
    TotalSplitPercent,
    [Status],
    GETUTCDATE(),
    0
FROM [etl].[stg_premium_split_versions];

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Premium Split Versions';
PRINT '';
GO

-- ================================================================
-- Step 4: Export Premium Split Participants
-- ================================================================

PRINT 'Step 4: Exporting Premium Split Participants...';

INSERT INTO [dbo].[PremiumSplitParticipants] (
    Id,
    VersionId,
    BrokerId,
    BrokerName,
    SplitPercent,
    IsWritingAgent,
    HierarchyId,
    Sequence,
    WritingBrokerId,
    GroupId,
    EffectiveFrom,
    EffectiveTo,
    CreationTime,
    IsDeleted
)
SELECT 
    Id,
    VersionId,
    BrokerId,
    BrokerName,
    SplitPercent,
    IsWritingAgent,
    HierarchyId,
    Sequence,
    WritingBrokerId,
    CONCAT('G', GroupId) AS GroupId,  -- Add G prefix
    EffectiveFrom,
    EffectiveTo,
    GETUTCDATE(),
    0
FROM [etl].[stg_premium_split_participants];

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Premium Split Participants';
PRINT '';
GO

-- ================================================================
-- Step 5: Export Hierarchies
-- ================================================================

PRINT 'Step 5: Exporting Hierarchies...';

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
    [Status],
    CreationTime,
    IsDeleted
)
SELECT 
    Id,
    Name,
    CONCAT('G', GroupId) AS GroupId,  -- Add G prefix
    GroupName,
    BrokerId,
    BrokerName,
    ProposalId,
    SitusState,
    VersionNumber AS CurrentVersionId,
    [Status],
    GETUTCDATE(),
    0
FROM [etl].[stg_hierarchies];

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Hierarchies';
PRINT '';
GO

-- ================================================================
-- Step 6: Export Hierarchy Versions
-- ================================================================

PRINT 'Step 6: Exporting Hierarchy Versions...';

INSERT INTO [dbo].[HierarchyVersions] (
    Id,
    HierarchyId,
    VersionNumber,
    EffectiveFrom,
    EffectiveTo,
    [Status],
    CreationTime,
    IsDeleted
)
SELECT 
    Id,
    HierarchyId,
    VersionNumber,
    EffectiveFrom,
    EffectiveTo,
    [Status],
    GETUTCDATE(),
    0
FROM [etl].[stg_hierarchy_versions];

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Hierarchy Versions';
PRINT '';
GO

-- ================================================================
-- Step 7: Export Hierarchy Participants
-- ================================================================

PRINT 'Step 7: Exporting Hierarchy Participants...';

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
    ScheduleId,
    CreationTime,
    IsDeleted
)
SELECT 
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
    ScheduleId,
    GETUTCDATE(),
    0
FROM [etl].[stg_hierarchy_participants];

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Hierarchy Participants';
PRINT '';
GO

-- ================================================================
-- Step 8: Export Policies
-- ================================================================

PRINT 'Step 8: Exporting Policies...';

INSERT INTO [dbo].[Policies] (
    PolicyId,
    GroupId,
    CertificateNumber,
    PremiumAmount,
    ProductCode,
    PlanCode,
    CertEffectiveDate,
    CertIssuedState,
    CertStatus,
    SplitConfigHash,
    ProposalId,
    CreationTime,
    IsDeleted
)
SELECT 
    p.PolicyId,
    CASE WHEN p.GroupId IS NULL OR p.GroupId = '' THEN NULL 
         ELSE CONCAT('G', p.GroupId) 
    END AS GroupId,
    p.CertificateNumber,
    p.PremiumAmount,
    p.ProductCode,
    p.PlanCode,
    p.CertEffectiveDate,
    p.CertIssuedState,
    p.CertStatus,
    p.SplitConfigHash,
    pkm.ProposalId,
    GETUTCDATE(),
    0
FROM [etl].[stg_policies] p
LEFT JOIN [etl].[stg_proposal_key_mapping] pkm 
    ON pkm.GroupId = p.GroupId
    AND pkm.EffectiveYear = YEAR(p.CertEffectiveDate)
    AND pkm.ProductCode = p.ProductCode
    AND pkm.PlanCode = p.PlanCode;

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Policies';
PRINT '';
GO

-- ================================================================
-- Step 9: Export PolicyHierarchyAssignments
-- ================================================================

PRINT 'Step 9: Exporting PolicyHierarchyAssignments...';

INSERT INTO [dbo].[PolicyHierarchyAssignments] (
    Id,
    PolicyId,
    WritingBrokerId,
    SplitSequence,
    SplitPercent,
    NonConformantReason,
    CreationTime,
    IsDeleted
)
SELECT 
    CONCAT('PHA-', ROW_NUMBER() OVER (ORDER BY PolicyId, SplitSequence)),
    PolicyId,
    WritingBrokerId,
    SplitSequence,
    SplitPercent,
    NonConformantReason,
    GETUTCDATE(),
    0
FROM [etl].[stg_policy_hierarchy_assignments];

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' PolicyHierarchyAssignments';
PRINT '';
GO

-- ================================================================
-- Step 10: Note about Commission Assignments
-- ================================================================

PRINT 'Step 10: Commission Assignments Status...';
PRINT '   (Already populated by proposal builder - no export needed)';

SELECT COUNT(*) as assignment_count 
FROM [dbo].[CommissionAssignmentVersions];

SELECT COUNT(*) as recipient_count 
FROM [dbo].[CommissionAssignmentRecipients];

PRINT '';
GO

-- ================================================================
-- Step 11: Verification
-- ================================================================

PRINT '================================================================';
PRINT 'VERIFICATION - Production Data Counts';
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
PRINT 'Sample Assignments:';
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
