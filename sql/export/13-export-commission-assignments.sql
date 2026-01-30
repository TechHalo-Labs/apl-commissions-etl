-- ================================================================
-- Export: Commission Assignments
-- ================================================================
-- Exports commission assignment versions and recipients to production
-- ================================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '================================================================';
PRINT 'Export: Commission Assignments';
PRINT '================================================================';
PRINT '';

-- ================================================================
-- Step 1: Clear existing assignment data
-- ================================================================

PRINT 'Step 1: Clearing existing assignment data...';

DELETE FROM [dbo].[CommissionAssignmentRecipients];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' CommissionAssignmentRecipients';

DELETE FROM [dbo].[CommissionAssignmentVersions];
PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' CommissionAssignmentVersions';

PRINT '';

-- ================================================================
-- Step 2: Export CommissionAssignmentVersions
-- ================================================================

PRINT 'Step 2: Exporting CommissionAssignmentVersions...';

INSERT INTO [dbo].[CommissionAssignmentVersions] (
    Id,
    BrokerId,
    BrokerName,
    ProposalId,
    GroupId,
    HierarchyId,
    HierarchyVersionId,
    HierarchyParticipantId,
    VersionNumber,
    EffectiveFrom,
    EffectiveTo,
    Status,
    Type,
    ChangeDescription,
    TotalAssignedPercent,
    CreationTime,
    CreatorUserId,
    LastModificationTime,
    LastModifierUserId,
    IsDeleted,
    DeleterUserId,
    DeletionTime
)
SELECT 
    v.Id,
    v.BrokerId,
    v.BrokerName,
    v.ProposalId,
    v.GroupId,
    v.HierarchyId,
    v.HierarchyVersionId,
    v.HierarchyParticipantId,
    v.VersionNumber,
    v.EffectiveFrom,
    v.EffectiveTo,
    v.Status,
    v.Type,
    v.ChangeDescription,
    v.TotalAssignedPercent,
    v.CreationTime,
    NULL as CreatorUserId,
    NULL as LastModificationTime,
    NULL as LastModifierUserId,
    0 as IsDeleted,
    NULL as DeleterUserId,
    NULL as DeletionTime
FROM [$(ETL_SCHEMA)].[stg_commission_assignment_versions] v
WHERE v.BrokerId IS NOT NULL; -- Only if source broker exists in our system

DECLARE @versionCount INT = @@ROWCOUNT;
PRINT '  ✓ Exported ' + CAST(@versionCount AS VARCHAR) + ' CommissionAssignmentVersions';
PRINT '';

-- ================================================================
-- Step 3: Export CommissionAssignmentRecipients
-- ================================================================

PRINT 'Step 3: Exporting CommissionAssignmentRecipients...';

INSERT INTO [dbo].[CommissionAssignmentRecipients] (
    Id,
    VersionId,
    RecipientBrokerId,
    RecipientName,
    RecipientNPN,
    Percentage,
    RecipientHierarchyId,
    Notes
)
SELECT 
    r.Id,
    r.AssignmentVersionId AS VersionId,  -- Staging uses AssignmentVersionId
    r.RecipientBrokerId,
    r.RecipientBrokerName AS RecipientName,  -- Staging uses RecipientBrokerName
    NULL AS RecipientNPN,  -- Not in staging table
    r.[Percent] AS Percentage,  -- Staging uses Percent
    NULL AS RecipientHierarchyId,  -- Not in staging table
    NULL AS Notes  -- Not in staging table
FROM [$(ETL_SCHEMA)].[stg_commission_assignment_recipients] r
WHERE r.RecipientBrokerId IS NOT NULL
  AND EXISTS (SELECT 1 FROM [dbo].[CommissionAssignmentVersions] v WHERE v.Id = r.AssignmentVersionId);

DECLARE @recipientCount INT = @@ROWCOUNT;
PRINT '  ✓ Exported ' + CAST(@recipientCount AS VARCHAR) + ' CommissionAssignmentRecipients';
PRINT '';

-- ================================================================
-- Step 4: Verification
-- ================================================================

PRINT '================================================================';
PRINT 'Verification:';
PRINT '================================================================';

SELECT 
    'CommissionAssignmentVersions' as [Table],
    COUNT(*) as [Count]
FROM [dbo].[CommissionAssignmentVersions]
UNION ALL
SELECT 
    'CommissionAssignmentRecipients',
    COUNT(*)
FROM [dbo].[CommissionAssignmentRecipients];

PRINT '';
PRINT 'Sample exported assignments:';
SELECT TOP 5
    v.Id as VersionId,
    b.Name as SourceBroker,
    r.RecipientName as RecipientBroker,
    r.Percentage,
    v.EffectiveFrom,
    CASE 
        WHEN v.EffectiveTo IS NULL THEN 'Active'
        ELSE CAST(v.EffectiveTo AS VARCHAR(20))
    END as EffectiveTo
FROM [dbo].[CommissionAssignmentVersions] v
INNER JOIN [dbo].[Brokers] b ON b.Id = v.BrokerId
LEFT JOIN [dbo].[CommissionAssignmentRecipients] r ON r.VersionId = v.Id
ORDER BY v.BrokerId;

PRINT '';
PRINT '✓ Commission Assignments Export Complete';
PRINT '================================================================';
GO
