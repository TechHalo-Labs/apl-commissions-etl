-- =====================================================
-- Export CommissionAssignmentVersions and CommissionAssignmentRecipients
-- Only exports records that don't already exist
-- Fixed: Use COALESCE for IsDeleted since staging may not have it
-- =====================================================

PRINT 'Exporting missing CommissionAssignmentVersions...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[CommissionAssignmentVersions] (
    Id, BrokerId, BrokerName, ProposalId, GroupId, HierarchyId, HierarchyVersionId,
    HierarchyParticipantId, VersionNumber, EffectiveFrom, EffectiveTo,
    [Status], [Type], ChangeDescription, TotalAssignedPercent,
    CreationTime, IsDeleted
)
SELECT 
    scav.Id,
    scav.BrokerId,
    scav.BrokerName,
    scav.ProposalId,
    NULL AS GroupId,  -- GroupId should be null per project rules
    scav.HierarchyId,
    scav.HierarchyVersionId,
    scav.HierarchyParticipantId,
    scav.VersionNumber,
    COALESCE(scav.EffectiveFrom, GETUTCDATE()) AS EffectiveFrom,
    scav.EffectiveTo,
    COALESCE(TRY_CAST(scav.[Status] AS INT), 1) AS [Status],  -- 1 = Active
    COALESCE(TRY_CAST(scav.[Type] AS INT), 0) AS [Type],
    scav.ChangeDescription,
    COALESCE(scav.TotalAssignedPercent, 0) AS TotalAssignedPercent,
    COALESCE(scav.CreationTime, GETUTCDATE()) AS CreationTime,
    0 AS IsDeleted  -- Default to not deleted
FROM [$(ETL_SCHEMA)].[stg_commission_assignment_versions] scav
WHERE scav.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[CommissionAssignmentVersions]);

DECLARE @cavCount INT;
SELECT @cavCount = @@ROWCOUNT;
PRINT 'CommissionAssignmentVersions exported: ' + CAST(@cavCount AS VARCHAR);
GO

PRINT 'Exporting missing CommissionAssignmentRecipients...';

-- Production schema: Id, VersionId, RecipientBrokerId, RecipientName, RecipientNPN, Percentage, RecipientHierarchyId, Notes
-- Staging schema: Id, AssignmentVersionId, RecipientBrokerId, RecipientBrokerName, Percent, RecipientType, CreationTime, IsDeleted

INSERT INTO [$(PRODUCTION_SCHEMA)].[CommissionAssignmentRecipients] (
    Id, VersionId, RecipientBrokerId, RecipientName, 
    RecipientNPN, Percentage, RecipientHierarchyId, Notes
)
SELECT 
    scar.Id,
    scar.AssignmentVersionId AS VersionId,
    scar.RecipientBrokerId,
    scar.RecipientBrokerName AS RecipientName,
    NULL AS RecipientNPN,
    scar.[Percent] AS Percentage,
    NULL AS RecipientHierarchyId,
    NULL AS Notes
FROM [$(ETL_SCHEMA)].[stg_commission_assignment_recipients] scar
WHERE scar.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[CommissionAssignmentRecipients])
  -- Ensure the parent assignment version exists
  AND scar.AssignmentVersionId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[CommissionAssignmentVersions]);

DECLARE @carCount INT = @@ROWCOUNT;
PRINT 'CommissionAssignmentRecipients exported: ' + CAST(@carCount AS VARCHAR);
GO

PRINT '=== Assignment Export Complete ===';
