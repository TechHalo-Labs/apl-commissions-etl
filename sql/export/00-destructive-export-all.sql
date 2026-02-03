-- ================================================================
-- DESTRUCTIVE EXPORT: Complete ETL to Production
-- ================================================================
-- WARNING: This will DELETE ALL existing production data and
--          replace it with staging data
-- ================================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '╔════════════════════════════════════════════════════════════════╗';
PRINT '║  DESTRUCTIVE EXPORT: ALL ENTITIES                              ║';
PRINT '║  WARNING: This will DELETE existing production data!           ║';
PRINT '╚════════════════════════════════════════════════════════════════╝';
PRINT '';

-- ================================================================
-- Step 1: Delete existing production data (reverse FK order)
-- ================================================================

PRINT 'Step 1: Clearing existing production data...';
PRINT '';

-- Commission data (delete first - has FKs to everything)
DELETE FROM [dbo].[GLJournalLinesDryRun];
DELETE FROM [dbo].[GLJournalEntriesDryRun];
DELETE FROM [dbo].[RunBrokerTraceabilities];
DELETE FROM [dbo].[BrokerTraceabilities];
DELETE FROM [dbo].[CommissionTraceabilityReports];
DELETE FROM [dbo].[CommissionRunPremiums];
DELETE FROM [dbo].[PremiumTransactions];
DELETE FROM [dbo].[CommissionRuns];
PRINT '  ✓ Cleared commission data';

-- Policy hierarchy assignments
DELETE FROM [dbo].[PolicyHierarchyAssignments];
PRINT '  ✓ Cleared PolicyHierarchyAssignments';

-- Commission assignments
DELETE FROM [dbo].[CommissionAssignmentRecipients];
DELETE FROM [dbo].[CommissionAssignmentVersions];
PRINT '  ✓ Cleared CommissionAssignments';

-- Split distributions and hierarchy splits
DELETE FROM [dbo].[SplitDistributions];
DELETE FROM [dbo].[HierarchySplits];
PRINT '  ✓ Cleared Splits and Distributions';

-- State rules
DELETE FROM [dbo].[StateRuleStates];
DELETE FROM [dbo].[StateRules];
PRINT '  ✓ Cleared StateRules';

-- Hierarchy participants and versions
DELETE FROM [dbo].[HierarchyParticipants];
DELETE FROM [dbo].[HierarchyVersions];
DELETE FROM [dbo].[Hierarchies];
PRINT '  ✓ Cleared Hierarchies';

-- Premium splits
DELETE FROM [dbo].[PremiumSplitParticipants];
DELETE FROM [dbo].[PremiumSplitVersions];
PRINT '  ✓ Cleared PremiumSplits';

-- Proposals and products
DELETE FROM [dbo].[ProposalProducts];
DELETE FROM [dbo].[Proposals];
PRINT '  ✓ Cleared Proposals';

-- Policies
DELETE FROM [dbo].[Policies];
PRINT '  ✓ Cleared Policies';

-- Groups
DELETE FROM [dbo].[Group];
PRINT '  ✓ Cleared Groups';

-- Products and Plans
DELETE FROM [dbo].[Products];
DELETE FROM [dbo].[Plans];
PRINT '  ✓ Cleared Products and Plans';

-- Brokers and related
DELETE FROM [dbo].[BrokerBankingInfos];
DELETE FROM [dbo].[BrokerLicenses];
DELETE FROM [dbo].[Brokers];
PRINT '  ✓ Cleared Brokers';

-- Schedules (COMMENTED OUT - preserving existing production schedules)
-- DELETE FROM [dbo].[ScheduleRateTiers];
-- DELETE FROM [dbo].[SpecialScheduleRates];
-- DELETE FROM [dbo].[FeeSchedules];
-- DELETE FROM [dbo].[Schedules];
-- PRINT '  ✓ Cleared Schedules';
PRINT '  ⏭️  Skipped Schedules (preserving existing production data)';

PRINT '';
PRINT '✅ All production data cleared';
PRINT '';
GO

-- ================================================================
-- Step 2: Export Schedules (COMMENTED OUT - preserving existing production schedules)
-- ================================================================

PRINT 'Step 2: Skipping Schedules (preserving existing production data)...';

-- SET IDENTITY_INSERT [dbo].[Schedules] ON;
-- 
-- INSERT INTO [dbo].[Schedules] (
--     Id, ExternalId, Name, Description, Status, CommissionType,
--     RateStructure, EffectiveDate, EndDate, ProductLines, ProductCodes,
--     Owner, ContractCount, ProductCount, CurrentVersionId,
--     CurrentVersionNumber, CreationTime, IsDeleted
-- )
-- SELECT 
--     Id, ExternalId, Name, Description, Status, CommissionType,
--     RateStructure, EffectiveDate, EndDate, ProductLines, ProductCodes,
--     Owner, ContractCount, ProductCount, CurrentVersionId,
--     CurrentVersionNumber, CreationTime, IsDeleted
-- FROM [etl].[stg_schedules];
-- 
-- SET IDENTITY_INSERT [dbo].[Schedules] OFF;
-- 
-- PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Schedules';

PRINT '  ⏭️  Skipped - using existing production Schedules';
PRINT '';
GO

-- ================================================================
-- Step 3: Export Brokers
-- ================================================================

PRINT 'Step 3: Exporting Brokers...';

SET IDENTITY_INSERT [dbo].[Brokers] ON;

INSERT INTO [dbo].[Brokers] (
    Id, ExternalPartyId, Name, Status, Type, NPN,
    Email, Phone, Address, City, State, ZipCode,
    IsActive, CreationTime, IsDeleted
)
SELECT 
    Id, ExternalPartyId, Name, Status, Type, NPN,
    Email, Phone, Address, City, State, ZipCode,
    IsActive, CreationTime, IsDeleted
FROM [etl].[stg_brokers];

SET IDENTITY_INSERT [dbo].[Brokers] OFF;

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Brokers';
PRINT '';
GO

-- ================================================================
-- Step 4: Export Broker Licenses
-- ================================================================

PRINT 'Step 4: Exporting Broker Licenses...';

SET IDENTITY_INSERT [dbo].[BrokerLicenses] ON;

INSERT INTO [dbo].[BrokerLicenses] (
    Id, BrokerId, State, LicenseNumber, Type, Status,
    EffectiveDate, ExpirationDate, GracePeriodDate,
    LicenseCode, IsResidentLicense, ApplicableCounty,
    CreationTime, IsDeleted
)
SELECT 
    Id,
    BrokerId,
    COALESCE(State, 'XX') AS State,
    COALESCE(LicenseNumber, 'N/A') AS LicenseNumber,
    COALESCE(Type, 0) AS Type,
    COALESCE(Status, 0) AS Status,
    COALESCE(EffectiveDate, GETUTCDATE()) AS EffectiveDate,
    COALESCE(ExpirationDate, '2099-01-01') AS ExpirationDate,
    '2099-01-01' AS GracePeriodDate,  -- Far-future grace period
    LicenseCode,
    COALESCE(IsResidentLicense, 0) AS IsResidentLicense,
    ApplicableCounty,
    COALESCE(CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_broker_licenses]
WHERE BrokerId IS NOT NULL
  AND EXISTS (SELECT 1 FROM [dbo].[Brokers] WHERE Id = stg_broker_licenses.BrokerId);

SET IDENTITY_INSERT [dbo].[BrokerLicenses] OFF;

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Broker Licenses';

-- Create corresponding Broker Appointments for each license state
PRINT '  Creating Broker Appointments for license states...';

-- State name lookup (inline for simplicity)
INSERT INTO [dbo].[BrokerAppointments] (
    BrokerId, StateCode, StateName, LicenseCode, LicenseCodeLabel,
    EffectiveDate, ExpirationDate, GracePeriodDate, OriginalEffectiveDate,
    Status, NiprStatus, IsCommissionEligible,
    CreationTime, IsDeleted
)
SELECT DISTINCT
    bl.BrokerId,
    bl.State AS StateCode,
    CASE bl.State 
        WHEN 'AL' THEN 'Alabama' WHEN 'AK' THEN 'Alaska' WHEN 'AZ' THEN 'Arizona' 
        WHEN 'AR' THEN 'Arkansas' WHEN 'CA' THEN 'California' WHEN 'CO' THEN 'Colorado'
        WHEN 'CT' THEN 'Connecticut' WHEN 'DE' THEN 'Delaware' WHEN 'FL' THEN 'Florida'
        WHEN 'GA' THEN 'Georgia' WHEN 'HI' THEN 'Hawaii' WHEN 'ID' THEN 'Idaho'
        WHEN 'IL' THEN 'Illinois' WHEN 'IN' THEN 'Indiana' WHEN 'IA' THEN 'Iowa'
        WHEN 'KS' THEN 'Kansas' WHEN 'KY' THEN 'Kentucky' WHEN 'LA' THEN 'Louisiana'
        WHEN 'ME' THEN 'Maine' WHEN 'MD' THEN 'Maryland' WHEN 'MA' THEN 'Massachusetts'
        WHEN 'MI' THEN 'Michigan' WHEN 'MN' THEN 'Minnesota' WHEN 'MS' THEN 'Mississippi'
        WHEN 'MO' THEN 'Missouri' WHEN 'MT' THEN 'Montana' WHEN 'NE' THEN 'Nebraska'
        WHEN 'NV' THEN 'Nevada' WHEN 'NH' THEN 'New Hampshire' WHEN 'NJ' THEN 'New Jersey'
        WHEN 'NM' THEN 'New Mexico' WHEN 'NY' THEN 'New York' WHEN 'NC' THEN 'North Carolina'
        WHEN 'ND' THEN 'North Dakota' WHEN 'OH' THEN 'Ohio' WHEN 'OK' THEN 'Oklahoma'
        WHEN 'OR' THEN 'Oregon' WHEN 'PA' THEN 'Pennsylvania' WHEN 'RI' THEN 'Rhode Island'
        WHEN 'SC' THEN 'South Carolina' WHEN 'SD' THEN 'South Dakota' WHEN 'TN' THEN 'Tennessee'
        WHEN 'TX' THEN 'Texas' WHEN 'UT' THEN 'Utah' WHEN 'VT' THEN 'Vermont'
        WHEN 'VA' THEN 'Virginia' WHEN 'WA' THEN 'Washington' WHEN 'WV' THEN 'West Virginia'
        WHEN 'WI' THEN 'Wisconsin' WHEN 'WY' THEN 'Wyoming' WHEN 'DC' THEN 'District of Columbia'
        ELSE bl.State
    END AS StateName,
    bl.Type AS LicenseCode,
    CASE bl.Type 
        WHEN 0 THEN 'Life' WHEN 1 THEN 'Health' WHEN 2 THEN 'Variable' ELSE 'Other'
    END AS LicenseCodeLabel,
    bl.EffectiveDate,
    bl.ExpirationDate,
    '2099-01-01' AS GracePeriodDate,  -- Far-future grace period
    bl.EffectiveDate AS OriginalEffectiveDate,
    bl.Status,
    'Active' AS NiprStatus,
    1 AS IsCommissionEligible,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [dbo].[BrokerLicenses] bl
WHERE NOT EXISTS (
    SELECT 1 FROM [dbo].[BrokerAppointments] ba 
    WHERE ba.BrokerId = bl.BrokerId AND ba.StateCode = bl.State
);

PRINT '  ✓ Created ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Broker Appointments';

-- Update all appointments to have 2099-01-01 grace period
UPDATE [dbo].[BrokerAppointments]
SET GracePeriodDate = '2099-01-01'
WHERE GracePeriodDate IS NULL OR GracePeriodDate <> '2099-01-01';

PRINT '  ✓ Updated appointments with 2099-01-01 grace period';
PRINT '';
GO

-- ================================================================
-- Step 5: Export Groups
-- ================================================================

PRINT 'Step 5: Exporting Groups...';

INSERT INTO [dbo].[Group] (
    Id, GroupNumber, GroupName, GroupSize, SitusState,
    EffectiveDate, TerminationDate, Status, PrimaryBrokerId,
    CreationTime, IsDeleted
)
SELECT 
    Id, GroupNumber, GroupName, GroupSize, SitusState,
    EffectiveDate, TerminationDate, Status, PrimaryBrokerId,
    CreationTime, IsDeleted
FROM [etl].[stg_groups];

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Groups';
PRINT '';
GO

-- ================================================================
-- Step 6: Export Products
-- ================================================================

PRINT 'Step 6: Exporting Products...';

INSERT INTO [dbo].[Products] (
    Id, ProductCode, ProductName, Category, Type,
    IsActive, CreationTime, IsDeleted
)
SELECT 
    Id, ProductCode, ProductName, Category, Type,
    IsActive, CreationTime, IsDeleted
FROM [etl].[stg_products];

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Products';
PRINT '';
GO

-- ================================================================
-- Step 7: Export Plans
-- ================================================================

PRINT 'Step 7: Exporting Plans...';

INSERT INTO [dbo].[Plans] (
    Id, PlanCode, PlanName, ProductId, Description,
    IsActive, CreationTime, IsDeleted
)
SELECT 
    Id, PlanCode, PlanName, ProductId, Description,
    IsActive, CreationTime, IsDeleted
FROM [etl].[stg_plans]
WHERE EXISTS (SELECT 1 FROM [dbo].[Products] WHERE Id = stg_plans.ProductId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Plans';
PRINT '';
GO

-- ================================================================
-- Step 8: Export Proposals
-- ================================================================

PRINT 'Step 8: Exporting Proposals...';

INSERT INTO [dbo].[Proposals] (
    Id, ProposalNumber, GroupId, GroupName, BrokerId,
    BrokerUniquePartyId, BrokerName, ProductCodes, ProductCount,
    SitusState, Status, EffectiveDate, ExpirationDate,
    CreationTime, IsDeleted
)
SELECT 
    Id, ProposalNumber, GroupId, GroupName, BrokerId,
    BrokerUniquePartyId, BrokerName, ProductCodes, ProductCount,
    SitusState, Status, EffectiveDate, ExpirationDate,
    CreationTime, IsDeleted
FROM [etl].[stg_proposals];

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Proposals';
PRINT '';
GO

-- ================================================================
-- Step 9: Export Proposal Products
-- ================================================================

PRINT 'Step 9: Exporting Proposal Products...';

SET IDENTITY_INSERT [dbo].[ProposalProducts] ON;

INSERT INTO [dbo].[ProposalProducts] (
    Id, ProposalId, ProductCode, ProductName, CreationTime, IsDeleted
)
SELECT 
    Id, ProposalId, ProductCode, ProductName, CreationTime, IsDeleted
FROM [etl].[stg_proposal_products]
WHERE EXISTS (SELECT 1 FROM [dbo].[Proposals] WHERE Id = stg_proposal_products.ProposalId);

SET IDENTITY_INSERT [dbo].[ProposalProducts] OFF;

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Proposal Products';
PRINT '';
GO

-- ================================================================
-- Step 10: Export Premium Split Versions
-- ================================================================

PRINT 'Step 10: Exporting Premium Split Versions...';

INSERT INTO [dbo].[PremiumSplitVersions] (
    Id, ProposalId, GroupId, GroupName, VersionNumber,
    EffectiveFrom, EffectiveTo, ChangeDescription, TotalSplitPercent,
    Status, Source, HubspotDealId, CreationTime, IsDeleted
)
SELECT 
    Id, ProposalId, GroupId, GroupName, VersionNumber,
    EffectiveFrom, EffectiveTo, ChangeDescription, TotalSplitPercent,
    Status, Source, HubspotDealId, CreationTime, IsDeleted
FROM [etl].[stg_premium_split_versions]
WHERE EXISTS (SELECT 1 FROM [dbo].[Proposals] WHERE Id = stg_premium_split_versions.ProposalId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Premium Split Versions';
PRINT '';
GO

-- ================================================================
-- Step 11: Export Premium Split Participants
-- ================================================================

PRINT 'Step 11: Exporting Premium Split Participants...';

INSERT INTO [dbo].[PremiumSplitParticipants] (
    Id, VersionId, BrokerId, BrokerUniquePartyId, BrokerName, BrokerNPN,
    SplitPercent, IsWritingAgent, HierarchyId, HierarchyName,
    TemplateId, TemplateName, EffectiveFrom, EffectiveTo, Notes,
    CreationTime, IsDeleted
)
SELECT 
    Id, VersionId, BrokerId, BrokerUniquePartyId, BrokerName, BrokerNPN,
    SplitPercent, IsWritingAgent, HierarchyId, HierarchyName,
    TemplateId, TemplateName, EffectiveFrom, EffectiveTo, Notes,
    CreationTime, IsDeleted
FROM [etl].[stg_premium_split_participants]
WHERE EXISTS (SELECT 1 FROM [dbo].[PremiumSplitVersions] WHERE Id = stg_premium_split_participants.VersionId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Premium Split Participants';
PRINT '';
GO

-- ================================================================
-- Step 12: Export Hierarchies
-- ================================================================

PRINT 'Step 12: Exporting Hierarchies...';

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
    Id, Name, Description, Type, Status, ProposalId, ProposalNumber,
    GroupId, GroupName, GroupNumber, BrokerId, BrokerName, BrokerLevel,
    ContractId, ContractNumber, ContractType, ContractStatus,
    SourceType, HasOverrides, DeviationCount, SitusState,
    COALESCE(EffectiveDate, GETUTCDATE()) AS EffectiveDate,
    CurrentVersionId, CurrentVersionNumber,
    TemplateId, TemplateVersion, TemplateSyncStatus,
    CreationTime, IsDeleted
FROM [etl].[stg_hierarchies];

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Hierarchies';
PRINT '';
GO

-- ================================================================
-- Step 13: Export Hierarchy Versions
-- ================================================================

PRINT 'Step 13: Exporting Hierarchy Versions...';

INSERT INTO [dbo].[HierarchyVersions] (
    Id, HierarchyId, Version, Status, EffectiveFrom, EffectiveTo,
    ChangeReason, CreationTime, IsDeleted
)
SELECT 
    Id, HierarchyId, Version, Status, EffectiveFrom, EffectiveTo,
    ChangeReason, CreationTime, IsDeleted
FROM [etl].[stg_hierarchy_versions]
WHERE EXISTS (SELECT 1 FROM [dbo].[Hierarchies] WHERE Id = stg_hierarchy_versions.HierarchyId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Hierarchy Versions';
PRINT '';
GO

-- ================================================================
-- Step 14: Export Hierarchy Participants
-- ================================================================

PRINT 'Step 14: Exporting Hierarchy Participants...';

INSERT INTO [dbo].[HierarchyParticipants] (
    Id, HierarchyVersionId, EntityId, EntityName, Level, SortOrder,
    ScheduleCode, ScheduleId, CommissionRate, CreationTime, IsDeleted
)
SELECT 
    Id, HierarchyVersionId, EntityId, EntityName, Level, SortOrder,
    ScheduleCode, ScheduleId, CommissionRate, CreationTime, IsDeleted
FROM [etl].[stg_hierarchy_participants]
WHERE EXISTS (SELECT 1 FROM [dbo].[HierarchyVersions] WHERE Id = stg_hierarchy_participants.HierarchyVersionId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Hierarchy Participants';
PRINT '';
GO

-- ================================================================
-- Step 15: Export State Rules
-- ================================================================

PRINT 'Step 15: Exporting State Rules...';

INSERT INTO [dbo].[StateRules] (
    Id, HierarchyVersionId, ShortName, Name, Description,
    Type, SortOrder, CreationTime, IsDeleted
)
SELECT 
    Id, HierarchyVersionId, ShortName, Name, Description,
    Type, SortOrder, CreationTime, IsDeleted
FROM [etl].[stg_state_rules]
WHERE EXISTS (SELECT 1 FROM [dbo].[HierarchyVersions] WHERE Id = stg_state_rules.HierarchyVersionId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' State Rules';
PRINT '';
GO

-- ================================================================
-- Step 16: Export State Rule States
-- ================================================================

PRINT 'Step 16: Exporting State Rule States...';

INSERT INTO [dbo].[StateRuleStates] (
    Id, StateRuleId, StateCode, StateName, CreationTime, IsDeleted
)
SELECT 
    Id, StateRuleId, StateCode, StateName, CreationTime, IsDeleted
FROM [etl].[stg_state_rule_states]
WHERE EXISTS (SELECT 1 FROM [dbo].[StateRules] WHERE Id = stg_state_rule_states.StateRuleId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' State Rule States';
PRINT '';
GO

-- ================================================================
-- Step 17: Export Hierarchy Splits
-- ================================================================

PRINT 'Step 17: Exporting Hierarchy Splits...';

INSERT INTO [dbo].[HierarchySplits] (
    Id, StateRuleId, ProductId, ProductCode, ProductName,
    SortOrder, CreationTime, IsDeleted
)
SELECT 
    Id, StateRuleId, ProductId, ProductCode, ProductName,
    SortOrder, CreationTime, IsDeleted
FROM [etl].[stg_hierarchy_splits]
WHERE EXISTS (SELECT 1 FROM [dbo].[StateRules] WHERE Id = stg_hierarchy_splits.StateRuleId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Hierarchy Splits';
PRINT '';
GO

-- ================================================================
-- Step 18: Export Split Distributions
-- ================================================================

PRINT 'Step 18: Exporting Split Distributions...';

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

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Split Distributions';
PRINT '';
GO

-- ================================================================
-- Step 19: Export Policies
-- ================================================================

PRINT 'Step 19: Exporting Policies...';

INSERT INTO [dbo].[Policies] (
    Id, PolicyNumber, CertificateNumber, OldPolicyNumber,
    PolicyType, Status, StatusDate, BrokerId, ContractId,
    GroupId, CarrierName, CarrierId, ProductCode, ProductName,
    PlanCode, PlanName, MasterCategory, Category,
    InsuredName, InsuredFirstName, InsuredLastName,
    Premium, FaceAmount, PayMode, Frequency,
    EffectiveDate, IssueDate, ExpirationDate,
    State, Division, CompanyCode, LionRecordNumber,
    CustomerId, PaidThroughDate, ProposalId, ProposalAssignedAt,
    ProposalAssignmentSource, CreationTime, IsDeleted
)
SELECT 
    Id, PolicyNumber, CertificateNumber, OldPolicyNumber,
    COALESCE(PolicyType, 0) AS PolicyType,
    COALESCE(Status, 0) AS Status,
    StatusDate, BrokerId, ContractId,
    GroupId, CarrierName, CarrierId, ProductCode, ProductName,
    PlanCode, PlanName, MasterCategory, Category,
    InsuredName, InsuredFirstName, InsuredLastName,
    COALESCE(Premium, 0) AS Premium,
    COALESCE(FaceAmount, 0) AS FaceAmount,
    PayMode, Frequency,
    EffectiveDate, IssueDate, ExpirationDate,
    State, Division, CompanyCode, LionRecordNumber,
    CustomerId, PaidThroughDate, ProposalId, ProposalAssignedAt,
    ProposalAssignmentSource,
    COALESCE(CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_policies];

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Policies';
PRINT '';
GO

-- ================================================================
-- Step 20: Export Policy Hierarchy Assignments
-- ================================================================

PRINT 'Step 20: Exporting Policy Hierarchy Assignments...';

INSERT INTO [dbo].[PolicyHierarchyAssignments] (
    Id, PolicyId, WritingBrokerId, HierarchyId, VersionId,
    ParticipantId, SplitSequence, SplitPercent,
    NonConformantReason, Source, CreationTime, IsDeleted
)
SELECT 
    Id, PolicyId, WritingBrokerId, HierarchyId, VersionId,
    ParticipantId, SplitSequence, SplitPercent,
    NonConformantReason, Source, CreationTime, IsDeleted
FROM [etl].[stg_policy_hierarchy_assignments]
WHERE EXISTS (SELECT 1 FROM [dbo].[Policies] WHERE Id = stg_policy_hierarchy_assignments.PolicyId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Policy Hierarchy Assignments';
PRINT '';
GO

-- ================================================================
-- Step 21: Export Commission Assignments
-- ================================================================

PRINT 'Step 21: Exporting Commission Assignment Versions...';

INSERT INTO [dbo].[CommissionAssignmentVersions] (
    Id, BrokerId, BrokerName, ProposalId, GroupId,
    HierarchyId, HierarchyVersionId, HierarchyParticipantId,
    VersionNumber, EffectiveFrom, EffectiveTo, Status, Type,
    ChangeDescription, TotalAssignedPercent, CreationTime, IsDeleted
)
SELECT 
    Id, BrokerId, BrokerName, ProposalId, GroupId,
    HierarchyId, HierarchyVersionId, HierarchyParticipantId,
    VersionNumber, EffectiveFrom, EffectiveTo, Status, Type,
    ChangeDescription, TotalAssignedPercent, CreationTime, IsDeleted
FROM [etl].[stg_commission_assignment_versions]
WHERE ProposalId = '__DEFAULT__'
   OR EXISTS (SELECT 1 FROM [dbo].[Proposals] WHERE Id = stg_commission_assignment_versions.ProposalId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Commission Assignment Versions';

PRINT '';
PRINT 'Step 22: Exporting Commission Assignment Recipients...';

INSERT INTO [dbo].[CommissionAssignmentRecipients] (
    Id, VersionId, RecipientBrokerId, RecipientName, Percentage,
    CreationTime
)
SELECT 
    Id, 
    AssignmentVersionId AS VersionId, 
    RecipientBrokerId, 
    RecipientBrokerName AS RecipientName,
    [Percent] AS Percentage,
    CreationTime
FROM [etl].[stg_commission_assignment_recipients]
WHERE EXISTS (SELECT 1 FROM [dbo].[CommissionAssignmentVersions] WHERE Id = stg_commission_assignment_recipients.AssignmentVersionId);

PRINT '  ✓ Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Commission Assignment Recipients';
PRINT '';
GO

-- ================================================================
-- Step 23: Verification
-- ================================================================

PRINT '';
PRINT '╔════════════════════════════════════════════════════════════════╗';
PRINT '║  VERIFICATION                                                  ║';
PRINT '╚════════════════════════════════════════════════════════════════╝';
PRINT '';

SELECT 
    'Schedules' as [Table],
    COUNT(*) as [Count]
FROM [dbo].[Schedules]
UNION ALL
SELECT 'Brokers', COUNT(*) FROM [dbo].[Brokers]
UNION ALL
SELECT 'BrokerLicenses', COUNT(*) FROM [dbo].[BrokerLicenses]
UNION ALL
SELECT 'Groups', COUNT(*) FROM [dbo].[Group]
UNION ALL
SELECT 'Products', COUNT(*) FROM [dbo].[Products]
UNION ALL
SELECT 'Plans', COUNT(*) FROM [dbo].[Plans]
UNION ALL
SELECT 'Proposals', COUNT(*) FROM [dbo].[Proposals]
UNION ALL
SELECT 'ProposalProducts', COUNT(*) FROM [dbo].[ProposalProducts]
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
SELECT 'StateRules', COUNT(*) FROM [dbo].[StateRules]
UNION ALL
SELECT 'StateRuleStates', COUNT(*) FROM [dbo].[StateRuleStates]
UNION ALL
SELECT 'HierarchySplits', COUNT(*) FROM [dbo].[HierarchySplits]
UNION ALL
SELECT 'SplitDistributions', COUNT(*) FROM [dbo].[SplitDistributions]
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
PRINT '╔════════════════════════════════════════════════════════════════╗';
PRINT '║  ✅ DESTRUCTIVE EXPORT COMPLETED                               ║';
PRINT '╚════════════════════════════════════════════════════════════════╝';
PRINT '';

GO
