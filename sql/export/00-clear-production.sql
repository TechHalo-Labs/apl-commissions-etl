-- ================================================================
-- DESTRUCTIVE CLEAR: Delete all production data
-- ================================================================
-- WARNING: This will DELETE ALL existing production data
-- Make sure you've backed up to backup schema first!
-- ================================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '╔════════════════════════════════════════════════════════════════╗';
PRINT '║  DESTRUCTIVE CLEAR: Deleting ALL Production Data              ║';
PRINT '║  ⚠️  WARNING: This is IRREVERSIBLE without backup!             ║';
PRINT '╚════════════════════════════════════════════════════════════════╝';
PRINT '';

-- ================================================================
-- Delete in reverse FK dependency order
-- ================================================================

PRINT 'Clearing commission data...';
DELETE FROM [dbo].[GLJournalLinesDryRun];
DELETE FROM [dbo].[GLJournalEntriesDryRun];
DELETE FROM [dbo].[RunBrokerTraceabilities];
DELETE FROM [dbo].[BrokerTraceabilities];
DELETE FROM [dbo].[CommissionTraceabilityReports];
DELETE FROM [dbo].[CommissionRunPremiums];
DELETE FROM [dbo].[PremiumTransactions];
DELETE FROM [dbo].[CommissionRuns];
PRINT '  ✓ Commission data cleared';
PRINT '';

PRINT 'Clearing policy hierarchy assignments...';
DELETE FROM [dbo].[PolicyHierarchyAssignments];
PRINT '  ✓ PolicyHierarchyAssignments cleared';
PRINT '';

PRINT 'Clearing commission assignments...';
DELETE FROM [dbo].[CommissionAssignmentRecipients];
DELETE FROM [dbo].[CommissionAssignmentVersions];
PRINT '  ✓ CommissionAssignments cleared';
PRINT '';

PRINT 'Clearing split distributions and hierarchy splits...';
DELETE FROM [dbo].[SplitDistributions];
DELETE FROM [dbo].[HierarchySplits];
PRINT '  ✓ Splits and Distributions cleared';
PRINT '';

PRINT 'Clearing state rules...';
DELETE FROM [dbo].[StateRuleStates];
DELETE FROM [dbo].[StateRules];
PRINT '  ✓ StateRules cleared';
PRINT '';

PRINT 'Clearing hierarchies...';
DELETE FROM [dbo].[HierarchyParticipants];
DELETE FROM [dbo].[HierarchyVersions];
DELETE FROM [dbo].[Hierarchies];
PRINT '  ✓ Hierarchies cleared';
PRINT '';

PRINT 'Clearing premium splits...';
DELETE FROM [dbo].[PremiumSplitParticipants];
DELETE FROM [dbo].[PremiumSplitVersions];
PRINT '  ✓ PremiumSplits cleared';
PRINT '';

PRINT 'Clearing proposals...';
DELETE FROM [dbo].[ProposalProducts];
DELETE FROM [dbo].[Proposals];
PRINT '  ✓ Proposals cleared';
PRINT '';

PRINT 'Clearing policies...';
DELETE FROM [dbo].[Policies];
PRINT '  ✓ Policies cleared';
PRINT '';

PRINT 'Clearing employer groups...';
DELETE FROM [dbo].[EmployerGroups];
PRINT '  ✓ EmployerGroups cleared';
PRINT '';

PRINT 'Clearing products and plans...';
DELETE FROM [dbo].[Products];
DELETE FROM [dbo].[Plans];
PRINT '  ✓ Products and Plans cleared';
PRINT '';

PRINT 'Clearing brokers and licenses...';
DELETE FROM [dbo].[BrokerBankingInfos];
DELETE FROM [dbo].[BrokerLicenses];
DELETE FROM [dbo].[Brokers];
PRINT '  ✓ Brokers cleared';
PRINT '';

PRINT 'Clearing schedules...';
DELETE FROM [dbo].[ScheduleRateTiers];
DELETE FROM [dbo].[SpecialScheduleRates];
DELETE FROM [dbo].[FeeSchedules];
DELETE FROM [dbo].[Schedules];
PRINT '  ✓ Schedules cleared';
PRINT '';

PRINT '╔════════════════════════════════════════════════════════════════╗';
PRINT '║  ✅ PRODUCTION DATA CLEARED                                    ║';
PRINT '╚════════════════════════════════════════════════════════════════╝';
PRINT '';
PRINT 'All production tables have been truncated.';
PRINT 'Backup timestamp: 20260129_151605';
PRINT '';
PRINT 'To restore: Run /tmp/restore-from-backup.sql';
PRINT '';

GO
