-- ================================================================
-- DESTRUCTIVE EXPORT: Complete ETL to Production
-- ================================================================
-- WARNING: This will DELETE ALL existing production data
-- ================================================================

SET NOCOUNT ON;
GO

PRINT '╔════════════════════════════════════════════════════════════════╗';
PRINT '║  DESTRUCTIVE EXPORT: CLEARING ALL PRODUCTION DATA             ║';
PRINT '║  WARNING: Deleting existing production data!                  ║';
PRINT '╚════════════════════════════════════════════════════════════════╝';
PRINT '';

-- ================================================================
-- Step 1: Delete existing production data (reverse FK order)
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

PRINT 'Clearing policy and hierarchy assignments...';
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

PRINT 'Clearing groups (EmployerGroups)...';
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

PRINT '✅ All production data cleared';
PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
PRINT 'Now run the export scripts in order:';
PRINT '  1. 01-export-schedules.sql';
PRINT '  2. 02-export-brokers.sql';
PRINT '  3. 05-export-groups.sql';
PRINT '  4. 06-export-products.sql';
PRINT '  5. 07-export-proposals.sql';
PRINT '  6. 08-export-hierarchies.sql';
PRINT '  7. 09-export-policies.sql';
PRINT '  8. 11-export-splits.sql';
PRINT '  9. 13-export-commission-assignments.sql';
PRINT ' 10. 13-export-licenses.sql';
PRINT ' 11. 14-export-policy-hierarchy-assignments.sql';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '';

GO
