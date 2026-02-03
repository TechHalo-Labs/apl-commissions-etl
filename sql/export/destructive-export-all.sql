-- ================================================================
-- TARGETED DESTRUCTIVE EXPORT: Preserve Safe Data, Replace Hierarchies
-- ================================================================
-- SAFE TO PRESERVE (100% unaffected by hierarchy changes):
--   - EmployerGroups, Products, Plans, Brokers, Schedules, FeeSchedules
-- WARNING: Deletes hierarchy-related data and regenerates it
-- ================================================================

SET NOCOUNT ON;
GO

PRINT '╔════════════════════════════════════════════════════════════════╗';
PRINT '║  TARGETED EXPORT: PRESERVING SAFE DATA, REPLACING HIERARCHIES ║';
PRINT '║  SAFE: EmployerGroups, Products, Plans, Brokers, Schedules     ║';
PRINT '║  REPLACED: All hierarchy-related tables                        ║';
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

-- EmployerGroups, Products, and Plans are preserved (not affected by hierarchy changes)

PRINT 'Clearing broker details (keeping Brokers table)...';
DELETE FROM [dbo].[BrokerBankingInfos];
DELETE FROM [dbo].[BrokerLicenses];
PRINT '  ✓ Broker details cleared (Brokers table preserved)';
PRINT '';

PRINT 'Preserving core reference data (EmployerGroups, Products, Plans, Brokers, Schedules)...';
PRINT '  ✓ EmployerGroups, Products, Plans, Brokers, Schedules, FeeSchedules preserved';
PRINT '';

PRINT '✅ All production data cleared';
PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
PRINT 'Now run the export scripts in order (skipping preserved tables):';
PRINT '  ✓ 01-export-schedules.sql (SKIP - Schedules preserved)';
PRINT '  ✓ 02-export-brokers.sql (SKIP - Brokers preserved)';
PRINT '  ✓ 05-export-groups.sql (SKIP - EmployerGroups preserved)';
PRINT '  ✓ 06-export-products.sql (SKIP - Products preserved)';
PRINT '  ✓ 06a-export-plans.sql (SKIP - Plans preserved)';
PRINT '  🔄 07-export-proposals.sql (RUN - Proposals affected by EffectiveDateFrom changes)';
PRINT '  🔄 08-export-hierarchies.sql (RUN - Hierarchies completely regenerated)';
PRINT '  🔄 09-export-policies.sql (RUN - Policies reference new hierarchies)';
PRINT '  🔄 11-export-splits.sql (RUN - Premium splits reference new hierarchies)';
PRINT '  🔄 13-export-commission-assignments.sql (RUN - Assignments may reference hierarchies)';
PRINT '  🔄 13-export-licenses.sql (RUN - Broker details)';
PRINT '  🔄 14-export-policy-hierarchy-assignments.sql (RUN - PHA reference new hierarchies)';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '';

GO
