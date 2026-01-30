-- =============================================================================
-- Restore ETL Staging Tables from new_data Backups
-- Fast restoration of previously successful ETL state
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'RESTORE: Staging tables from new_data backups';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Restore stg_brokers
-- =============================================================================
PRINT 'Step 1: Restoring stg_brokers...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_brokers];

INSERT INTO [$(ETL_SCHEMA)].[stg_brokers]
SELECT * FROM [new_data].[stg_brokers_backup];

DECLARE @broker_count INT = @@ROWCOUNT;
PRINT '  ✅ Restored: ' + FORMAT(@broker_count, 'N0') + ' brokers';

-- =============================================================================
-- Step 2: Restore stg_groups
-- =============================================================================
PRINT '';
PRINT 'Step 2: Restoring stg_groups...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_groups];

INSERT INTO [$(ETL_SCHEMA)].[stg_groups]
SELECT * FROM [new_data].[stg_groups_backup];

DECLARE @group_count INT = @@ROWCOUNT;
PRINT '  ✅ Restored: ' + FORMAT(@group_count, 'N0') + ' groups';

-- =============================================================================
-- Step 3: Restore stg_proposals
-- =============================================================================
PRINT '';
PRINT 'Step 3: Restoring stg_proposals...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_proposals];

INSERT INTO [$(ETL_SCHEMA)].[stg_proposals]
SELECT * FROM [new_data].[stg_proposals_backup];

DECLARE @proposal_count INT = @@ROWCOUNT;
PRINT '  ✅ Restored: ' + FORMAT(@proposal_count, 'N0') + ' proposals';

-- =============================================================================
-- Step 4: Restore stg_proposal_products
-- =============================================================================
PRINT '';
PRINT 'Step 4: Restoring stg_proposal_products...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_proposal_products];

INSERT INTO [$(ETL_SCHEMA)].[stg_proposal_products]
SELECT * FROM [new_data].[stg_proposal_products_backup];

DECLARE @pp_count INT = @@ROWCOUNT;
PRINT '  ✅ Restored: ' + FORMAT(@pp_count, 'N0') + ' proposal products';

-- =============================================================================
-- Step 5: Restore stg_hierarchies
-- =============================================================================
PRINT '';
PRINT 'Step 5: Restoring stg_hierarchies...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_hierarchies];

INSERT INTO [$(ETL_SCHEMA)].[stg_hierarchies]
SELECT * FROM [new_data].[stg_hierarchies_backup];

DECLARE @hier_count INT = @@ROWCOUNT;
PRINT '  ✅ Restored: ' + FORMAT(@hier_count, 'N0') + ' hierarchies';

-- =============================================================================
-- Step 6: Restore stg_hierarchy_versions
-- =============================================================================
PRINT '';
PRINT 'Step 6: Restoring stg_hierarchy_versions...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_hierarchy_versions];

INSERT INTO [$(ETL_SCHEMA)].[stg_hierarchy_versions]
SELECT * FROM [new_data].[stg_hierarchy_versions_backup];

DECLARE @hv_count INT = @@ROWCOUNT;
PRINT '  ✅ Restored: ' + FORMAT(@hv_count, 'N0') + ' hierarchy versions';

-- =============================================================================
-- Step 7: Restore stg_hierarchy_participants
-- =============================================================================
PRINT '';
PRINT 'Step 7: Restoring stg_hierarchy_participants...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_hierarchy_participants];

INSERT INTO [$(ETL_SCHEMA)].[stg_hierarchy_participants]
SELECT * FROM [new_data].[stg_hierarchy_participants_backup];

DECLARE @hp_count INT = @@ROWCOUNT;
PRINT '  ✅ Restored: ' + FORMAT(@hp_count, 'N0') + ' hierarchy participants';

-- =============================================================================
-- Step 8: Restore stg_premium_split_versions
-- =============================================================================
PRINT '';
PRINT 'Step 8: Restoring stg_premium_split_versions...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_premium_split_versions];

INSERT INTO [$(ETL_SCHEMA)].[stg_premium_split_versions]
SELECT * FROM [new_data].[stg_premium_split_versions_backup];

DECLARE @psv_count INT = @@ROWCOUNT;
PRINT '  ✅ Restored: ' + FORMAT(@psv_count, 'N0') + ' premium split versions';

-- =============================================================================
-- Step 9: Restore stg_premium_split_participants
-- =============================================================================
PRINT '';
PRINT 'Step 9: Restoring stg_premium_split_participants...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_premium_split_participants];

INSERT INTO [$(ETL_SCHEMA)].[stg_premium_split_participants]
SELECT * FROM [new_data].[stg_premium_split_participants_backup];

DECLARE @psp_count INT = @@ROWCOUNT;
PRINT '  ✅ Restored: ' + FORMAT(@psp_count, 'N0') + ' premium split participants';

-- =============================================================================
-- Step 10: Restore stg_policies
-- =============================================================================
PRINT '';
PRINT 'Step 10: Restoring stg_policies...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_policies];

INSERT INTO [$(ETL_SCHEMA)].[stg_policies]
SELECT * FROM [new_data].[stg_policies_backup];

DECLARE @policy_count INT = @@ROWCOUNT;
PRINT '  ✅ Restored: ' + FORMAT(@policy_count, 'N0') + ' policies';

-- =============================================================================
-- Step 11: Restore stg_policy_hierarchy_assignments
-- =============================================================================
PRINT '';
PRINT 'Step 11: Restoring stg_policy_hierarchy_assignments...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments];

INSERT INTO [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments]
SELECT * FROM [new_data].[stg_policy_hierarchy_assignments_backup];

DECLARE @pha_count INT = @@ROWCOUNT;
PRINT '  ✅ Restored: ' + FORMAT(@pha_count, 'N0') + ' PHA assignments';

-- =============================================================================
-- Step 12: Restore stg_policy_hierarchy_participants
-- =============================================================================
PRINT '';
PRINT 'Step 12: Restoring stg_policy_hierarchy_participants...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_policy_hierarchy_participants];

INSERT INTO [$(ETL_SCHEMA)].[stg_policy_hierarchy_participants]
SELECT * FROM [new_data].[stg_policy_hierarchy_participants_backup];

DECLARE @php_count INT = @@ROWCOUNT;
PRINT '  ✅ Restored: ' + FORMAT(@php_count, 'N0') + ' PHA participants';

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

PRINT '';
PRINT 'Staging tables restored:';
SELECT 'stg_brokers' as tbl, COUNT(*) as cnt FROM [$(ETL_SCHEMA)].[stg_brokers]
UNION ALL SELECT 'stg_groups', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_groups]
UNION ALL SELECT 'stg_proposals', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_proposals]
UNION ALL SELECT 'stg_proposal_products', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_proposal_products]
UNION ALL SELECT 'stg_hierarchies', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_hierarchies]
UNION ALL SELECT 'stg_hierarchy_versions', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_hierarchy_versions]
UNION ALL SELECT 'stg_hierarchy_participants', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_hierarchy_participants]
UNION ALL SELECT 'stg_premium_split_versions', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_premium_split_versions]
UNION ALL SELECT 'stg_premium_split_participants', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_premium_split_participants]
UNION ALL SELECT 'stg_policies', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_policies]
UNION ALL SELECT 'stg_policy_hierarchy_assignments', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments]
UNION ALL SELECT 'stg_policy_hierarchy_participants', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_participants]
ORDER BY 1;

PRINT '';
PRINT '============================================================';
PRINT 'RESTORATION COMPLETED';
PRINT '============================================================';
PRINT '';
PRINT '✅ ALL staging tables restored from backups';
PRINT '';
PRINT 'Note: Schedule rates were already ingested separately (1,133,420 records)';
PRINT '';
PRINT 'Next: Verify data with SELECT queries or run export to production';
PRINT '';

GO
