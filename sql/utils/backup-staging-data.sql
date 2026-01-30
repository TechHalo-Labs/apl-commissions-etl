-- =====================================================
-- Backup Staging Data to Timestamped Schema
-- Usage: sqlcmd -v BACKUP_SCHEMA=backup281928
-- =====================================================

SET NOCOUNT ON;

DECLARE @backupSchema NVARCHAR(128) = '$(BACKUP_SCHEMA)';

PRINT '═════════════════════════════════════════════════════════════════';
PRINT 'BACKUP STAGING DATA';
PRINT '═════════════════════════════════════════════════════════════════';
PRINT 'Backup Schema: ' + @backupSchema;
PRINT 'Source Schema: etl';
PRINT '';

-- =====================================================
-- Step 1: Create backup schema
-- =====================================================
PRINT 'Step 1: Creating backup schema...';

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = @backupSchema)
BEGIN
    DECLARE @createSchemaSql NVARCHAR(MAX) = N'CREATE SCHEMA ' + QUOTENAME(@backupSchema);
    EXEC sp_executesql @createSchemaSql;
    PRINT '  ✓ Schema created: ' + @backupSchema;
END
ELSE
BEGIN
    PRINT '  ⚠ Schema already exists: ' + @backupSchema;
END
GO

-- =====================================================
-- Step 2: Backup staging tables
-- =====================================================
PRINT '';
PRINT 'Step 2: Backing up staging tables...';

-- Brokers
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_brokers] FROM [etl].[stg_brokers];
DECLARE @brokerCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_brokers: ' + CAST(@brokerCount AS VARCHAR) + ' rows';

-- Groups
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_groups] FROM [etl].[stg_groups];
DECLARE @groupCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_groups: ' + CAST(@groupCount AS VARCHAR) + ' rows';

-- Products
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_products] FROM [etl].[stg_products];
DECLARE @productCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_products: ' + CAST(@productCount AS VARCHAR) + ' rows';

-- Schedules
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_schedules] FROM [etl].[stg_schedules];
DECLARE @scheduleCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_schedules: ' + CAST(@scheduleCount AS VARCHAR) + ' rows';

-- Schedule Rates
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_schedule_rates] FROM [etl].[stg_schedule_rates];
DECLARE @rateCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_schedule_rates: ' + CAST(@rateCount AS VARCHAR) + ' rows';

-- Schedule Versions
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_schedule_versions] FROM [etl].[stg_schedule_versions];
DECLARE @versionCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_schedule_versions: ' + CAST(@versionCount AS VARCHAR) + ' rows';

-- Proposals
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_proposals] FROM [etl].[stg_proposals];
DECLARE @proposalCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_proposals: ' + CAST(@proposalCount AS VARCHAR) + ' rows';

-- Premium Split Versions
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_premium_split_versions] FROM [etl].[stg_premium_split_versions];
DECLARE @splitVersionCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_premium_split_versions: ' + CAST(@splitVersionCount AS VARCHAR) + ' rows';

-- Premium Split Participants
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_premium_split_participants] FROM [etl].[stg_premium_split_participants];
DECLARE @splitPartCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_premium_split_participants: ' + CAST(@splitPartCount AS VARCHAR) + ' rows';

-- Proposal Key Mapping
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_proposal_key_mapping] FROM [etl].[stg_proposal_key_mapping];
DECLARE @keyMapCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_proposal_key_mapping: ' + CAST(@keyMapCount AS VARCHAR) + ' rows';

-- Hierarchies
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_hierarchies] FROM [etl].[stg_hierarchies];
DECLARE @hierarchyCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_hierarchies: ' + CAST(@hierarchyCount AS VARCHAR) + ' rows';

-- Hierarchy Versions
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_hierarchy_versions] FROM [etl].[stg_hierarchy_versions];
DECLARE @hierVersionCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_hierarchy_versions: ' + CAST(@hierVersionCount AS VARCHAR) + ' rows';

-- Hierarchy Participants
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_hierarchy_participants] FROM [etl].[stg_hierarchy_participants];
DECLARE @hierPartCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_hierarchy_participants: ' + CAST(@hierPartCount AS VARCHAR) + ' rows';

-- Policies
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_policies] FROM [etl].[stg_policies];
DECLARE @policyCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_policies: ' + CAST(@policyCount AS VARCHAR) + ' rows';

-- Premium Transactions
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_premium_transactions] FROM [etl].[stg_premium_transactions];
DECLARE @premiumCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_premium_transactions: ' + CAST(@premiumCount AS VARCHAR) + ' rows';

-- Policy Hierarchy Assignments
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_policy_hierarchy_assignments] FROM [etl].[stg_policy_hierarchy_assignments];
DECLARE @phaCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_policy_hierarchy_assignments: ' + CAST(@phaCount AS VARCHAR) + ' rows';

-- Policy Hierarchy Participants
SELECT * INTO [$(BACKUP_SCHEMA)].[stg_policy_hierarchy_participants] FROM [etl].[stg_policy_hierarchy_participants];
DECLARE @phpCount INT = @@ROWCOUNT;
PRINT '  ✓ stg_policy_hierarchy_participants: ' + CAST(@phpCount AS VARCHAR) + ' rows';

GO

-- =====================================================
-- Step 3: Verify backup
-- =====================================================
PRINT '';
PRINT 'Step 3: Verifying backup...';
PRINT '';

SELECT 
    TABLE_SCHEMA as [Schema],
    TABLE_NAME as [Table],
    (SELECT COUNT(*) FROM [$(BACKUP_SCHEMA)].[stg_brokers] WHERE TABLE_NAME = 'stg_brokers') as [Rows]
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = '$(BACKUP_SCHEMA)'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;

GO

PRINT '';
PRINT '═════════════════════════════════════════════════════════════════';
PRINT '✓ BACKUP COMPLETE';
PRINT '═════════════════════════════════════════════════════════════════';
PRINT '';
PRINT 'To restore: SELECT * INTO etl.stg_table FROM [$(BACKUP_SCHEMA)].stg_table';
PRINT '';
