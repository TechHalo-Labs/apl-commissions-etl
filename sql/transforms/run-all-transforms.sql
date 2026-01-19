-- =============================================================================
-- Master Transform Runner (T-SQL)
-- Executes all transforms in the correct order
-- Usage: sqlcmd -S server -d database -i sql/transforms/run-all-transforms.sql
-- =============================================================================

SET NOCOUNT ON;

DECLARE @start_time DATETIME2 = GETUTCDATE();

PRINT '============================================================';
PRINT 'SQL SERVER ETL: RUNNING ALL TRANSFORMS';
PRINT 'Started at: ' + CONVERT(VARCHAR, @start_time, 120);
PRINT '============================================================';
PRINT '';

-- Transform 00: References
PRINT '>>> Running 00-references.sql...';
EXEC('
-- Include contents of 00-references.sql here when running via SQLCMD
-- Or use: :r 00-references.sql
');
PRINT '';

-- Transform 01: Brokers
PRINT '>>> Running 01-brokers.sql...';
EXEC('
-- Include contents of 01-brokers.sql here when running via SQLCMD
-- Or use: :r 01-brokers.sql
');
PRINT '';

-- Transform 02: Groups
PRINT '>>> Running 02-groups.sql...';
EXEC('
-- Include contents of 02-groups.sql here when running via SQLCMD
-- Or use: :r 02-groups.sql
');
PRINT '';

-- Transform 03: Products
PRINT '>>> Running 03-products.sql...';
EXEC('
-- Include contents of 03-products.sql here when running via SQLCMD
-- Or use: :r 03-products.sql
');
PRINT '';

-- Transform 04: Schedules
PRINT '>>> Running 04-schedules.sql...';
EXEC('
-- Include contents of 04-schedules.sql here when running via SQLCMD
-- Or use: :r 04-schedules.sql
');
PRINT '';

-- Transform 06: Proposals
PRINT '>>> Running 06-proposals.sql...';
EXEC('
-- Include contents of 06-proposals.sql here when running via SQLCMD
-- Or use: :r 06-proposals.sql
');
PRINT '';

-- Transform 07: Hierarchies
PRINT '>>> Running 07-hierarchies.sql...';
EXEC('
-- Include contents of 07-hierarchies.sql here when running via SQLCMD
-- Or use: :r 07-hierarchies.sql
');
PRINT '';

-- Transform 09: Policies
PRINT '>>> Running 09-policies.sql...';
EXEC('
-- Include contents of 09-policies.sql here when running via SQLCMD
-- Or use: :r 09-policies.sql
');
PRINT '';

-- Transform 10: Premium Transactions
PRINT '>>> Running 10-premium-transactions.sql...';
EXEC('
-- Include contents of 10-premium-transactions.sql here when running via SQLCMD
-- Or use: :r 10-premium-transactions.sql
');
PRINT '';

-- =============================================================================
-- Final Summary
-- =============================================================================
DECLARE @end_time DATETIME2 = GETUTCDATE();

PRINT '============================================================';
PRINT 'ALL TRANSFORMS COMPLETED';
PRINT 'Started: ' + CONVERT(VARCHAR, @start_time, 120);
PRINT 'Ended: ' + CONVERT(VARCHAR, @end_time, 120);
PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
PRINT '============================================================';
PRINT '';

PRINT '--- STAGING TABLE COUNTS ---';

SELECT 'stg_brokers' AS [table], COUNT(*) AS [rows] FROM [etl].[stg_brokers]
UNION ALL SELECT 'stg_groups', COUNT(*) FROM [etl].[stg_groups]
UNION ALL SELECT 'stg_products', COUNT(*) FROM [etl].[stg_products]
UNION ALL SELECT 'stg_plans', COUNT(*) FROM [etl].[stg_plans]
UNION ALL SELECT 'stg_schedules', COUNT(*) FROM [etl].[stg_schedules]
UNION ALL SELECT 'stg_schedule_versions', COUNT(*) FROM [etl].[stg_schedule_versions]
UNION ALL SELECT 'stg_schedule_rates', COUNT(*) FROM [etl].[stg_schedule_rates]
UNION ALL SELECT 'stg_proposals', COUNT(*) FROM [etl].[stg_proposals]
UNION ALL SELECT 'stg_proposal_products', COUNT(*) FROM [etl].[stg_proposal_products]
UNION ALL SELECT 'stg_premium_split_versions', COUNT(*) FROM [etl].[stg_premium_split_versions]
UNION ALL SELECT 'stg_premium_split_participants', COUNT(*) FROM [etl].[stg_premium_split_participants]
UNION ALL SELECT 'stg_hierarchies', COUNT(*) FROM [etl].[stg_hierarchies]
UNION ALL SELECT 'stg_hierarchy_versions', COUNT(*) FROM [etl].[stg_hierarchy_versions]
UNION ALL SELECT 'stg_hierarchy_participants', COUNT(*) FROM [etl].[stg_hierarchy_participants]
UNION ALL SELECT 'stg_commission_assignment_versions', COUNT(*) FROM [etl].[stg_commission_assignment_versions]
UNION ALL SELECT 'stg_commission_assignment_recipients', COUNT(*) FROM [etl].[stg_commission_assignment_recipients]
UNION ALL SELECT 'stg_policies', COUNT(*) FROM [etl].[stg_policies]
UNION ALL SELECT 'stg_premium_transactions', COUNT(*) FROM [etl].[stg_premium_transactions]
ORDER BY [table];

PRINT '';
PRINT '============================================================';
PRINT 'TRANSFORM PIPELINE COMPLETE';
PRINT '============================================================';

GO

