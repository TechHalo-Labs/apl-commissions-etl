-- Quick table existence check
SET NOCOUNT ON;

PRINT 'Checking if work tables exist...';
PRINT '';

-- Check work tables
IF OBJECT_ID('[etl].[work_split_participants]', 'U') IS NOT NULL
    PRINT '✓ work_split_participants exists';
ELSE
    PRINT '✗ work_split_participants MISSING - Run 03-staging-tables.sql first!';

IF OBJECT_ID('[etl].[work_split_signatures]', 'U') IS NOT NULL
    PRINT '✓ work_split_signatures exists';
ELSE
    PRINT '✗ work_split_signatures MISSING - Run 03-staging-tables.sql first!';

IF OBJECT_ID('[etl].[work_hierarchy_id_map]', 'U') IS NOT NULL
    PRINT '✓ work_hierarchy_id_map exists';
ELSE
    PRINT '✗ work_hierarchy_id_map MISSING - Run 03-staging-tables.sql first!';

IF OBJECT_ID('[etl].[work_splitseq_to_hierarchy]', 'U') IS NOT NULL
    PRINT '✓ work_splitseq_to_hierarchy exists';
ELSE
    PRINT '✗ work_splitseq_to_hierarchy MISSING - Run 03-staging-tables.sql first!';

IF OBJECT_ID('[etl].[work_hierarchy_data]', 'U') IS NOT NULL
    PRINT '✓ work_hierarchy_data exists';
ELSE
    PRINT '✗ work_hierarchy_data MISSING - Run 03-staging-tables.sql first!';

PRINT '';
PRINT 'Checking if staging tables exist...';
PRINT '';

IF OBJECT_ID('[etl].[stg_brokers]', 'U') IS NOT NULL
    PRINT '✓ stg_brokers exists';
ELSE
    PRINT '✗ stg_brokers MISSING - Run 03-staging-tables.sql first!';

IF OBJECT_ID('[etl].[stg_groups]', 'U') IS NOT NULL
    PRINT '✓ stg_groups exists';
ELSE
    PRINT '✗ stg_groups MISSING - Run 03-staging-tables.sql first!';

IF OBJECT_ID('[etl].[stg_schedules]', 'U') IS NOT NULL
    PRINT '✓ stg_schedules exists';
ELSE
    PRINT '✗ stg_schedules MISSING - Run 03-staging-tables.sql first!';

IF OBJECT_ID('[etl].[input_certificate_info]', 'U') IS NOT NULL
    PRINT '✓ input_certificate_info exists';
ELSE
    PRINT '✗ input_certificate_info MISSING - Run ingest phase first!';

PRINT '';
PRINT 'Table check complete.';
