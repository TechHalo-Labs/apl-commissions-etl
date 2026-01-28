-- =============================================================================
-- Copy Schedule Data from poc_etl to etl schema
-- Fixes Issue 4: NULL ScheduleId root cause
-- =============================================================================
-- The transform script 04-schedules.sql expects data in etl.raw_schedule_rates
-- But schedule data lives in poc_etl.raw_schedule_rates
-- This script copies it to the correct location
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'COPY SCHEDULES: poc_etl → etl';
PRINT '============================================================';
PRINT '';

-- Verify source data exists
DECLARE @source_count BIGINT = (SELECT COUNT(*) FROM [poc_etl].[raw_schedule_rates]);
PRINT 'Source data (poc_etl.raw_schedule_rates): ' + FORMAT(@source_count, 'N0') + ' rows';

IF @source_count = 0
BEGIN
    PRINT '❌ ERROR: No source data found in poc_etl.raw_schedule_rates';
    RETURN;
END

-- Clear target table
PRINT '';
PRINT 'Clearing etl.raw_schedule_rates...';
TRUNCATE TABLE [etl].[raw_schedule_rates];

-- Copy data
PRINT 'Copying schedule rates...';

DECLARE @start_time DATETIME2 = GETUTCDATE();

INSERT INTO [etl].[raw_schedule_rates]
SELECT * 
FROM [poc_etl].[raw_schedule_rates];

DECLARE @copied INT = @@ROWCOUNT;
DECLARE @duration_sec INT = DATEDIFF(SECOND, @start_time, GETUTCDATE());

PRINT '✅ Copied: ' + FORMAT(@copied, 'N0') + ' rows in ' + CAST(@duration_sec AS VARCHAR) + ' seconds';

-- Verification
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 
    '[poc_etl].[raw_schedule_rates]' AS source_table,
    COUNT(*) AS row_count,
    COUNT(DISTINCT ScheduleName) AS unique_schedules
FROM [poc_etl].[raw_schedule_rates];

SELECT 
    '[etl].[raw_schedule_rates]' AS target_table,
    COUNT(*) AS row_count,
    COUNT(DISTINCT ScheduleName) AS unique_schedules
FROM [etl].[raw_schedule_rates];

-- Sample data check
SELECT TOP 5
    ScheduleName,
    ProductCode,
    [State],
    GroupSize,
    Year1,
    Year2,
    [Level]
FROM [etl].[raw_schedule_rates]
WHERE ScheduleName IS NOT NULL
ORDER BY ScheduleName;

PRINT '';
PRINT '============================================================';
PRINT 'SCHEDULE COPY COMPLETED';
PRINT '============================================================';
PRINT '';
PRINT 'Next steps:';
PRINT '  1. Run 04-schedules.sql to transform schedules to staging';
PRINT '  2. Run 07-hierarchies.sql to link ScheduleId to hierarchy participants';
PRINT '  3. Verify ScheduleId population: SELECT COUNT(*) FROM stg_hierarchy_participants WHERE ScheduleId IS NOT NULL';

GO
