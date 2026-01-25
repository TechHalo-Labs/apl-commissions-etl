-- =============================================================================
-- Export: Special Schedule Rates (Heaped Schedule Year Rates)
-- =============================================================================
-- Exports stg_special_schedule_rates to dbo.SpecialScheduleRates
-- Used for heaped schedules where commission rates vary by policy year
-- (e.g., Year 1 = 15%, Year 2 = 10%, Year 10 = 8%)
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'EXPORT: Special Schedule Rates (Heaped Year Rates)';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Check if staging table exists
-- =============================================================================
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'stg_special_schedule_rates'
)
BEGIN
    PRINT 'SKIPPED: stg_special_schedule_rates table does not exist';
    PRINT 'Run 01-add-missing-staging-tables.sql first, then populate staging';
    PRINT '============================================================';
    RETURN;
END

-- =============================================================================
-- Step 2: Check current state
-- =============================================================================
DECLARE @before_count INT;
SELECT @before_count = COUNT(*) FROM [dbo].[SpecialScheduleRates];
PRINT 'SpecialScheduleRates before export: ' + CAST(@before_count AS VARCHAR);

DECLARE @staging_count INT;
SELECT @staging_count = COUNT(*) FROM [etl].[stg_special_schedule_rates];
PRINT 'SpecialScheduleRates in staging: ' + CAST(@staging_count AS VARCHAR);

IF @staging_count = 0
BEGIN
    PRINT '';
    PRINT 'INFO: No records in staging table. Nothing to export.';
    PRINT 'If heaped schedules exist, populate stg_special_schedule_rates first.';
    PRINT '============================================================';
    RETURN;
END

-- =============================================================================
-- Step 3: Export special schedule rates
-- Only export for schedule rates that exist in production
-- =============================================================================
PRINT '';
PRINT 'Step 3: Exporting special schedule rates...';

SET IDENTITY_INSERT [dbo].[SpecialScheduleRates] ON;

INSERT INTO [dbo].[SpecialScheduleRates] (
    Id,
    ScheduleRateId,
    [Year],
    Rate,
    CreationTime,
    IsDeleted
)
SELECT
    ssr.Id,
    ssr.ScheduleRateId,
    ssr.[Year],
    ssr.Rate,
    COALESCE(ssr.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(ssr.IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_special_schedule_rates] ssr
WHERE 
    -- ScheduleRate must exist in production
    EXISTS (SELECT 1 FROM [dbo].[ScheduleRates] sr WHERE sr.Id = ssr.ScheduleRateId)
    -- Don't create duplicates
    AND NOT EXISTS (
        SELECT 1 FROM [dbo].[SpecialScheduleRates] existing 
        WHERE existing.ScheduleRateId = ssr.ScheduleRateId
          AND existing.[Year] = ssr.[Year]
    );

SET IDENTITY_INSERT [dbo].[SpecialScheduleRates] OFF;

DECLARE @exported INT = @@ROWCOUNT;
PRINT 'New SpecialScheduleRates exported: ' + CAST(@exported AS VARCHAR);

-- =============================================================================
-- Step 4: Report skipped records
-- =============================================================================
PRINT '';
PRINT 'Step 4: Reporting skipped records...';

-- Report staging records that couldn't be exported due to missing schedule rate
DECLARE @no_rate_count INT;
SELECT @no_rate_count = COUNT(*)
FROM [etl].[stg_special_schedule_rates] ssr
WHERE NOT EXISTS (SELECT 1 FROM [dbo].[ScheduleRates] sr WHERE sr.Id = ssr.ScheduleRateId);

IF @no_rate_count > 0
BEGIN
    PRINT 'WARNING: ' + CAST(@no_rate_count AS VARCHAR) + ' staging records skipped (ScheduleRateId not in production)';
    
    -- Show sample of skipped records
    SELECT TOP 10 
        ssr.Id,
        ssr.ScheduleRateId,
        ssr.[Year],
        ssr.Rate,
        'ScheduleRateId not in dbo.ScheduleRates' AS Reason
    FROM [etl].[stg_special_schedule_rates] ssr
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[ScheduleRates] sr WHERE sr.Id = ssr.ScheduleRateId)
    ORDER BY ssr.ScheduleRateId, ssr.[Year];
END

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

DECLARE @after_count INT;
SELECT @after_count = COUNT(*) FROM [dbo].[SpecialScheduleRates];
PRINT 'SpecialScheduleRates after export: ' + CAST(@after_count AS VARCHAR);
PRINT 'Net new records: ' + CAST(@after_count - @before_count AS VARCHAR);

-- Breakdown by year
SELECT 'Production by Year' AS Metric, [Year], COUNT(*) AS Cnt, AVG(Rate) AS AvgRate
FROM [dbo].[SpecialScheduleRates]
GROUP BY [Year]
ORDER BY [Year];

PRINT '';
PRINT '============================================================';
PRINT 'SPECIAL SCHEDULE RATES EXPORT COMPLETED';
PRINT '============================================================';

GO
