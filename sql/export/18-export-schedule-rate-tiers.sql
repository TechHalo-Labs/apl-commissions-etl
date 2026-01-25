-- =============================================================================
-- Export: Schedule Rate Tiers (Group-Size Tiered Rates)
-- =============================================================================
-- Exports stg_schedule_rate_tiers to dbo.ScheduleRateTiers
-- Used for volume-based rate tiers where commission rates vary by group size
-- (e.g., 1-50 employees = 12%, 51-100 = 10%, 100+ = 8%)
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'EXPORT: Schedule Rate Tiers (Group-Size Tiered Rates)';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Check if staging table exists
-- =============================================================================
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'stg_schedule_rate_tiers'
)
BEGIN
    PRINT 'SKIPPED: stg_schedule_rate_tiers table does not exist';
    PRINT 'Run 01-add-missing-staging-tables.sql first, then populate staging';
    PRINT '============================================================';
    RETURN;
END

-- =============================================================================
-- Step 2: Check current state
-- =============================================================================
DECLARE @before_count INT;
SELECT @before_count = COUNT(*) FROM [dbo].[ScheduleRateTiers];
PRINT 'ScheduleRateTiers before export: ' + CAST(@before_count AS VARCHAR);

DECLARE @staging_count INT;
SELECT @staging_count = COUNT(*) FROM [etl].[stg_schedule_rate_tiers];
PRINT 'ScheduleRateTiers in staging: ' + CAST(@staging_count AS VARCHAR);

IF @staging_count = 0
BEGIN
    PRINT '';
    PRINT 'INFO: No records in staging table. Nothing to export.';
    PRINT 'If tiered rates exist, populate stg_schedule_rate_tiers first.';
    PRINT '============================================================';
    RETURN;
END

-- =============================================================================
-- Step 3: Export schedule rate tiers
-- Only export for schedule rates that exist in production
-- =============================================================================
PRINT '';
PRINT 'Step 3: Exporting schedule rate tiers...';

SET IDENTITY_INSERT [dbo].[ScheduleRateTiers] ON;

INSERT INTO [dbo].[ScheduleRateTiers] (
    Id,
    ScheduleRateId,
    MinVolume,
    MaxVolume,
    Rate,
    FirstYearRate,
    RenewalRate,
    CreationTime,
    IsDeleted
)
SELECT
    srt.Id,
    srt.ScheduleRateId,
    srt.MinVolume,
    srt.MaxVolume,
    srt.Rate,
    srt.FirstYearRate,
    srt.RenewalRate,
    COALESCE(srt.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(srt.IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_schedule_rate_tiers] srt
WHERE 
    -- ScheduleRate must exist in production
    EXISTS (SELECT 1 FROM [dbo].[ScheduleRates] sr WHERE sr.Id = srt.ScheduleRateId)
    -- Don't create duplicates
    AND srt.Id NOT IN (SELECT Id FROM [dbo].[ScheduleRateTiers]);

SET IDENTITY_INSERT [dbo].[ScheduleRateTiers] OFF;

DECLARE @exported INT = @@ROWCOUNT;
PRINT 'New ScheduleRateTiers exported: ' + CAST(@exported AS VARCHAR);

-- =============================================================================
-- Step 4: Report skipped records
-- =============================================================================
PRINT '';
PRINT 'Step 4: Reporting skipped records...';

-- Report staging records that couldn't be exported due to missing schedule rate
DECLARE @no_rate_count INT;
SELECT @no_rate_count = COUNT(*)
FROM [etl].[stg_schedule_rate_tiers] srt
WHERE NOT EXISTS (SELECT 1 FROM [dbo].[ScheduleRates] sr WHERE sr.Id = srt.ScheduleRateId);

IF @no_rate_count > 0
BEGIN
    PRINT 'WARNING: ' + CAST(@no_rate_count AS VARCHAR) + ' staging records skipped (ScheduleRateId not in production)';
    
    -- Show sample of skipped records
    SELECT TOP 10 
        srt.Id,
        srt.ScheduleRateId,
        srt.MinVolume,
        srt.MaxVolume,
        srt.Rate,
        'ScheduleRateId not in dbo.ScheduleRates' AS Reason
    FROM [etl].[stg_schedule_rate_tiers] srt
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[ScheduleRates] sr WHERE sr.Id = srt.ScheduleRateId)
    ORDER BY srt.ScheduleRateId, srt.MinVolume;
END

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

DECLARE @after_count INT;
SELECT @after_count = COUNT(*) FROM [dbo].[ScheduleRateTiers];
PRINT 'ScheduleRateTiers after export: ' + CAST(@after_count AS VARCHAR);
PRINT 'Net new records: ' + CAST(@after_count - @before_count AS VARCHAR);

-- Breakdown by volume range
SELECT 'Production by Volume Range' AS Metric,
    CASE 
        WHEN MaxVolume IS NULL THEN CONCAT(CAST(CAST(MinVolume AS INT) AS VARCHAR), '+')
        ELSE CONCAT(CAST(CAST(MinVolume AS INT) AS VARCHAR), '-', CAST(CAST(MaxVolume AS INT) AS VARCHAR))
    END AS VolumeRange,
    COUNT(*) AS Cnt,
    AVG(Rate) AS AvgRate
FROM [dbo].[ScheduleRateTiers]
GROUP BY 
    CASE 
        WHEN MaxVolume IS NULL THEN CONCAT(CAST(CAST(MinVolume AS INT) AS VARCHAR), '+')
        ELSE CONCAT(CAST(CAST(MinVolume AS INT) AS VARCHAR), '-', CAST(CAST(MaxVolume AS INT) AS VARCHAR))
    END
ORDER BY MIN(MinVolume);

PRINT '';
PRINT '============================================================';
PRINT 'SCHEDULE RATE TIERS EXPORT COMPLETED';
PRINT '============================================================';

GO
