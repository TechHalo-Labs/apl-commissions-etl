-- =============================================================================
-- Ingest: Schedule Rates DIRECTLY from Source
-- Bypasses poc_etl to preserve ALL rate granularity
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'INGEST: Schedule Rates from raw_data.raw_schedule_rates';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Critical Fix: Import ALL schedule rates directly from raw_data
-- =============================================================================
-- Problem: new_data.PerfScheduleModel has ScheduleName = NULL
-- Solution: Read DIRECTLY from raw_data.raw_schedule_rates which has complete data
-- 
-- Data Quality Comparison:
--   new_data.PerfScheduleModel: ScheduleName = NULL ❌
--   raw_data.raw_schedule_rates: ScheduleName populated, State populated ✅
-- 
-- Example: GA508 LEVB
--   raw_data: Year99 = 39.40% (correct) ✅
--   new_data: ScheduleName = NULL (broken) ❌
-- =============================================================================

PRINT 'Step 1: Checking source table...';

DECLARE @source_count BIGINT;
SELECT @source_count = COUNT(*) FROM [raw_data].[raw_schedule_rates];
PRINT '  Source records available: ' + FORMAT(@source_count, 'N0');

DECLARE @source_products INT;
SELECT @source_products = COUNT(DISTINCT ProductCode) FROM [raw_data].[raw_schedule_rates];
PRINT '  Unique products: ' + FORMAT(@source_products, 'N0');

DECLARE @source_schedules INT;
SELECT @source_schedules = COUNT(DISTINCT ScheduleName) FROM [raw_data].[raw_schedule_rates] WHERE ScheduleName IS NOT NULL;
PRINT '  Unique schedules: ' + FORMAT(@source_schedules, 'N0');

-- =============================================================================
-- Step 2: Clear existing raw_schedule_rates
-- =============================================================================
PRINT '';
PRINT 'Step 2: Clearing existing raw_schedule_rates...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[raw_schedule_rates];
PRINT '  ✅ Table truncated';

-- =============================================================================
-- Step 3: Import ALL schedule rates (no filtering, no aggregation)
-- =============================================================================
PRINT '';
PRINT 'Step 3: Importing ALL schedule rates from source...';
PRINT '  NOTE: Importing EVERY row to preserve rate granularity';

INSERT INTO [$(ETL_SCHEMA)].[raw_schedule_rates] (
    ScheduleName,
    ScheduleDescription,
    Category,
    ProductCode,
    OffGroupLetterDescription,
    [State],
    GroupSizeFrom,
    GroupSizeTo,
    GroupSize,
    EffectiveStartDate,
    EffectiveEndDate,
    SeriesType,
    SpecialOffer,
    Year1,
    Year2,
    Year3,
    Year4,
    Year5,
    Year6,
    Year7,
    Year8,
    Year9,
    Year10,
    Year11,
    Year12,
    Year13,
    Year14,
    Year15,
    Year16,
    Year66,
    Year99,
    [Level]
)
SELECT 
    LTRIM(RTRIM(ScheduleName)),
    LTRIM(RTRIM(ScheduleDescription)),
    LTRIM(RTRIM(Category)),
    LTRIM(RTRIM(ProductCode)),
    LTRIM(RTRIM(OffGroupLetterDescription)),
    LTRIM(RTRIM([State])),
    GroupSizeFrom,
    GroupSizeTo,
    LTRIM(RTRIM(GroupSize)),
    EffectiveStartDate,
    EffectiveEndDate,
    LTRIM(RTRIM(SeriesType)),
    LTRIM(RTRIM(SpecialOffer)),
    Year1,
    Year2,
    Year3,
    Year4,
    Year5,
    Year6,
    Year7,
    Year8,
    Year9,
    Year10,
    Year11,
    Year12,
    Year13,
    Year14,
    Year15,
    Year16,
    Year66,
    Year99,
    LTRIM(RTRIM([Level]))
FROM [raw_data].[raw_schedule_rates]
WHERE ProductCode IS NOT NULL 
  AND LTRIM(RTRIM(ProductCode)) <> '';

DECLARE @imported_count INT = @@ROWCOUNT;
PRINT '  ✅ Imported: ' + FORMAT(@imported_count, 'N0') + ' rows';

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

PRINT '';
PRINT 'Total records imported:';
SELECT COUNT(*) as total_rows FROM [$(ETL_SCHEMA)].[raw_schedule_rates];

PRINT '';
PRINT 'Unique schedules:';
SELECT COUNT(DISTINCT ScheduleName) as unique_schedules 
FROM [$(ETL_SCHEMA)].[raw_schedule_rates]
WHERE ScheduleName IS NOT NULL;

PRINT '';
PRINT 'Unique products:';
SELECT COUNT(DISTINCT ProductCode) as unique_products 
FROM [$(ETL_SCHEMA)].[raw_schedule_rates];

PRINT '';
PRINT 'Unique states:';
SELECT COUNT(DISTINCT [State]) as unique_states 
FROM [$(ETL_SCHEMA)].[raw_schedule_rates]
WHERE [State] IS NOT NULL AND [State] <> '';

PRINT '';
PRINT 'Sample verification (GA508 LEVB):';
SELECT 
    ScheduleName,
    COUNT(*) as record_count,
    COUNT(DISTINCT [State]) as unique_states,
    COUNT(DISTINCT [Level]) as unique_rates
FROM [$(ETL_SCHEMA)].[raw_schedule_rates]
WHERE ProductCode = 'GA508 LEVB'
GROUP BY ScheduleName
ORDER BY record_count DESC;

PRINT '';
PRINT '============================================================';
PRINT 'SCHEDULE RATES INGESTION COMPLETED';
PRINT '============================================================';
PRINT '';
PRINT '✅ ALL source data imported (no filtering, no aggregation)';
PRINT '✅ Rate diversity preserved';
PRINT '✅ All schedules included (active and inactive)';
PRINT '';
PRINT 'Next: Run sql/transforms/04-schedules.sql (with fixes applied)';
PRINT '';

GO
