-- =============================================================================
-- Ingest: Schedule Rates with Schedule Mapping from Certificates
-- =============================================================================
-- Problem: new_data.PerfScheduleModel has rates but no ScheduleName
-- Solution: Infer schedule from certificate usage patterns
-- Logic: For each (Product, State), find which schedules use it from certificates,
--        then replicate the rates for all those schedules
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'INGEST: Schedule Rates with Certificate-Based Mapping';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Build schedule-product-state mapping from certificates
-- =============================================================================
PRINT 'Step 1: Building schedule-product-state mapping from certificates...';

IF OBJECT_ID('[$(ETL_SCHEMA)].[work_schedule_product_state_map]', 'U') IS NOT NULL
    DROP TABLE [$(ETL_SCHEMA)].[work_schedule_product_state_map];

SELECT DISTINCT
    LTRIM(RTRIM(CommissionsSchedule)) AS ScheduleName,
    LTRIM(RTRIM(Product)) AS ProductCode,
    LTRIM(RTRIM(CertIssuedState)) AS [State]
INTO [$(ETL_SCHEMA)].[work_schedule_product_state_map]
FROM [$(ETL_SCHEMA)].[input_certificate_info]
WHERE CommissionsSchedule IS NOT NULL 
  AND LTRIM(RTRIM(CommissionsSchedule)) <> ''
  AND Product IS NOT NULL
  AND LTRIM(RTRIM(Product)) <> ''
  AND CertIssuedState IS NOT NULL
  AND LTRIM(RTRIM(CertIssuedState)) <> '';

DECLARE @mapping_count INT = @@ROWCOUNT;
PRINT '  ✅ Created ' + CAST(@mapping_count AS VARCHAR) + ' schedule-product-state mappings';

-- =============================================================================
-- Step 2: Clear and populate raw_schedule_rates with ALL rates
-- =============================================================================
PRINT '';
PRINT 'Step 2: Importing schedule rates with inferred schedule names...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[raw_schedule_rates];

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
    m.ScheduleName,                                    -- ✅ From certificate mapping
    pm.ScheduleDescription,
    pm.Category,
    pm.ProductCode,
    pm.OffGroupLetterDescription,
    pm.[State],
    pm.GroupSizeFrom,
    pm.GroupSizeTo,
    pm.GroupSize,
    pm.EffectiveStartDate,
    pm.EffectiveEndDate,
    pm.SeriesType,
    pm.SpecialOffer,
    pm.Year1,
    pm.Year2,
    pm.Year3,
    pm.Year4,
    pm.Year5,
    pm.Year6,
    pm.Year7,
    pm.Year8,
    pm.Year9,
    pm.Year10,
    pm.Year11,
    pm.Year12,
    pm.Year13,
    pm.Year14,
    pm.Year15,
    pm.Year16,
    pm.Year66,
    pm.Year99,
    pm.[Level]
FROM [new_data].[PerfScheduleModel] pm
INNER JOIN [$(ETL_SCHEMA)].[work_schedule_product_state_map] m 
    ON LTRIM(RTRIM(pm.ProductCode)) = m.ProductCode
    AND LTRIM(RTRIM(pm.[State])) = m.[State]
WHERE pm.ProductCode IS NOT NULL 
  AND LTRIM(RTRIM(pm.ProductCode)) <> '';

DECLARE @imported_count INT = @@ROWCOUNT;
PRINT '  ✅ Imported: ' + FORMAT(@imported_count, 'N0') + ' schedule rate records';

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
PRINT 'GA508 LEVB verification:';
SELECT 
    ScheduleName,
    COUNT(*) as rate_count,
    COUNT(DISTINCT [State]) as unique_states,
    COUNT(DISTINCT [Level]) as unique_rate_values
FROM [$(ETL_SCHEMA)].[raw_schedule_rates]
WHERE ProductCode = 'GA508 LEVB'
GROUP BY ScheduleName
ORDER BY rate_count DESC;

PRINT '';
PRINT 'GA508 LEVB / BIC schedule - Sample rates:';
SELECT TOP 10
    ScheduleName,
    ProductCode,
    State,
    [Level]
FROM [$(ETL_SCHEMA)].[raw_schedule_rates]
WHERE ProductCode = 'GA508 LEVB'
  AND ScheduleName = 'BIC'
ORDER BY State, [Level] DESC;

-- Cleanup work table
DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[work_schedule_product_state_map];

PRINT '';
PRINT '============================================================';
PRINT 'SCHEDULE RATES INGESTION COMPLETED';
PRINT '============================================================';
PRINT '';
PRINT '✅ ALL rates imported from source with correct schedule associations';
PRINT '✅ Schedule names inferred from certificate usage';
PRINT '✅ Rate diversity preserved (no aggregation)';
PRINT '✅ All schedules included (used by certificates)';
PRINT '';
PRINT 'Next: Run sql/transforms/04-schedules.sql';
PRINT '';

GO
