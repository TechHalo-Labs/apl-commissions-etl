-- =============================================================================
-- Transform: Schedules (T-SQL)
-- Creates schedules, versions, and rates from raw_schedule_rates
-- Usage: sqlcmd -S server -d database -i sql/transforms/04-schedules.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: Schedules';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Truncate staging tables
-- =============================================================================
PRINT 'Step 1: Truncating staging tables...';
TRUNCATE TABLE [etl].[stg_schedule_rates];
TRUNCATE TABLE [etl].[stg_schedule_versions];
TRUNCATE TABLE [etl].[stg_schedules];

-- =============================================================================
-- Step 2: Get active schedules (used in certificate info)
-- =============================================================================
PRINT 'Step 2: Identifying active schedules from certificates...';

DROP TABLE IF EXISTS #active_schedules;
SELECT DISTINCT LTRIM(RTRIM(CommissionsSchedule)) AS ScheduleName
INTO #active_schedules
FROM [etl].[input_certificate_info]
WHERE CommissionsSchedule IS NOT NULL 
  AND LTRIM(RTRIM(CommissionsSchedule)) <> '';

PRINT 'Active schedules from certificates: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 3: Create schedules from unique ScheduleName values
-- =============================================================================
PRINT '';
PRINT 'Step 3: Creating stg_schedules...';

INSERT INTO [etl].[stg_schedules] (
    Id, ExternalId, Name, [Description], [Status], CommissionType, RateStructure,
    EffectiveDate, EndDate, ProductCodes, ProductCount, CreationTime, IsDeleted
)
SELECT
    ROW_NUMBER() OVER (ORDER BY LTRIM(RTRIM(r.ScheduleName))) AS Id,
    LTRIM(RTRIM(r.ScheduleName)) AS ExternalId,
    CONCAT(
        COALESCE(NULLIF(LTRIM(RTRIM(MAX(r.Category))), ''), ''),
        CASE WHEN MAX(r.Category) <> '' THEN ' - ' ELSE '' END,
        LTRIM(RTRIM(r.ScheduleName))
    ) AS Name,
    MAX(LTRIM(RTRIM(r.ScheduleDescription))) AS [Description],
    'Active' AS [Status],
    'Percentage' AS CommissionType,
    'Tiered' AS RateStructure,
    MIN(TRY_CONVERT(DATE, r.EffectiveStartDate)) AS EffectiveDate,
    MAX(TRY_CONVERT(DATE, NULLIF(r.EffectiveEndDate, ''))) AS EndDate,
    NULL AS ProductCodes,  -- JSON array not needed for calculation
    COUNT(DISTINCT LTRIM(RTRIM(r.ProductCode))) AS ProductCount,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[raw_schedule_rates] r
WHERE LTRIM(RTRIM(r.ScheduleName)) <> ''
  AND EXISTS (SELECT 1 FROM #active_schedules a WHERE a.ScheduleName = LTRIM(RTRIM(r.ScheduleName)))
GROUP BY LTRIM(RTRIM(r.ScheduleName));

DECLARE @sched_count INT = @@ROWCOUNT;
PRINT 'Schedules created: ' + CAST(@sched_count AS VARCHAR);

-- =============================================================================
-- Step 4: Create schedule versions (one per schedule)
-- =============================================================================
PRINT '';
PRINT 'Step 4: Creating stg_schedule_versions...';

INSERT INTO [etl].[stg_schedule_versions] (
    Id, ScheduleId, VersionNumber, [Status], EffectiveDate, EndDate,
    ChangeReason, ApprovedBy, ApprovedAt, CreationTime, IsDeleted
)
SELECT
    Id AS Id,
    Id AS ScheduleId,
    '1.0' AS VersionNumber,
    1 AS [Status],  -- Active
    EffectiveDate,
    EndDate,
    'Initial version from migration' AS ChangeReason,
    'System' AS ApprovedBy,
    GETUTCDATE() AS ApprovedAt,
    CreationTime,
    0 AS IsDeleted
FROM [etl].[stg_schedules];

PRINT 'Schedule versions created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Update schedules with CurrentVersionId
UPDATE [etl].[stg_schedules]
SET CurrentVersionId = Id,
    CurrentVersionNumber = '1.0';

-- =============================================================================
-- Step 5: Create schedule rates
-- =============================================================================
PRINT '';
PRINT 'Step 5: Creating stg_schedule_rates...';

INSERT INTO [etl].[stg_schedule_rates] (
    Id, ScheduleVersionId, CoverageType, ProductCode, ProductName, 
    RateValue, FirstYearRate, RenewalRate, RateType, RateTypeString,
    Category, GroupSize, GroupSizeFrom, GroupSizeTo, [Level], [State],
    OffGroupLetterDescription, CreationTime, IsDeleted
)
SELECT
    ROW_NUMBER() OVER (ORDER BY s.Id, r.ProductCode, r.[State], r.GroupSizeFrom) AS Id,
    sv.Id AS ScheduleVersionId,
    LTRIM(RTRIM(r.Category)) AS CoverageType,
    LTRIM(RTRIM(r.ProductCode)) AS ProductCode,
    CONCAT(LTRIM(RTRIM(r.Category)), ' - ', LTRIM(RTRIM(r.ProductCode))) AS ProductName,
    -- RateValue: Use Level (this is the base commission rate)
    COALESCE(TRY_CAST(r.[Level] AS DECIMAL(18,4)), 0) AS RateValue,
    -- FirstYearRate: Year1 (heaped first year rate), fall back to Level if NULL/empty/zero
    COALESCE(
        NULLIF(TRY_CAST(NULLIF(LTRIM(RTRIM(r.Year1)), '') AS DECIMAL(18,4)), 0),
        TRY_CAST(NULLIF(LTRIM(RTRIM(r.[Level])), '') AS DECIMAL(18,4)),
        0
    ) AS FirstYearRate,
    -- RenewalRate: Year2 or Year66 (heaped renewal rate), fall back to Level if NULL/empty/zero
    COALESCE(
        NULLIF(TRY_CAST(NULLIF(LTRIM(RTRIM(r.Year2)), '') AS DECIMAL(18,4)), 0),
        NULLIF(TRY_CAST(NULLIF(LTRIM(RTRIM(r.Year66)), '') AS DECIMAL(18,4)), 0),
        TRY_CAST(NULLIF(LTRIM(RTRIM(r.[Level])), '') AS DECIMAL(18,4)),
        0
    ) AS RenewalRate,
    0 AS RateType,
    'Percentage' AS RateTypeString,
    LTRIM(RTRIM(r.Category)) AS Category,
    LTRIM(RTRIM(r.GroupSize)) AS GroupSize,
    TRY_CAST(r.GroupSizeFrom AS INT) AS GroupSizeFrom,
    TRY_CAST(r.GroupSizeTo AS INT) AS GroupSizeTo,
    LTRIM(RTRIM(r.[Level])) AS [Level],
    LTRIM(RTRIM(r.[State])) AS [State],
    LTRIM(RTRIM(r.OffGroupLetterDescription)) AS OffGroupLetterDescription,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[raw_schedule_rates] r
INNER JOIN [etl].[stg_schedules] s ON s.ExternalId = LTRIM(RTRIM(r.ScheduleName))
INNER JOIN [etl].[stg_schedule_versions] sv ON sv.ScheduleId = s.Id
WHERE LTRIM(RTRIM(r.ProductCode)) <> '';

PRINT 'Schedule rates created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 6: Consolidate to catch-all format where applicable
-- If a product has the same rate for ALL states, consolidate to a single row with State = NULL
-- =============================================================================
PRINT '';
PRINT 'Step 6: Converting uniform-rate schedules to catch-all format...';

-- Identify schedules where each product has only ONE distinct rate across all states
DROP TABLE IF EXISTS #uniform_rate_products;

SELECT sr.ScheduleVersionId, sr.ProductCode, 
       MIN(sr.[Level]) AS [Level],
       MIN(sr.FirstYearRate) AS FirstYearRate,
       MIN(sr.RenewalRate) AS RenewalRate,
       COUNT(DISTINCT sr.[State]) AS StateCount,
       COUNT(DISTINCT CONCAT(sr.[Level], '|', sr.FirstYearRate, '|', sr.RenewalRate)) AS DistinctRates
INTO #uniform_rate_products
FROM [etl].[stg_schedule_rates] sr
WHERE sr.[State] IS NOT NULL AND sr.[State] <> ''
GROUP BY sr.ScheduleVersionId, sr.ProductCode
HAVING COUNT(DISTINCT CONCAT(sr.[Level], '|', sr.FirstYearRate, '|', sr.RenewalRate)) = 1
   AND COUNT(DISTINCT sr.[State]) > 1;  -- Must have multiple states with same rate

DECLARE @uniform_products INT = @@ROWCOUNT;
PRINT 'Products with uniform rates across multiple states: ' + CAST(@uniform_products AS VARCHAR);

-- Delete the state-specific rows for uniform products
DELETE sr
FROM [etl].[stg_schedule_rates] sr
INNER JOIN #uniform_rate_products urp 
    ON urp.ScheduleVersionId = sr.ScheduleVersionId 
    AND urp.ProductCode = sr.ProductCode
WHERE sr.[State] IS NOT NULL AND sr.[State] <> '';

PRINT 'State-specific rate rows deleted: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Insert a single catch-all row for each uniform product
INSERT INTO [etl].[stg_schedule_rates] (
    Id, ScheduleVersionId, CoverageType, ProductCode, ProductName, 
    RateValue, FirstYearRate, RenewalRate, RateType, RateTypeString,
    Category, GroupSize, GroupSizeFrom, GroupSizeTo, [Level], [State],
    OffGroupLetterDescription, CreationTime, IsDeleted
)
SELECT
    (SELECT ISNULL(MAX(Id), 0) FROM [etl].[stg_schedule_rates]) + ROW_NUMBER() OVER (ORDER BY urp.ScheduleVersionId, urp.ProductCode) AS Id,
    urp.ScheduleVersionId,
    NULL AS CoverageType,
    urp.ProductCode,
    urp.ProductCode AS ProductName,
    TRY_CAST(urp.[Level] AS DECIMAL(18,4)) AS RateValue,
    urp.FirstYearRate,
    urp.RenewalRate,
    0 AS RateType,
    'Percentage' AS RateTypeString,
    NULL AS Category,
    NULL AS GroupSize,
    NULL AS GroupSizeFrom,
    NULL AS GroupSizeTo,
    urp.[Level],
    NULL AS [State],  -- Catch-all: NULL state applies to all states
    NULL AS OffGroupLetterDescription,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #uniform_rate_products urp;

PRINT 'Catch-all rate rows created: ' + CAST(@@ROWCOUNT AS VARCHAR);

DROP TABLE IF EXISTS #uniform_rate_products;

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'Schedules' AS entity, COUNT(*) AS cnt FROM [etl].[stg_schedules];
SELECT 'Schedule Versions' AS entity, COUNT(*) AS cnt FROM [etl].[stg_schedule_versions];
SELECT 'Schedule Rates' AS entity, COUNT(*) AS cnt FROM [etl].[stg_schedule_rates];

-- Rate coverage summary
-- Heaped: Has Year1/Year2 values (FirstYearRate or RenewalRate > 0)
-- Level only: Only has Level value (FirstYearRate = 0 AND RenewalRate = 0 AND Level > 0)
SELECT 'Rate coverage by type' AS metric,
       SUM(CASE WHEN FirstYearRate > 0 OR RenewalRate > 0 THEN 1 ELSE 0 END) AS heaped_rates,
       SUM(CASE WHEN FirstYearRate = 0 AND RenewalRate = 0 AND TRY_CAST([Level] AS DECIMAL(18,4)) > 0 THEN 1 ELSE 0 END) AS level_only_rates,
       SUM(CASE WHEN FirstYearRate = 0 AND RenewalRate = 0 AND (TRY_CAST([Level] AS DECIMAL(18,4)) = 0 OR [Level] IS NULL) THEN 1 ELSE 0 END) AS zero_rates
FROM [etl].[stg_schedule_rates];

-- Top schedules by rate count
SELECT TOP 10 
    s.ExternalId AS ScheduleName,
    s.Name,
    COUNT(sr.Id) AS rate_count
FROM [etl].[stg_schedules] s
LEFT JOIN [etl].[stg_schedule_versions] sv ON sv.ScheduleId = s.Id
LEFT JOIN [etl].[stg_schedule_rates] sr ON sr.ScheduleVersionId = sv.Id
GROUP BY s.Id, s.ExternalId, s.Name
ORDER BY rate_count DESC;

-- Cleanup
DROP TABLE IF EXISTS #active_schedules;

PRINT '';
PRINT '============================================================';
PRINT 'SCHEDULES TRANSFORM COMPLETED';
PRINT '============================================================';

GO

