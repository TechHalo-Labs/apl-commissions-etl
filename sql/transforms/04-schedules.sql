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
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_schedule_rates];
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_schedule_versions];
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_schedules];

-- =============================================================================
-- Step 2: Import ALL schedules (FILTER REMOVED)
-- =============================================================================
-- ⚠️ CRITICAL FIX: Previous logic filtered schedules to only those used by certificates
-- 
-- Problem: This excluded 35-68% of schedules per product (e.g., GA508 LEVB: 45 → 29)
-- Impact:
--   1. Cannot assign certificates to unused schedules
--   2. No complete schedule library for reference
--   3. Missing rates for future assignments
-- 
-- Fix: Import ALL schedules from source (no filtering by certificate usage)
-- =============================================================================
PRINT 'Step 2: Importing ALL schedules (no filtering)...';
PRINT '  NOTE: Previous logic filtered to only schedules used by certificates';
PRINT '  NOTE: Now importing ALL schedules for complete rate library';

-- No work table needed - we're importing everything
PRINT 'All schedules will be imported from raw_schedule_rates';

-- =============================================================================
-- Step 3: Create schedules from unique ScheduleName values
-- =============================================================================
PRINT '';
PRINT 'Step 3: Creating stg_schedules...';

INSERT INTO [$(ETL_SCHEMA)].[stg_schedules] (
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
FROM [$(ETL_SCHEMA)].[raw_schedule_rates] r
WHERE LTRIM(RTRIM(r.ScheduleName)) <> ''
  AND LTRIM(RTRIM(r.ScheduleName)) IS NOT NULL
GROUP BY LTRIM(RTRIM(r.ScheduleName));

DECLARE @sched_count INT = @@ROWCOUNT;
PRINT 'Schedules created: ' + CAST(@sched_count AS VARCHAR);

-- =============================================================================
-- Step 4: Create schedule versions (one per schedule)
-- =============================================================================
PRINT '';
PRINT 'Step 4: Creating stg_schedule_versions...';

INSERT INTO [$(ETL_SCHEMA)].[stg_schedule_versions] (
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
FROM [$(ETL_SCHEMA)].[stg_schedules];

PRINT 'Schedule versions created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Update schedules with CurrentVersionId
UPDATE [$(ETL_SCHEMA)].[stg_schedules]
SET CurrentVersionId = Id,
    CurrentVersionNumber = '1.0';

-- =============================================================================
-- Step 5: Create schedule rates
-- =============================================================================
PRINT '';
PRINT 'Step 5: Creating stg_schedule_rates...';

INSERT INTO [$(ETL_SCHEMA)].[stg_schedule_rates] (
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
FROM [$(ETL_SCHEMA)].[raw_schedule_rates] r
INNER JOIN [$(ETL_SCHEMA)].[stg_schedules] s ON s.ExternalId = LTRIM(RTRIM(r.ScheduleName))
INNER JOIN [$(ETL_SCHEMA)].[stg_schedule_versions] sv ON sv.ScheduleId = s.Id
WHERE LTRIM(RTRIM(r.ProductCode)) <> '';

PRINT 'Schedule rates created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 6: Consolidate to catch-all format where applicable (DISABLED)
-- ⚠️ CRITICAL FIX: This consolidation logic was DESTROYING state data!
-- 
-- Original logic:
-- - If a product has the same rate for ALL states, consolidate to a single row with State = NULL
-- 
-- Problem: This deleted 68% of schedule rate data (e.g., GAO21HSF: 48 records → 15)
-- Impact: 
--   1. Rate lookups by state FAIL (State = NULL in production)
--   2. Cannot apply state-specific MAC caps
--   3. Breaks regulatory compliance & reporting
--   4. Destroys audit trails
-- 
-- Fix: DISABLED consolidation - preserve ALL state-specific rate rows
-- =============================================================================
PRINT '';
PRINT 'Step 6: Catch-all consolidation DISABLED (preserving state data)...';
PRINT 'All state-specific rate rows will be preserved';

/*
-- ===== DISABLED CONSOLIDATION LOGIC =====
-- This logic has been disabled to preserve critical state data
-- If re-enabled, it will delete state-specific rows and break rate lookups

DROP TABLE IF EXISTS #uniform_rate_products;

SELECT sr.ScheduleVersionId, sr.ProductCode, 
       MIN(sr.[Level]) AS [Level],
       MIN(sr.FirstYearRate) AS FirstYearRate,
       MIN(sr.RenewalRate) AS RenewalRate,
       COUNT(DISTINCT sr.[State]) AS StateCount,
       COUNT(DISTINCT CONCAT(sr.[Level], '|', sr.FirstYearRate, '|', sr.RenewalRate)) AS DistinctRates
INTO #uniform_rate_products
FROM [$(ETL_SCHEMA)].[stg_schedule_rates] sr
WHERE sr.[State] IS NOT NULL AND sr.[State] <> ''
GROUP BY sr.ScheduleVersionId, sr.ProductCode
HAVING COUNT(DISTINCT CONCAT(sr.[Level], '|', sr.FirstYearRate, '|', sr.RenewalRate)) = 1
   AND COUNT(DISTINCT sr.[State]) > 1;

DECLARE @uniform_products INT = @@ROWCOUNT;
PRINT 'Products with uniform rates across multiple states: ' + CAST(@uniform_products AS VARCHAR);

DELETE sr
FROM [$(ETL_SCHEMA)].[stg_schedule_rates] sr
INNER JOIN #uniform_rate_products urp 
    ON urp.ScheduleVersionId = sr.ScheduleVersionId 
    AND urp.ProductCode = sr.ProductCode
WHERE sr.[State] IS NOT NULL AND sr.[State] <> '';

PRINT 'State-specific rate rows deleted: ' + CAST(@@ROWCOUNT AS VARCHAR);

INSERT INTO [$(ETL_SCHEMA)].[stg_schedule_rates] (
    Id, ScheduleVersionId, CoverageType, ProductCode, ProductName, 
    RateValue, FirstYearRate, RenewalRate, RateType, RateTypeString,
    Category, GroupSize, GroupSizeFrom, GroupSizeTo, [Level], [State],
    OffGroupLetterDescription, CreationTime, IsDeleted
)
SELECT
    (SELECT ISNULL(MAX(Id), 0) FROM [$(ETL_SCHEMA)].[stg_schedule_rates]) + ROW_NUMBER() OVER (ORDER BY urp.ScheduleVersionId, urp.ProductCode) AS Id,
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
    NULL AS [State],
    NULL AS OffGroupLetterDescription,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #uniform_rate_products urp;

PRINT 'Catch-all rate rows created: ' + CAST(@@ROWCOUNT AS VARCHAR);

DROP TABLE IF EXISTS #uniform_rate_products;
-- ===== END DISABLED CONSOLIDATION LOGIC =====
*/

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'Schedules' AS entity, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_schedules];
SELECT 'Schedule Versions' AS entity, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_schedule_versions];
SELECT 'Schedule Rates' AS entity, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_schedule_rates];

-- Rate coverage summary
-- Heaped: Has Year1/Year2 values (FirstYearRate or RenewalRate > 0)
-- Level only: Only has Level value (FirstYearRate = 0 AND RenewalRate = 0 AND Level > 0)
SELECT 'Rate coverage by type' AS metric,
       SUM(CASE WHEN FirstYearRate > 0 OR RenewalRate > 0 THEN 1 ELSE 0 END) AS heaped_rates,
       SUM(CASE WHEN FirstYearRate = 0 AND RenewalRate = 0 AND TRY_CAST([Level] AS DECIMAL(18,4)) > 0 THEN 1 ELSE 0 END) AS level_only_rates,
       SUM(CASE WHEN FirstYearRate = 0 AND RenewalRate = 0 AND (TRY_CAST([Level] AS DECIMAL(18,4)) = 0 OR [Level] IS NULL) THEN 1 ELSE 0 END) AS zero_rates
FROM [$(ETL_SCHEMA)].[stg_schedule_rates];

-- Top schedules by rate count
SELECT TOP 10 
    s.ExternalId AS ScheduleName,
    s.Name,
    COUNT(sr.Id) AS rate_count
FROM [$(ETL_SCHEMA)].[stg_schedules] s
LEFT JOIN [$(ETL_SCHEMA)].[stg_schedule_versions] sv ON sv.ScheduleId = s.Id
LEFT JOIN [$(ETL_SCHEMA)].[stg_schedule_rates] sr ON sr.ScheduleVersionId = sv.Id
GROUP BY s.Id, s.ExternalId, s.Name
ORDER BY rate_count DESC;

PRINT '';
PRINT '============================================================';
PRINT 'SCHEDULES TRANSFORM COMPLETED';
PRINT '============================================================';

GO

