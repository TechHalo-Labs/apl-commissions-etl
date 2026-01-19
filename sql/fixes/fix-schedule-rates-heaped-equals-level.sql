-- =============================================================================
-- Fix: Reset FirstYearRate and RenewalRate to 0 where they equal Level
-- 
-- Problem: The ETL incorrectly used Level as a fallback for empty Year1/Year2,
-- causing FirstYearRate, RenewalRate, and Level to all show the same value.
--
-- Solution: If FirstYearRate == RenewalRate == Level, reset heaped rates to 0.
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'FIX: Schedule Rates - Reset heaped rates that equal Level';
PRINT '============================================================';
PRINT '';

-- Preview affected rows
PRINT 'Preview: Rows to be updated...';
SELECT TOP 20
    sr.Id,
    s.ExternalId AS ScheduleCode,
    sr.ProductCode,
    sr.[State],
    sr.FirstYearRate,
    sr.RenewalRate,
    sr.[Level],
    TRY_CAST(sr.[Level] AS DECIMAL(18,6)) AS LevelAsDecimal
FROM [dbo].[ScheduleRates] sr
INNER JOIN [dbo].[ScheduleVersions] sv ON sv.Id = sr.ScheduleVersionId
INNER JOIN [dbo].[Schedules] s ON s.Id = sv.ScheduleId
WHERE sr.[Level] IS NOT NULL 
  AND sr.[Level] <> ''
  AND sr.FirstYearRate = TRY_CAST(sr.[Level] AS DECIMAL(18,6))
  AND sr.RenewalRate = TRY_CAST(sr.[Level] AS DECIMAL(18,6));

-- Count affected rows
DECLARE @affected_count INT;
SELECT @affected_count = COUNT(*)
FROM [dbo].[ScheduleRates]
WHERE [Level] IS NOT NULL 
  AND [Level] <> ''
  AND FirstYearRate = TRY_CAST([Level] AS DECIMAL(18,6))
  AND RenewalRate = TRY_CAST([Level] AS DECIMAL(18,6));

PRINT '';
PRINT 'Total rows to update: ' + CAST(@affected_count AS VARCHAR);
PRINT '';

-- Perform the update
PRINT 'Updating ScheduleRates...';

UPDATE [dbo].[ScheduleRates]
SET FirstYearRate = 0,
    RenewalRate = 0,
    LastModificationTime = GETUTCDATE()
WHERE [Level] IS NOT NULL 
  AND [Level] <> ''
  AND FirstYearRate = TRY_CAST([Level] AS DECIMAL(18,6))
  AND RenewalRate = TRY_CAST([Level] AS DECIMAL(18,6));

PRINT 'Rows updated: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Verification
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 
    'Heaped rates (Year1 or Year2 > 0)' AS rate_type,
    COUNT(*) AS cnt
FROM [dbo].[ScheduleRates]
WHERE FirstYearRate > 0 OR RenewalRate > 0

UNION ALL

SELECT 
    'Level-only rates (heaped = 0, level > 0)' AS rate_type,
    COUNT(*) AS cnt
FROM [dbo].[ScheduleRates]
WHERE FirstYearRate = 0 
  AND RenewalRate = 0 
  AND TRY_CAST([Level] AS DECIMAL(18,6)) > 0

UNION ALL

SELECT 
    'Zero rates (all = 0)' AS rate_type,
    COUNT(*) AS cnt
FROM [dbo].[ScheduleRates]
WHERE FirstYearRate = 0 
  AND RenewalRate = 0 
  AND (TRY_CAST([Level] AS DECIMAL(18,6)) = 0 OR [Level] IS NULL OR [Level] = '');

PRINT '';
PRINT '============================================================';
PRINT 'FIX COMPLETED';
PRINT '============================================================';

GO
