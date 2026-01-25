-- =============================================================================
-- VERIFICATION: ETL Export Gap Status
-- =============================================================================
-- Run this script to check current status of identified gaps:
-- 1. SpecialScheduleRates - count in production
-- 2. ScheduleRateTiers - count in production
-- 3. HierarchyParticipantProductRates - count in production
-- 4. Staging tables existence
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'ETL EXPORT GAP STATUS VERIFICATION';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- 1. Check Production Table Counts
-- =============================================================================
PRINT '--- Production Table Counts ---';
PRINT '';

SELECT 'SpecialScheduleRates' AS [Table], COUNT(*) AS [Count] FROM [dbo].[SpecialScheduleRates]
UNION ALL
SELECT 'ScheduleRateTiers', COUNT(*) FROM [dbo].[ScheduleRateTiers]
UNION ALL
SELECT 'HierarchyParticipantProductRates', COUNT(*) FROM [dbo].[HierarchyParticipantProductRates]
UNION ALL
SELECT 'ScheduleRates (base)', COUNT(*) FROM [dbo].[ScheduleRates]
UNION ALL
SELECT 'HierarchyParticipants (base)', COUNT(*) FROM [dbo].[HierarchyParticipants];

PRINT '';

-- =============================================================================
-- 2. Check Staging Tables Existence
-- =============================================================================
PRINT '--- Staging Tables Existence ---';
PRINT '';

SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'stg_special_schedule_rates'
    ) THEN 'EXISTS' ELSE 'MISSING' END AS stg_special_schedule_rates,
    CASE WHEN EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'stg_schedule_rate_tiers'
    ) THEN 'EXISTS' ELSE 'MISSING' END AS stg_schedule_rate_tiers,
    CASE WHEN EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'stg_hierarchy_participant_product_rates'
    ) THEN 'EXISTS' ELSE 'MISSING' END AS stg_hierarchy_participant_product_rates;

-- =============================================================================
-- 3. Check Staging Table Counts (if they exist)
-- =============================================================================
PRINT '';
PRINT '--- Staging Table Counts (if tables exist) ---';
PRINT '';

DECLARE @sql NVARCHAR(MAX) = '';

IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'stg_special_schedule_rates')
BEGIN
    SET @sql = 'SELECT ''stg_special_schedule_rates'' AS [Table], COUNT(*) AS [Count] FROM [etl].[stg_special_schedule_rates]';
    EXEC sp_executesql @sql;
END
ELSE
    PRINT 'stg_special_schedule_rates: Table does not exist';

IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'stg_schedule_rate_tiers')
BEGIN
    SET @sql = 'SELECT ''stg_schedule_rate_tiers'' AS [Table], COUNT(*) AS [Count] FROM [etl].[stg_schedule_rate_tiers]';
    EXEC sp_executesql @sql;
END
ELSE
    PRINT 'stg_schedule_rate_tiers: Table does not exist';

IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'stg_hierarchy_participant_product_rates')
BEGIN
    SET @sql = 'SELECT ''stg_hierarchy_participant_product_rates'' AS [Table], COUNT(*) AS [Count] FROM [etl].[stg_hierarchy_participant_product_rates]';
    EXEC sp_executesql @sql;
END
ELSE
    PRINT 'stg_hierarchy_participant_product_rates: Table does not exist';

-- =============================================================================
-- 4. Check Proposal Constraints
-- =============================================================================
PRINT '';
PRINT '--- Proposal Constraints Status ---';
PRINT '';

SELECT 
    'Proposals with Plan Code Filtering' AS Metric,
    COUNT(*) AS [Count]
FROM [dbo].[Proposals]
WHERE EnablePlanCodeFiltering = 1

UNION ALL

SELECT 
    'Proposals with Effective Date Filtering',
    COUNT(*)
FROM [dbo].[Proposals]
WHERE EnableEffectiveDateFiltering = 1

UNION ALL

SELECT 
    'Proposals with ConstrainingEffectiveDateFrom',
    COUNT(*)
FROM [dbo].[Proposals]
WHERE ConstrainingEffectiveDateFrom IS NOT NULL

UNION ALL

SELECT 
    'Total Proposals',
    COUNT(*)
FROM [dbo].[Proposals];

-- =============================================================================
-- 5. Sample Heaped Schedule Data (if any)
-- =============================================================================
PRINT '';
PRINT '--- Sample Heaped Schedule Data (SpecialScheduleRates) ---';
PRINT '';

IF (SELECT COUNT(*) FROM [dbo].[SpecialScheduleRates]) > 0
BEGIN
    SELECT TOP 10 
        ssr.Id,
        ssr.ScheduleRateId,
        sr.ProductCode,
        ssr.[Year],
        ssr.Rate,
        s.Name AS ScheduleName
    FROM [dbo].[SpecialScheduleRates] ssr
    INNER JOIN [dbo].[ScheduleRates] sr ON sr.Id = ssr.ScheduleRateId
    INNER JOIN [dbo].[ScheduleVersions] sv ON sv.Id = sr.ScheduleVersionId
    INNER JOIN [dbo].[Schedules] s ON s.Id = sv.ScheduleId
    ORDER BY s.Name, sr.ProductCode, ssr.[Year];
END
ELSE
    PRINT 'No SpecialScheduleRates data found in production.';

-- =============================================================================
-- 6. Sample Tiered Rate Data (if any)
-- =============================================================================
PRINT '';
PRINT '--- Sample Tiered Rate Data (ScheduleRateTiers) ---';
PRINT '';

IF (SELECT COUNT(*) FROM [dbo].[ScheduleRateTiers]) > 0
BEGIN
    SELECT TOP 10 
        srt.Id,
        srt.ScheduleRateId,
        sr.ProductCode,
        srt.MinVolume,
        srt.MaxVolume,
        srt.Rate,
        srt.FirstYearRate,
        srt.RenewalRate,
        s.Name AS ScheduleName
    FROM [dbo].[ScheduleRateTiers] srt
    INNER JOIN [dbo].[ScheduleRates] sr ON sr.Id = srt.ScheduleRateId
    INNER JOIN [dbo].[ScheduleVersions] sv ON sv.Id = sr.ScheduleVersionId
    INNER JOIN [dbo].[Schedules] s ON s.Id = sv.ScheduleId
    ORDER BY s.Name, sr.ProductCode, srt.MinVolume;
END
ELSE
    PRINT 'No ScheduleRateTiers data found in production.';

-- =============================================================================
-- 7. Sample Product Rate Overrides (if any)
-- =============================================================================
PRINT '';
PRINT '--- Sample Product Rate Overrides (HierarchyParticipantProductRates) ---';
PRINT '';

IF (SELECT COUNT(*) FROM [dbo].[HierarchyParticipantProductRates]) > 0
BEGIN
    SELECT TOP 10 
        hppr.Id,
        hppr.HierarchyParticipantId,
        hp.EntityName AS ParticipantName,
        hppr.ProductCode,
        hppr.FirstYearRate,
        hppr.RenewalRate,
        h.Name AS HierarchyName
    FROM [dbo].[HierarchyParticipantProductRates] hppr
    INNER JOIN [dbo].[HierarchyParticipants] hp ON hp.Id = hppr.HierarchyParticipantId
    INNER JOIN [dbo].[HierarchyVersions] hv ON hv.Id = hp.HierarchyVersionId
    INNER JOIN [dbo].[Hierarchies] h ON h.Id = hv.HierarchyId
    ORDER BY h.Name, hp.EntityName, hppr.ProductCode;
END
ELSE
    PRINT 'No HierarchyParticipantProductRates data found in production.';

-- =============================================================================
-- Summary
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION COMPLETE';
PRINT '============================================================';
PRINT '';
PRINT 'If production tables have 0 records, this means either:';
PRINT '1. Raw data source does not include this information, OR';
PRINT '2. Staging tables need to be populated from transforms, OR';
PRINT '3. Export scripts need to be run after staging is populated.';
PRINT '';
PRINT 'Next Steps:';
PRINT '1. Run 01-add-missing-staging-tables.sql to create staging tables';
PRINT '2. Identify raw data sources and create transform scripts';
PRINT '3. Run export scripts 17, 18, 19 after staging is populated';

GO
