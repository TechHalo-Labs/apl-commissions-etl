-- ================================================================
-- EXPORT: Schedule Versions and Schedule Rates
-- ================================================================
-- Exports from etl.stg_schedule_versions and etl.stg_schedule_rates
-- to dbo.ScheduleVersions and dbo.ScheduleRates
-- ================================================================

SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;
GO

PRINT '================================================================'
PRINT 'EXPORT: Schedule Versions and Schedule Rates'
PRINT '================================================================'
PRINT ''

-- ================================================================
-- Step 1: Clear existing data (in correct FK order)
-- ================================================================
PRINT 'Step 1: Clearing existing data...'

DELETE FROM dbo.ScheduleRates;
PRINT '  Cleared ScheduleRates: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows'

DELETE FROM dbo.ScheduleVersions;
PRINT '  Cleared ScheduleVersions: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows'

PRINT ''
GO

-- ================================================================
-- Step 2: Export Schedule Versions
-- ================================================================
PRINT 'Step 2: Exporting Schedule Versions...'

SET IDENTITY_INSERT dbo.ScheduleVersions ON;

INSERT INTO dbo.ScheduleVersions (
    Id, scheduleId, versionNumber, status, effectiveDate, endDate,
    changeReason, approvedBy, approvedAt, CreationTime, IsDeleted
)
SELECT 
    Id, ScheduleId, VersionNumber, Status, EffectiveDate, EndDate,
    ChangeReason, ApprovedBy, ApprovedAt, CreationTime, IsDeleted
FROM etl.stg_schedule_versions
WHERE EXISTS (SELECT 1 FROM dbo.Schedules WHERE Id = stg_schedule_versions.ScheduleId);

SET IDENTITY_INSERT dbo.ScheduleVersions OFF;

PRINT '  Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Schedule Versions'
PRINT ''
GO

-- ================================================================
-- Step 3: Export Schedule Rates
-- ================================================================
PRINT 'Step 3: Exporting Schedule Rates...'

SET IDENTITY_INSERT dbo.ScheduleRates ON;

INSERT INTO dbo.ScheduleRates (
    Id, ScheduleVersionId, ProductCode, ProductName, Category,
    OffGroupLetterDescription, State, GroupSizeFrom, GroupSizeTo, GroupSize,
    FirstYearRate, RenewalRate, BonusRate, OverrideRate,
    Level, RateTypeString, RateType, CoverageType,
    RateValue, MinCoverage, MaxCoverage,
    CreationTime, IsDeleted
)
SELECT 
    Id, ScheduleVersionId, ProductCode, ProductName, Category,
    OffGroupLetterDescription, State, GroupSizeFrom, GroupSizeTo, GroupSize,
    FirstYearRate, RenewalRate, BonusRate, OverrideRate,
    Level, RateTypeString, RateType, CoverageType,
    RateValue, MinCoverage, MaxCoverage,
    CreationTime, IsDeleted
FROM etl.stg_schedule_rates
WHERE EXISTS (SELECT 1 FROM dbo.ScheduleVersions WHERE Id = stg_schedule_rates.ScheduleVersionId);

SET IDENTITY_INSERT dbo.ScheduleRates OFF;

PRINT '  Exported ' + CAST(@@ROWCOUNT AS VARCHAR) + ' Schedule Rates'
PRINT ''
GO

-- ================================================================
-- Step 4: Verification
-- ================================================================
PRINT '================================================================'
PRINT 'VERIFICATION'
PRINT '================================================================'
PRINT ''

SELECT 'ScheduleVersions' AS Entity,
    (SELECT COUNT(*) FROM etl.stg_schedule_versions) AS Staging,
    (SELECT COUNT(*) FROM dbo.ScheduleVersions) AS Production
UNION ALL
SELECT 'ScheduleRates',
    (SELECT COUNT(*) FROM etl.stg_schedule_rates),
    (SELECT COUNT(*) FROM dbo.ScheduleRates);

PRINT ''
PRINT '================================================================'
PRINT 'EXPORT COMPLETE'
PRINT '================================================================'
GO
