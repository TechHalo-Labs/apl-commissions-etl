-- =====================================================
-- Export Schedules from etl staging to dbo
-- =====================================================

PRINT 'Clearing existing schedule data...';

-- Clear FK references first
UPDATE [$(PRODUCTION_SCHEMA)].[EmployerGroups] SET ActiveScheduleId = NULL;
UPDATE [$(PRODUCTION_SCHEMA)].[Contracts] SET ScheduleId = NULL WHERE ScheduleId IS NOT NULL;
DELETE FROM [$(PRODUCTION_SCHEMA)].[GroupScheduleAssignments];

-- Delete in FK order
DELETE FROM [$(PRODUCTION_SCHEMA)].[ScheduleRates];
UPDATE [$(PRODUCTION_SCHEMA)].[Schedules] SET CurrentVersionId = NULL;
DELETE FROM [$(PRODUCTION_SCHEMA)].[ScheduleVersions];
DELETE FROM [$(PRODUCTION_SCHEMA)].[Schedules];

PRINT 'Existing schedule data cleared';
GO

-- Export Schedules
PRINT 'Exporting Schedules...';

SET IDENTITY_INSERT [$(PRODUCTION_SCHEMA)].[Schedules] ON;

INSERT INTO [$(PRODUCTION_SCHEMA)].[Schedules] (
    Id, ExternalId, Name, Description, [Status], CommissionType, RateStructure,
    EffectiveDate, EndDate, ProductLines, ProductCodes, Owner,
    ContractCount, ProductCount, CurrentVersionId, CurrentVersionNumber,
    CreationTime, IsDeleted
)
SELECT 
    CAST(s.Id AS BIGINT) AS Id,
    s.ExternalId,
    s.Name,
    s.Description,
    CASE s.[Status] WHEN 'Active' THEN 0 WHEN 'Inactive' THEN 1 ELSE 0 END AS [Status],
    s.CommissionType,
    s.RateStructure,
    s.EffectiveDate,
    s.EndDate,
    s.ProductLines,
    s.ProductCodes,
    s.Owner,
    COALESCE(TRY_CAST(s.ContractCount AS INT), 0) AS ContractCount,
    COALESCE(TRY_CAST(s.ProductCount AS INT), 0) AS ProductCount,
    NULL AS CurrentVersionId,
    COALESCE(CAST(FLOOR(TRY_CAST(s.CurrentVersionNumber AS FLOAT)) AS INT), 1) AS CurrentVersionNumber,
    s.CreationTime,
    s.IsDeleted
FROM [$(ETL_SCHEMA)].[stg_schedules] s;

SET IDENTITY_INSERT [$(PRODUCTION_SCHEMA)].[Schedules] OFF;

DECLARE @schedCount INT;
SELECT @schedCount = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[Schedules];
PRINT 'Schedules exported: ' + CAST(@schedCount AS VARCHAR);
GO

-- Export Schedule Versions
PRINT 'Exporting Schedule Versions...';

SET IDENTITY_INSERT [$(PRODUCTION_SCHEMA)].[ScheduleVersions] ON;

INSERT INTO [$(PRODUCTION_SCHEMA)].[ScheduleVersions] (
    Id, scheduleId, versionNumber, [status], effectiveDate, endDate,
    changeReason, approvedBy, approvedAt, CreationTime, IsDeleted
)
SELECT 
    CAST(sv.Id AS BIGINT) AS Id,
    CAST(sv.ScheduleId AS BIGINT) AS scheduleId,
    COALESCE(CAST(FLOOR(TRY_CAST(sv.VersionNumber AS FLOAT)) AS INT), 1) AS versionNumber,
    COALESCE(TRY_CAST(sv.[Status] AS INT), 0) AS [status],
    sv.EffectiveDate AS effectiveDate,
    sv.EndDate AS endDate,
    sv.ChangeReason AS changeReason,
    sv.ApprovedBy AS approvedBy,
    sv.ApprovedAt AS approvedAt,
    sv.CreationTime,
    sv.IsDeleted
FROM [$(ETL_SCHEMA)].[stg_schedule_versions] sv;

SET IDENTITY_INSERT [$(PRODUCTION_SCHEMA)].[ScheduleVersions] OFF;

DECLARE @versCount INT;
SELECT @versCount = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[ScheduleVersions];
PRINT 'Schedule Versions exported: ' + CAST(@versCount AS VARCHAR);
GO

-- Update CurrentVersionId
PRINT 'Updating Schedules.CurrentVersionId...';

UPDATE s
SET s.CurrentVersionId = sv.Id
FROM [$(PRODUCTION_SCHEMA)].[Schedules] s
INNER JOIN [$(PRODUCTION_SCHEMA)].[ScheduleVersions] sv ON sv.scheduleId = s.Id;

PRINT 'CurrentVersionId updated';
GO

-- Export Schedule Rates
PRINT 'Exporting Schedule Rates...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[ScheduleRates] (
    ScheduleVersionId, ProductCode, ProductName, Category, OffGroupLetterDescription,
    [State], GroupSizeFrom, GroupSizeTo, GroupSize, FirstYearRate, RenewalRate,
    BonusRate, OverrideRate, [Level], RateTypeString, RateType, CoverageType,
    RateValue, MinCoverage, MaxCoverage, CreationTime, IsDeleted
)
SELECT 
    CAST(sr.ScheduleVersionId AS BIGINT) AS ScheduleVersionId,
    sr.ProductCode,
    sr.ProductName,
    sr.Category,
    sr.OffGroupLetterDescription,
    sr.[State],
    sr.GroupSizeFrom,
    sr.GroupSizeTo,
    sr.GroupSize,
    sr.FirstYearRate,
    sr.RenewalRate,
    sr.BonusRate,
    sr.OverrideRate,
    sr.[Level],
    sr.RateTypeString,
    COALESCE(TRY_CAST(sr.RateType AS INT), 0) AS RateType,
    sr.CoverageType,
    sr.RateValue,
    sr.MinCoverage,
    sr.MaxCoverage,
    sr.CreationTime,
    sr.IsDeleted
FROM [$(ETL_SCHEMA)].[stg_schedule_rates] sr;

DECLARE @rateCount INT;
SELECT @rateCount = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[ScheduleRates];
PRINT 'Schedule Rates exported: ' + CAST(@rateCount AS VARCHAR);
GO

-- Populate ProductCodeList as CSV of product codes from rates
PRINT 'Populating ProductCodeList on Schedules...';

UPDATE s
SET ProductCodeList = STUFF(
    (SELECT ',' + sr.ProductCode
     FROM [$(PRODUCTION_SCHEMA)].[ScheduleRates] sr 
     INNER JOIN [$(PRODUCTION_SCHEMA)].[ScheduleVersions] sv ON sv.Id = sr.ScheduleVersionId
     WHERE sv.ScheduleId = s.Id
     GROUP BY sr.ProductCode
     ORDER BY sr.ProductCode
     FOR XML PATH('')),
    1, 1, '')  -- Remove leading comma
FROM [$(PRODUCTION_SCHEMA)].[Schedules] s
WHERE EXISTS (
    SELECT 1 
    FROM [$(PRODUCTION_SCHEMA)].[ScheduleRates] sr2 
    INNER JOIN [$(PRODUCTION_SCHEMA)].[ScheduleVersions] sv2 ON sv2.Id = sr2.ScheduleVersionId
    WHERE sv2.ScheduleId = s.Id
);

DECLARE @prodCodeUpdated INT;
SELECT @prodCodeUpdated = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[Schedules] WHERE ProductCodeList IS NOT NULL;
PRINT 'Schedules with ProductCodeList populated: ' + CAST(@prodCodeUpdated AS VARCHAR);
GO

PRINT '=== Schedule Export Complete ===';

