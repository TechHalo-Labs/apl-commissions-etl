-- =============================================================================
-- Transform: Brokers (SQL Server)
-- =============================================================================
-- Imports ALL brokers from raw tables (not filtered to active only)
-- This ensures complete license and E&O coverage
-- Usage: sqlcmd -S server -d database -i transforms/01-brokers.sql
-- =============================================================================

SET NOCOUNT ON;

-- Configurable schema names (substituted by TypeScript executor)
-- $(ETL_SCHEMA) - Processing schema (default: etl)
-- $(DEBUG_MODE) - Debug mode flag (0 or 1)
-- $(MAX_BROKERS) - Max records in debug mode

PRINT '============================================================';
PRINT 'TRANSFORM: BROKERS';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Brokers - ALL brokers from raw tables (for complete license/E&O coverage)
-- =============================================================================
TRUNCATE TABLE [etl].[stg_brokers];

-- Individual brokers (ALL, not just active)
PRINT 'Loading ALL individual brokers...';

INSERT INTO [etl].[stg_brokers] (
    Id, ExternalPartyId, Name, FirstName, LastName, [Type], [Status], Email, HireDate, CreationTime, IsDeleted
)
SELECT
    TRY_CAST(REPLACE(ib.PartyUniqueId, 'P', '') AS BIGINT) AS Id,
    ib.PartyUniqueId AS ExternalPartyId,
    CONCAT(ib.IndividualLastName, ', ', ib.IndividualFirstName) AS Name,
    ib.IndividualFirstName AS FirstName,
    ib.IndividualLastName AS LastName,
    'Individual' AS [Type],
    CASE 
        WHEN ib.CurrentStatus = 'Active' THEN 'Active'
        WHEN ib.CurrentStatus = 'Terminated' THEN 'Terminated'
        WHEN ib.CurrentStatus = 'Terminated Residuals' THEN 'TerminatedResiduals'
        ELSE 'Active'
    END AS [Status],
    ib.EmailAddress AS Email,
    TRY_CONVERT(DATE, ib.HireDate) AS HireDate,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[raw_individual_brokers] ib
WHERE ib.PartyUniqueId IS NOT NULL AND ib.PartyUniqueId <> '';

PRINT 'Individual brokers loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Organization brokers (ALL, not just active)
PRINT 'Loading ALL organization brokers...';

INSERT INTO [etl].[stg_brokers] (
    Id, ExternalPartyId, Name, [Type], [Status], Email, HireDate, CreationTime, IsDeleted
)
SELECT
    TRY_CAST(REPLACE(ob.PartyUniqueId, 'P', '') AS BIGINT) AS Id,
    ob.PartyUniqueId AS ExternalPartyId,
    ob.OrganizationName AS Name,
    'Organization' AS [Type],
    CASE 
        WHEN ob.CurrentStatus = 'Active' THEN 'Active'
        WHEN ob.CurrentStatus = 'Terminated' THEN 'Terminated'
        WHEN ob.CurrentStatus = 'Terminated Residuals' THEN 'TerminatedResiduals'
        ELSE 'Active'
    END AS [Status],
    ob.EmailAddress AS Email,
    TRY_CONVERT(DATE, ob.HireDate) AS HireDate,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[raw_org_brokers] ob
WHERE ob.PartyUniqueId IS NOT NULL AND ob.PartyUniqueId <> ''
  AND ob.PartyUniqueId NOT IN (SELECT ExternalPartyId FROM [etl].[stg_brokers] WHERE ExternalPartyId IS NOT NULL);

PRINT 'Organization brokers loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Load brokers from LEGACY individual roster (fallback for historical brokers)
PRINT 'Loading brokers from legacy individual roster...';

IF OBJECT_ID('etl.raw_individual_brokers_legacy', 'U') IS NOT NULL
BEGIN
    INSERT INTO [etl].[stg_brokers] (
        Id, ExternalPartyId, Name, FirstName, LastName, [Type], [Status], Email, HireDate, CreationTime, IsDeleted
    )
    SELECT
        TRY_CAST(REPLACE(ib.PartyUniqueId, 'P', '') AS BIGINT) AS Id,
        ib.PartyUniqueId AS ExternalPartyId,
        CONCAT(ib.IndividualLastName, ', ', ib.IndividualFirstName) AS Name,
        ib.IndividualFirstName AS FirstName,
        ib.IndividualLastName AS LastName,
        'Individual' AS [Type],
        CASE 
            WHEN ib.CurrentStatus = 'Active' THEN 'Active'
            WHEN ib.CurrentStatus = 'Terminated' THEN 'Terminated'
            WHEN ib.CurrentStatus = 'Terminated Residuals' THEN 'TerminatedResiduals'
            ELSE 'Active'
        END AS [Status],
        ib.EmailAddress AS Email,
        TRY_CONVERT(DATE, ib.HireDate) AS HireDate,
        GETUTCDATE() AS CreationTime,
        0 AS IsDeleted
    FROM [etl].[raw_individual_brokers_legacy] ib
    WHERE ib.PartyUniqueId IS NOT NULL AND ib.PartyUniqueId <> ''
      AND ib.PartyUniqueId NOT IN (SELECT ExternalPartyId FROM [etl].[stg_brokers] WHERE ExternalPartyId IS NOT NULL);
    
    PRINT 'Legacy individual brokers loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
END
ELSE
BEGIN
    PRINT 'Legacy individual roster table not found (optional)';
END

-- Load brokers from LEGACY organization roster (fallback for historical orgs)
PRINT 'Loading brokers from legacy organization roster...';

IF OBJECT_ID('etl.raw_org_brokers_legacy', 'U') IS NOT NULL
BEGIN
    INSERT INTO [etl].[stg_brokers] (
        Id, ExternalPartyId, Name, [Type], [Status], Email, HireDate, CreationTime, IsDeleted
    )
    SELECT
        TRY_CAST(REPLACE(ob.PartyUniqueId, 'P', '') AS BIGINT) AS Id,
        ob.PartyUniqueId AS ExternalPartyId,
        ob.OrganizationName AS Name,
        'Organization' AS [Type],
        CASE 
            WHEN ob.CurrentStatus = 'Active' THEN 'Active'
            WHEN ob.CurrentStatus = 'Terminated' THEN 'Terminated'
            WHEN ob.CurrentStatus = 'Terminated Residuals' THEN 'TerminatedResiduals'
            ELSE 'Active'
        END AS [Status],
        ob.EmailAddress AS Email,
        TRY_CONVERT(DATE, ob.HireDate) AS HireDate,
        GETUTCDATE() AS CreationTime,
        0 AS IsDeleted
    FROM [etl].[raw_org_brokers_legacy] ob
    WHERE ob.PartyUniqueId IS NOT NULL AND ob.PartyUniqueId <> ''
      AND ob.PartyUniqueId NOT IN (SELECT ExternalPartyId FROM [etl].[stg_brokers] WHERE ExternalPartyId IS NOT NULL);
    
    PRINT 'Legacy organization brokers loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
END
ELSE
BEGIN
    PRINT 'Legacy organization roster table not found (optional)';
END

-- Finally, insert PLACEHOLDER brokers for any still-missing references
PRINT 'Loading placeholder brokers for remaining missing references...';

INSERT INTO [etl].[stg_brokers] (
    Id, ExternalPartyId, Name, [Type], [Status], CreationTime, IsDeleted
)
SELECT
    TRY_CAST(REPLACE(ab.BrokerId, 'P', '') AS BIGINT) AS Id,
    ab.BrokerId AS ExternalPartyId,
    CONCAT('Broker ', ab.BrokerId) AS Name,
    'Individual' AS [Type],
    'Active' AS [Status],
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[ref_active_brokers] ab
WHERE ab.BrokerId NOT IN (SELECT ExternalPartyId FROM [etl].[stg_brokers] WHERE ExternalPartyId IS NOT NULL)
  AND TRY_CAST(REPLACE(ab.BrokerId, 'P', '') AS BIGINT) IS NOT NULL
GROUP BY ab.BrokerId;

PRINT 'Placeholder brokers loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Also insert ALL brokers from certificates (for referential integrity with all policies)
PRINT 'Loading additional brokers from certificates...';

INSERT INTO [etl].[stg_brokers] (
    Id, ExternalPartyId, Name, [Type], [Status], CreationTime, IsDeleted
)
SELECT DISTINCT
    TRY_CAST(REPLACE(WritingBrokerID, 'P', '') AS BIGINT) AS Id,
    WritingBrokerID AS ExternalPartyId,
    CONCAT('Broker ', WritingBrokerID) AS Name,
    'Individual' AS [Type],
    'Active' AS [Status],
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[input_certificate_info]
WHERE WritingBrokerID IS NOT NULL AND WritingBrokerID <> ''
  AND TRY_CAST(REPLACE(WritingBrokerID, 'P', '') AS BIGINT) IS NOT NULL
  AND TRY_CAST(REPLACE(WritingBrokerID, 'P', '') AS BIGINT) NOT IN (SELECT Id FROM [etl].[stg_brokers]);

PRINT 'Additional brokers from certificates loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT COUNT(*) AS brokers_staged FROM [etl].[stg_brokers];

GO

-- =============================================================================
-- Broker Licenses - ALL licenses (not filtered to active brokers only)
-- Date correction rules:
--   1. If EffectiveDate is NULL but ExpirationDate exists, use ExpirationDate as EffectiveDate
--   2. If ExpirationDate is NULL or in the past, set to 2027-01-01 for Active licenses
--   3. Trust CurrentStatus over date validation (Active = valid license)
-- =============================================================================
TRUNCATE TABLE [etl].[stg_broker_licenses];

PRINT '';
PRINT 'Loading ALL broker licenses with date corrections...';

INSERT INTO [etl].[stg_broker_licenses] (
    Id, BrokerId, [State], LicenseNumber, [Type], [Status], EffectiveDate, ExpirationDate,
    LicenseCode, IsResidentLicense, ApplicableCounty, CreationTime, IsDeleted
)
SELECT
    ROW_NUMBER() OVER (ORDER BY l.PartyUniqueId, l.StateCode) AS Id,
    TRY_CAST(REPLACE(l.PartyUniqueId, 'P', '') AS BIGINT) AS BrokerId,
    l.StateCode AS [State],
    l.LicenseNumber,
    0 AS [Type],
    -- Status: Trust CurrentStatus field
    CASE 
        WHEN l.CurrentStatus = 'Active' THEN 0 
        WHEN l.CurrentStatus = 'Inactive' THEN 1 
        ELSE 2 
    END AS [Status],
    -- EffectiveDate: Use ExpirationDate if EffectiveDate is NULL, otherwise default to today
    COALESCE(
        TRY_CONVERT(DATETIME2, l.LicenseEffectiveDate),
        TRY_CONVERT(DATETIME2, l.LicenseExpirationDate),
        GETUTCDATE()
    ) AS EffectiveDate,
    -- ExpirationDate: If NULL or expired for Active licenses, set to 2027-01-01
    CASE 
        WHEN l.CurrentStatus = 'Active' AND (
            l.LicenseExpirationDate IS NULL OR 
            TRY_CONVERT(DATETIME2, l.LicenseExpirationDate) < GETUTCDATE()
        )
        THEN '2027-01-01'
        ELSE COALESCE(TRY_CONVERT(DATETIME2, l.LicenseExpirationDate), '2027-01-01')
    END AS ExpirationDate,
    l.LicenseCode,
    CASE WHEN l.IsResidenceLicense = 'Y' OR l.IsResidenceLicense = 'true' THEN 1 ELSE 0 END AS IsResidentLicense,
    l.ApplicableCounty,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[raw_licenses] l
WHERE l.PartyUniqueId IS NOT NULL AND l.PartyUniqueId <> '';

PRINT 'Broker licenses loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Report date corrections made
DECLARE @nullEffectiveFixed INT;
SELECT @nullEffectiveFixed = COUNT(*) FROM [etl].[raw_licenses] 
WHERE LicenseEffectiveDate IS NULL AND PartyUniqueId IS NOT NULL AND PartyUniqueId <> '';
PRINT 'Licenses with NULL EffectiveDate (corrected): ' + CAST(@nullEffectiveFixed AS VARCHAR);

DECLARE @expiredActiveFixed INT;
SELECT @expiredActiveFixed = COUNT(*) FROM [etl].[raw_licenses] 
WHERE CurrentStatus = 'Active' 
  AND LicenseExpirationDate IS NOT NULL 
  AND TRY_CONVERT(DATETIME2, LicenseExpirationDate) < GETUTCDATE()
  AND PartyUniqueId IS NOT NULL AND PartyUniqueId <> '';
PRINT 'Active licenses with expired dates (corrected to 2027-01-01): ' + CAST(@expiredActiveFixed AS VARCHAR);

SELECT COUNT(*) AS licenses_staged FROM [etl].[stg_broker_licenses];

GO

-- =============================================================================
-- Broker E&O Insurance - ALL E&O records (not filtered to active brokers only)
-- =============================================================================
TRUNCATE TABLE [etl].[stg_broker_eo_insurances];

PRINT '';
PRINT 'Loading ALL broker E&O insurance...';

INSERT INTO [etl].[stg_broker_eo_insurances] (
    Id, BrokerId, PolicyNumber, Carrier, CoverageAmount, DeductibleAmount, ClaimMaxAmount,
    AnnualMaxAmount, PolicyMaxAmount, LiabilityLimit, EffectiveDate, ExpirationDate,
    [Status], CreationTime, IsDeleted
)
SELECT
    ROW_NUMBER() OVER (ORDER BY e.PartyUniqueId, e.PolicyId) AS Id,
    TRY_CAST(REPLACE(e.PartyUniqueId, 'P', '') AS BIGINT) AS BrokerId,
    e.PolicyId AS PolicyNumber,
    e.CarrierName AS Carrier,
    0 AS CoverageAmount,
    TRY_CAST(e.DeductibleAmount AS DECIMAL(18,2)) AS DeductibleAmount,
    TRY_CAST(e.ClaimMaxAmount AS DECIMAL(18,2)) AS ClaimMaxAmount,
    TRY_CAST(e.AnnualMaxAmount AS DECIMAL(18,2)) AS AnnualMaxAmount,
    TRY_CAST(e.PolicyMaxAmount AS DECIMAL(18,2)) AS PolicyMaxAmount,
    TRY_CAST(e.LiabilityLimit AS DECIMAL(18,2)) AS LiabilityLimit,
    TRY_CONVERT(DATETIME2, e.FromDate) AS EffectiveDate,
    TRY_CONVERT(DATETIME2, e.ToDate) AS ExpirationDate,
    0 AS [Status],
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[raw_eo_insurance] e
WHERE e.PartyUniqueId IS NOT NULL AND e.PartyUniqueId <> '';

PRINT 'Broker E&O insurance loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT COUNT(*) AS eo_staged FROM [etl].[stg_broker_eo_insurances];

PRINT '';
PRINT '============================================================';
PRINT 'BROKERS TRANSFORM COMPLETED';
PRINT '============================================================';

GO

