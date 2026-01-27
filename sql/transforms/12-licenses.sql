-- =====================================================
-- Transform BrokerLicenses from new_data to etl staging
-- Maps PartyUniqueId to BrokerId
-- =====================================================

SET NOCOUNT ON;

PRINT 'Transforming BrokerLicenses...';

-- Truncate staging table
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_broker_licenses];

-- Insert transformed license data
INSERT INTO [$(ETL_SCHEMA)].[stg_broker_licenses] (
    Id, BrokerId, [State], LicenseNumber, LicenseCode, [Type], [Status],
    EffectiveDate, ExpirationDate, IsResidentLicense, ApplicableCounty,
    CreationTime, IsDeleted
)
SELECT 
    nl.Id,
    b.Id AS BrokerId,
    nl.StateCode AS [State],
    NULLIF(LTRIM(RTRIM(nl.LicenseNumber)), '') AS LicenseNumber,
    NULLIF(LTRIM(RTRIM(nl.LicenseCode)), '') AS LicenseCode,
    -- Type: map license code to int (default 0 for unknown)
    CASE 
        WHEN nl.LicenseCode = 'A' THEN 1
        WHEN nl.LicenseCode = 'B' THEN 2
        WHEN nl.LicenseCode = 'C' THEN 3
        ELSE 0
    END AS [Type],
    -- Status: map CurrentStatus to int (0=Active, 1=Inactive, 2=Expired)
    CASE 
        WHEN UPPER(nl.CurrentStatus) IN ('ACTIVE', 'A') THEN 0
        WHEN UPPER(nl.CurrentStatus) IN ('INACTIVE', 'I') THEN 1
        WHEN UPPER(nl.CurrentStatus) IN ('EXPIRED', 'E') THEN 2
        ELSE 0  -- Default to Active if NULL or unknown
    END AS [Status],
    -- Convert date strings to datetime2
    TRY_CAST(NULLIF(LTRIM(RTRIM(nl.LicenseEffectiveDate)), '') AS DATETIME2) AS EffectiveDate,
    TRY_CAST(NULLIF(LTRIM(RTRIM(nl.LicenseExpirationDate)), '') AS DATETIME2) AS ExpirationDate,
    -- IsResidenceLicense: check for 'Y', 'Yes', '1', or non-empty value
    CASE 
        WHEN UPPER(LTRIM(RTRIM(nl.IsResidenceLicense))) IN ('Y', 'YES', '1', 'TRUE') THEN 1
        ELSE 0
    END AS IsResidentLicense,
    NULLIF(LTRIM(RTRIM(nl.ApplicableCounty)), '') AS ApplicableCounty,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [new_data].[BrokerLicenses] nl
INNER JOIN [$(ETL_SCHEMA)].[stg_brokers] b ON b.ExternalPartyId = nl.PartyUniqueId
WHERE nl.PartyUniqueId IS NOT NULL
  AND nl.PartyUniqueId != ''
  AND nl.PartyUniqueId != 'NULL'
  AND nl.StateCode IS NOT NULL
  AND nl.StateCode != ''
  AND nl.StateCode != 'NULL';

DECLARE @licCount INT;
SELECT @licCount = @@ROWCOUNT;
PRINT 'Licenses transformed: ' + CAST(@licCount AS VARCHAR);

DECLARE @totalLicenses INT;
SELECT @totalLicenses = COUNT(*) FROM [$(ETL_SCHEMA)].[stg_broker_licenses];
PRINT 'Total licenses in staging: ' + CAST(@totalLicenses AS VARCHAR);

-- Report on licenses without matching broker
DECLARE @orphanLicenses INT;
SELECT @orphanLicenses = COUNT(*)
FROM [new_data].[BrokerLicenses] nl
LEFT JOIN [$(ETL_SCHEMA)].[stg_brokers] b ON b.ExternalPartyId = nl.PartyUniqueId
WHERE b.Id IS NULL
  AND nl.PartyUniqueId IS NOT NULL
  AND nl.PartyUniqueId != ''
  AND nl.PartyUniqueId != 'NULL';

PRINT 'Licenses without matching broker (skipped): ' + CAST(@orphanLicenses AS VARCHAR);

GO

PRINT '=== License Transform Complete ===';
