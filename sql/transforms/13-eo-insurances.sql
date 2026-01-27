-- =====================================================
-- Transform BrokerEO (Errors & Omissions) from new_data to etl staging
-- Maps PartyUniqueId to BrokerId
-- =====================================================

SET NOCOUNT ON;

PRINT 'Transforming BrokerEO Insurances...';

-- Truncate staging table
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_broker_eo_insurances];

-- Insert transformed E&O data
INSERT INTO [$(ETL_SCHEMA)].[stg_broker_eo_insurances] (
    Id, BrokerId, PolicyNumber, Carrier,
    CoverageAmount, MinimumRequired, DeductibleAmount, ClaimMaxAmount,
    AnnualMaxAmount, PolicyMaxAmount, LiabilityLimit,
    EffectiveDate, ExpirationDate, RenewalDate, [Status],
    CreationTime, IsDeleted
)
SELECT 
    neo.Id,
    b.Id AS BrokerId,
    NULLIF(LTRIM(RTRIM(neo.PolicyId)), 'NULL') AS PolicyNumber,
    NULLIF(LTRIM(RTRIM(neo.CarrierName)), 'NULL') AS Carrier,
    -- Coverage amounts: assume 1M if not specified
    1000000 AS CoverageAmount,
    100000 AS MinimumRequired,
    -- Convert string amounts to decimal, handle NULL
    TRY_CAST(NULLIF(LTRIM(RTRIM(neo.DeductibleAmount)), 'NULL') AS DECIMAL(18,2)) AS DeductibleAmount,
    TRY_CAST(NULLIF(LTRIM(RTRIM(neo.ClaimMaxAmount)), 'NULL') AS DECIMAL(18,2)) AS ClaimMaxAmount,
    TRY_CAST(NULLIF(LTRIM(RTRIM(neo.AnnualMaxAmount)), 'NULL') AS DECIMAL(18,2)) AS AnnualMaxAmount,
    TRY_CAST(NULLIF(LTRIM(RTRIM(neo.PolicyMaxAmount)), 'NULL') AS DECIMAL(18,2)) AS PolicyMaxAmount,
    TRY_CAST(NULLIF(LTRIM(RTRIM(neo.LiabilityLimit)), 'NULL') AS DECIMAL(18,2)) AS LiabilityLimit,
    -- Convert date strings to datetime2
    TRY_CAST(NULLIF(LTRIM(RTRIM(neo.FromDate)), '') AS DATETIME2) AS EffectiveDate,
    TRY_CAST(NULLIF(LTRIM(RTRIM(neo.ToDate)), '') AS DATETIME2) AS ExpirationDate,
    -- RenewalDate: assume 1 year after ToDate if available
    CASE 
        WHEN TRY_CAST(NULLIF(LTRIM(RTRIM(neo.ToDate)), '') AS DATETIME2) IS NOT NULL
        THEN DATEADD(YEAR, 1, TRY_CAST(NULLIF(LTRIM(RTRIM(neo.ToDate)), '') AS DATETIME2))
        ELSE NULL
    END AS RenewalDate,
    -- Status: 0=Active (default), assume all active unless expired
    CASE 
        WHEN TRY_CAST(NULLIF(LTRIM(RTRIM(neo.ToDate)), '') AS DATETIME2) < GETUTCDATE() THEN 2  -- Expired
        ELSE 0  -- Active
    END AS [Status],
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [new_data].[BrokerEO] neo
INNER JOIN [$(ETL_SCHEMA)].[stg_brokers] b ON b.ExternalPartyId = neo.PartyUniqueId
WHERE neo.PartyUniqueId IS NOT NULL
  AND neo.PartyUniqueId != ''
  AND neo.PartyUniqueId != 'NULL';

DECLARE @eoCount INT;
SELECT @eoCount = @@ROWCOUNT;
PRINT 'E&O insurances transformed: ' + CAST(@eoCount AS VARCHAR);

DECLARE @totalEO INT;
SELECT @totalEO = COUNT(*) FROM [$(ETL_SCHEMA)].[stg_broker_eo_insurances];
PRINT 'Total E&O insurances in staging: ' + CAST(@totalEO AS VARCHAR);

-- Report on E&O records without matching broker
DECLARE @orphanEO INT;
SELECT @orphanEO = COUNT(*)
FROM [new_data].[BrokerEO] neo
LEFT JOIN [$(ETL_SCHEMA)].[stg_brokers] b ON b.ExternalPartyId = neo.PartyUniqueId
WHERE b.Id IS NULL
  AND neo.PartyUniqueId IS NOT NULL
  AND neo.PartyUniqueId != ''
  AND neo.PartyUniqueId != 'NULL';

PRINT 'E&O insurances without matching broker (skipped): ' + CAST(@orphanEO AS VARCHAR);

GO

PRINT '=== E&O Transform Complete ===';
