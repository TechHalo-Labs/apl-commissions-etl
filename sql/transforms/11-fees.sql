-- ============================================================================
-- 11-fees.sql (SQL Server)
-- Transform fees from raw_fees to stg_fees (normalized for FeeSchedule export)
-- ============================================================================
-- Normalizes legacy fee data into canonical fee type codes
-- Prepares data for export to FeeSchedules, FeeScheduleVersions, FeeScheduleItems
-- ============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: FEES';
PRINT '============================================================';
PRINT '';

-- ============================================================================
-- Step 1: Create stg_fees table with normalized structure
-- ============================================================================
IF OBJECT_ID('etl.stg_fees', 'U') IS NOT NULL
    DROP TABLE [etl].[stg_fees];

CREATE TABLE [etl].[stg_fees] (
    Id NVARCHAR(100) NOT NULL PRIMARY KEY,
    GroupNumber NVARCHAR(100),
    GroupId NVARCHAR(100),  -- Will be resolved from Groups
    ProductCategory NVARCHAR(100),
    
    -- Canonical fee type fields
    FeeTypeCode NVARCHAR(50),        -- Canonical code (CERT_FEE, FLAT_FEE, etc.)
    FeeTypeName NVARCHAR(200),       -- Canonical name
    
    -- Schedule entry fields
    [Name] NVARCHAR(255),            -- Entry name
    Frequency NVARCHAR(50),          -- monthly, annual, one-time, etc.
    Basis NVARCHAR(50),              -- fixed, percent, certificates, lives
    Amount DECIMAL(18,2),            -- Fixed amount
    [Percent] DECIMAL(5,2),          -- Percentage value (e.g., 3.00 for 3%)
    Notes NVARCHAR(MAX),             -- Calculation method description
    
    -- Dates
    EffectiveDate DATETIME2,
    EndDate DATETIME2,
    
    -- Recipient broker
    RecipientBrokerId BIGINT,
    RecipientBrokerExternalId NVARCHAR(50),
    RecipientBrokerName NVARCHAR(500),
    
    -- Legacy metadata
    LegacyFeeType NVARCHAR(200),
    LegacyCalcMethod NVARCHAR(100),
    LegacyCalcMethodDesc NVARCHAR(500),
    LegacyAmountKind NVARCHAR(20),
    MaintenanceFlag NVARCHAR(10),
    
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0
);

PRINT 'Created [etl].[stg_fees]';

-- Create indexes
CREATE NONCLUSTERED INDEX IX_stg_fees_GroupNumber ON [etl].[stg_fees] (GroupNumber);
CREATE NONCLUSTERED INDEX IX_stg_fees_GroupId ON [etl].[stg_fees] (GroupId);
CREATE NONCLUSTERED INDEX IX_stg_fees_FeeTypeCode ON [etl].[stg_fees] (FeeTypeCode);
CREATE NONCLUSTERED INDEX IX_stg_fees_RecipientBrokerId ON [etl].[stg_fees] (RecipientBrokerId);

GO

-- ============================================================================
-- Step 2: Populate stg_fees from raw_fees with canonical mapping
-- ============================================================================
PRINT '';
PRINT 'Populating stg_fees from raw_fees with canonical mapping...';

INSERT INTO [etl].[stg_fees] (
    Id,
    GroupNumber,
    GroupId,
    ProductCategory,
    FeeTypeCode,
    FeeTypeName,
    [Name],
    Frequency,
    Basis,
    Amount,
    [Percent],
    Notes,
    EffectiveDate,
    EndDate,
    RecipientBrokerId,
    RecipientBrokerExternalId,
    RecipientBrokerName,
    LegacyFeeType,
    LegacyCalcMethod,
    LegacyCalcMethodDesc,
    LegacyAmountKind,
    MaintenanceFlag,
    CreationTime,
    IsDeleted
)
SELECT
    -- Generate unique ID from group + broker + fee calc method + dates + row number
    CONCAT(
        'FEE-',
        LTRIM(RTRIM(r.PRDNUM)), '-',
        LTRIM(RTRIM(r.PartyUniqueId)), '-',
        LEFT(REPLACE(LTRIM(RTRIM(r.FEECALCMETHOD)), ' ', ''), 10), '-',
        CONVERT(VARCHAR(10), TRY_CONVERT(DATE, r.DATESTART, 101), 112), '-',
        ROW_NUMBER() OVER (
            PARTITION BY r.PRDNUM, r.PartyUniqueId, r.FEECALCMETHOD, r.DATESTART
            ORDER BY r.PRODUCTCAT, r.AMOUNT
        )
    ) AS Id,
    
    -- Group number (trimmed)
    LTRIM(RTRIM(r.PRDNUM)) AS GroupNumber,
    
    -- GroupId will be resolved in Step 3
    NULL AS GroupId,
    
    -- Product category
    LTRIM(RTRIM(r.PRODUCTCAT)) AS ProductCategory,
    
    -- ============================================================================
    -- CANONICAL FEE TYPE MAPPING
    -- Maps FormattedFeeCalcMethod to canonical codes
    -- ============================================================================
    CASE LTRIM(RTRIM(r.FormattedFeeCalcMethod))
        WHEN 'Fee per Certificate $ Per Month' THEN 'CERT_FEE'
        WHEN 'Flat Fee $ Per Month' THEN 'FLAT_FEE'
        WHEN 'New Annual Production Fee Per Month' THEN 'PROD_FEE'
        WHEN 'One Time Flat Fee $' THEN 'ONETIME_FEE'
        WHEN 'Percent of Collected Premium Per Month' THEN 'PREM_FEE_M'
        WHEN 'Percent of Collected Premium Per Year' THEN 'PREM_FEE_A'
        ELSE 'OTHER_FEE'
    END AS FeeTypeCode,
    
    -- Canonical fee type name
    CASE LTRIM(RTRIM(r.FormattedFeeCalcMethod))
        WHEN 'Fee per Certificate $ Per Month' THEN 'Certificate Fee'
        WHEN 'Flat Fee $ Per Month' THEN 'Flat Fee'
        WHEN 'New Annual Production Fee Per Month' THEN 'Production Fee'
        WHEN 'One Time Flat Fee $' THEN 'One-Time Fee'
        WHEN 'Percent of Collected Premium Per Month' THEN 'Premium Fee (Monthly)'
        WHEN 'Percent of Collected Premium Per Year' THEN 'Premium Fee (Annual)'
        ELSE 'Other Fee'
    END AS FeeTypeName,
    
    -- Entry name (descriptive)
    CONCAT(
        CASE LTRIM(RTRIM(r.FormattedFeeCalcMethod))
            WHEN 'Fee per Certificate $ Per Month' THEN 'Certificate Fee'
            WHEN 'Flat Fee $ Per Month' THEN 'Flat Fee'
            WHEN 'New Annual Production Fee Per Month' THEN 'Production Fee'
            WHEN 'One Time Flat Fee $' THEN 'One-Time Fee'
            WHEN 'Percent of Collected Premium Per Month' THEN 'Premium Fee'
            WHEN 'Percent of Collected Premium Per Year' THEN 'Annual Premium Fee'
            ELSE 'Fee'
        END,
        ' - ',
        LTRIM(RTRIM(r.PRODUCTCAT))
    ) AS [Name],
    
    -- ============================================================================
    -- FREQUENCY NORMALIZATION
    -- ============================================================================
    CASE LTRIM(RTRIM(r.FormattedFeeCalcMethod))
        WHEN 'Fee per Certificate $ Per Month' THEN 'monthly'
        WHEN 'Flat Fee $ Per Month' THEN 'monthly'
        WHEN 'New Annual Production Fee Per Month' THEN 'monthly'
        WHEN 'One Time Flat Fee $' THEN 'one-time'
        WHEN 'Percent of Collected Premium Per Month' THEN 'monthly'
        WHEN 'Percent of Collected Premium Per Year' THEN 'annual'
        ELSE 'monthly'
    END AS Frequency,
    
    -- ============================================================================
    -- BASIS NORMALIZATION
    -- Maps to: fixed | percent | certificates | lives
    -- ============================================================================
    CASE LTRIM(RTRIM(r.FormattedFeeCalcMethod))
        WHEN 'Fee per Certificate $ Per Month' THEN 'certificates'
        WHEN 'Flat Fee $ Per Month' THEN 'fixed'
        WHEN 'New Annual Production Fee Per Month' THEN 'percent'
        WHEN 'One Time Flat Fee $' THEN 'fixed'
        WHEN 'Percent of Collected Premium Per Month' THEN 'percent'
        WHEN 'Percent of Collected Premium Per Year' THEN 'percent'
        ELSE 'fixed'
    END AS Basis,
    
    -- ============================================================================
    -- AMOUNT/PERCENT NORMALIZATION
    -- ============================================================================
    -- Amount: Only populate for fixed/certificates basis
    CASE 
        WHEN LTRIM(RTRIM(r.FormattedFeeCalcMethod)) IN (
            'Fee per Certificate $ Per Month',
            'Flat Fee $ Per Month',
            'One Time Flat Fee $'
        ) THEN TRY_CAST(NULLIF(LTRIM(RTRIM(r.AMOUNT)), '') AS DECIMAL(18,2))
        ELSE NULL
    END AS Amount,
    
    -- Percent: Only populate for percent basis (convert FormattedAmount to percentage)
    CASE 
        WHEN LTRIM(RTRIM(r.FormattedFeeCalcMethod)) IN (
            'New Annual Production Fee Per Month',
            'Percent of Collected Premium Per Month',
            'Percent of Collected Premium Per Year'
        ) THEN 
            -- FormattedAmount is decimal (e.g., 0.030), multiply by 100 for percentage (3.00)
            TRY_CAST(NULLIF(LTRIM(RTRIM(r.FormattedAmount)), '') AS DECIMAL(5,2)) * 100
        ELSE NULL
    END AS [Percent],
    
    -- Notes: Use FormattedFeeCalcMethod as description
    LTRIM(RTRIM(r.FormattedFeeCalcMethod)) AS Notes,
    
    -- Dates (MM/DD/YYYY format)
    TRY_CONVERT(DATETIME2, r.DATESTART, 101) AS EffectiveDate,
    TRY_CONVERT(DATETIME2, r.DATEEND, 101) AS EndDate,
    
    -- ============================================================================
    -- RECIPIENT BROKER
    -- ============================================================================
    -- Broker ID (extract numeric part from PartyUniqueId like 'P19690')
    CASE 
        WHEN LTRIM(RTRIM(r.PartyUniqueId)) LIKE 'P%' 
        THEN TRY_CAST(SUBSTRING(LTRIM(RTRIM(r.PartyUniqueId)), 2, LEN(r.PartyUniqueId)) AS BIGINT)
        ELSE NULL 
    END AS RecipientBrokerId,
    
    -- Keep original broker external ID
    LTRIM(RTRIM(r.PartyUniqueId)) AS RecipientBrokerExternalId,
    
    -- Broker name will be resolved in Step 3
    NULL AS RecipientBrokerName,
    
    -- ============================================================================
    -- LEGACY FIELDS (for audit/debugging)
    -- ============================================================================
    LTRIM(RTRIM(r.FEETYPE)) AS LegacyFeeType,
    LTRIM(RTRIM(r.FEECALCMETHOD)) AS LegacyCalcMethod,
    LTRIM(RTRIM(r.FormattedFeeCalcMethod)) AS LegacyCalcMethodDesc,
    LTRIM(RTRIM(r.AMTKIND)) AS LegacyAmountKind,
    LTRIM(RTRIM(r.MAINT)) AS MaintenanceFlag,
    
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted

FROM [etl].[raw_fees] r
WHERE LTRIM(RTRIM(r.PRDNUM)) <> ''
  AND LTRIM(RTRIM(r.PartyUniqueId)) <> ''
  AND LTRIM(RTRIM(r.FormattedFeeCalcMethod)) <> '';

DECLARE @feeCount INT = @@ROWCOUNT;
PRINT 'stg_fees populated: ' + CAST(@feeCount AS VARCHAR) + ' rows';

GO

-- ============================================================================
-- Step 3: Resolve GroupId and RecipientBrokerName
-- ============================================================================
PRINT '';
PRINT 'Resolving GroupId from stg_groups...';

UPDATE f
SET f.GroupId = g.Id
FROM [etl].[stg_fees] f
INNER JOIN [etl].[stg_groups] g ON g.Code = f.GroupNumber;

DECLARE @groupResolveCount INT = @@ROWCOUNT;
PRINT 'Resolved GroupId for ' + CAST(@groupResolveCount AS VARCHAR) + ' fees';

PRINT '';
PRINT 'Resolving RecipientBrokerName from stg_brokers...';

UPDATE f
SET f.RecipientBrokerName = b.Name
FROM [etl].[stg_fees] f
INNER JOIN [etl].[stg_brokers] b ON b.Id = f.RecipientBrokerId;

DECLARE @brokerResolveCount INT = @@ROWCOUNT;
PRINT 'Resolved RecipientBrokerName for ' + CAST(@brokerResolveCount AS VARCHAR) + ' fees';

GO

-- ============================================================================
-- Step 4: Summary Statistics
-- ============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'FEES TRANSFORM SUMMARY';
PRINT '============================================================';
PRINT '';

-- Count by canonical fee type code
SELECT 
    FeeTypeCode,
    FeeTypeName,
    Frequency,
    Basis,
    COUNT(*) AS FeeCount,
    COUNT(DISTINCT GroupId) AS UniqueGroups,
    COUNT(DISTINCT RecipientBrokerId) AS UniqueBrokers,
    AVG(CASE WHEN Amount IS NOT NULL THEN Amount ELSE 0 END) AS AvgAmount,
    AVG(CASE WHEN [Percent] IS NOT NULL THEN [Percent] ELSE 0 END) AS AvgPercent
FROM [etl].[stg_fees]
GROUP BY FeeTypeCode, FeeTypeName, Frequency, Basis
ORDER BY FeeCount DESC;

PRINT '';

-- Count by legacy calculation method (for verification)
SELECT 
    LegacyCalcMethodDesc,
    FeeTypeCode,
    COUNT(*) AS FeeCount
FROM [etl].[stg_fees]
GROUP BY LegacyCalcMethodDesc, FeeTypeCode
ORDER BY FeeCount DESC;

PRINT '';

-- Count fees with resolved GroupId
SELECT 
    'Fees with resolved GroupId' AS Metric,
    COUNT(*) AS [Count]
FROM [etl].[stg_fees]
WHERE GroupId IS NOT NULL

UNION ALL

SELECT 
    'Fees without GroupId' AS Metric,
    COUNT(*) AS [Count]
FROM [etl].[stg_fees]
WHERE GroupId IS NULL

UNION ALL

SELECT 
    'Fees with recipient broker' AS Metric,
    COUNT(*) AS [Count]
FROM [etl].[stg_fees]
WHERE RecipientBrokerId IS NOT NULL;

PRINT '';
PRINT '============================================================';
PRINT 'FEES TRANSFORM COMPLETE';
PRINT '============================================================';

GO

