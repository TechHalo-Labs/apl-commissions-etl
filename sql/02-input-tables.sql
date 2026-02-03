-- ============================================================================
-- 02-input-tables.sql (SQL Server)
-- Prep, Input, and Nonconformant Tables with Proper Types
-- ============================================================================
-- This file creates three sets of tables:
--   1. prep_* tables: Type-casted data from raw_* tables (before conformance)
--   2. input_* tables: Conformant data only (populated by conformance analysis)
--   3. nonconformant_* tables: Quarantined data (with reason codes)
-- ============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'CREATING INPUT TABLES';
PRINT '============================================================';
PRINT '';

-- ============================================================================
-- Step 1: Drop all existing tables for idempotent re-runs
-- ============================================================================
DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[input_commission_details];
DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[input_certificate_info];
DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[prep_commission_details];
DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[prep_certificate_info];
DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[nonconformant_commission_details];
DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[nonconformant_certificate_info];

-- ============================================================================
-- Step 2: Create prep_certificate_info (type-cast staging)
-- ============================================================================
CREATE TABLE [$(ETL_SCHEMA)].[prep_certificate_info] (
    Company NVARCHAR(100),
    ProductMasterCategory NVARCHAR(100),
    ProductCategory NVARCHAR(100),
    GroupId NVARCHAR(100),
    Product NVARCHAR(100),
    PlanCode NVARCHAR(100),
    CertificateId BIGINT,
    CertEffectiveDate DATE,
    CertIssuedState CHAR(2),
    CertStatus NVARCHAR(10),
    CertPremium DECIMAL(18,2),
    CertSplitSeq INT,
    CertSplitPercent DECIMAL(18,2),
    CustomerId NVARCHAR(100),
    RecStatus NVARCHAR(10),
    HierDriver NVARCHAR(100),
    HierVersion NVARCHAR(100),
    CommissionsSchedule NVARCHAR(100),
    CommissionType NVARCHAR(100),
    WritingBrokerID NVARCHAR(50),
    SplitBrokerId NVARCHAR(50),
    SplitBrokerSeq INT,
    ReassignedType NVARCHAR(100),
    PaidBrokerId NVARCHAR(50)
);
PRINT 'Created [$(ETL_SCHEMA)].[prep_certificate_info]';

CREATE NONCLUSTERED INDEX IX_prep_cert_CertificateId 
ON [$(ETL_SCHEMA)].[prep_certificate_info] (CertificateId);

-- ============================================================================
-- Step 3: Create prep_commission_details (type-cast staging)
-- ============================================================================
CREATE TABLE [$(ETL_SCHEMA)].[prep_commission_details] (
    Company NVARCHAR(100),
    CertificateId BIGINT,
    CertEffectiveDate DATE,
    SplitBrokerId NVARCHAR(50),
    PmtPostedDate DATE,
    PaidToDate DATE,
    PaidAmount DECIMAL(18,2),
    TransActionType NVARCHAR(100),
    InvoiceNumber NVARCHAR(100),
    CertInForceMonths INT,
    CommissionRate DECIMAL(18,4),
    RealCommissionRate DECIMAL(18,4),
    PaidBrokerId NVARCHAR(50),
    CommissionsType NVARCHAR(100),
    CommissionsSchedule NVARCHAR(100),
    CommissionsDriver NVARCHAR(100),
    CommissionsLevel INT,
    CreditCardType NVARCHAR(100),
    TransactionId NVARCHAR(100)
);
PRINT 'Created [$(ETL_SCHEMA)].[prep_commission_details]';

CREATE NONCLUSTERED INDEX IX_prep_cd_CertificateId 
ON [$(ETL_SCHEMA)].[prep_commission_details] (CertificateId);

-- ============================================================================
-- Step 4: Create input_certificate_info (conformant records only)
-- ============================================================================
CREATE TABLE [$(ETL_SCHEMA)].[input_certificate_info] (
    Company NVARCHAR(100),
    ProductMasterCategory NVARCHAR(100),
    ProductCategory NVARCHAR(100),
    GroupId NVARCHAR(100),
    Product NVARCHAR(100),
    PlanCode NVARCHAR(100),
    CertificateId BIGINT NOT NULL,
    CertEffectiveDate DATE NOT NULL,
    CertIssuedState CHAR(2),
    CertStatus NVARCHAR(10),
    CertPremium DECIMAL(18,2),
    CertSplitSeq INT NOT NULL,
    CertSplitPercent DECIMAL(18,2),
    CustomerId NVARCHAR(100),
    RecStatus NVARCHAR(10),
    HierDriver NVARCHAR(100),
    HierVersion NVARCHAR(100),
    CommissionsSchedule NVARCHAR(100),
    CommissionType NVARCHAR(100),
    WritingBrokerID NVARCHAR(50),
    SplitBrokerId NVARCHAR(50),
    SplitBrokerSeq INT NOT NULL,
    ReassignedType NVARCHAR(100),
    PaidBrokerId NVARCHAR(50)
);
PRINT 'Created [$(ETL_SCHEMA)].[input_certificate_info]';

CREATE NONCLUSTERED INDEX IX_input_cert_CertificateId 
ON [$(ETL_SCHEMA)].[input_certificate_info] (CertificateId, CertSplitSeq, SplitBrokerSeq);

CREATE NONCLUSTERED INDEX IX_input_cert_GroupId 
ON [$(ETL_SCHEMA)].[input_certificate_info] (GroupId);

-- ============================================================================
-- Step 5: Create input_commission_details (conformant records only)
-- ============================================================================
CREATE TABLE [$(ETL_SCHEMA)].[input_commission_details] (
    Company NVARCHAR(100),
    CertificateId BIGINT NOT NULL,
    CertEffectiveDate DATE NOT NULL,
    SplitBrokerId NVARCHAR(50) NOT NULL,
    PmtPostedDate DATE NOT NULL,
    PaidToDate DATE,
    PaidAmount DECIMAL(18,2) NOT NULL,
    TransActionType NVARCHAR(100),
    InvoiceNumber NVARCHAR(100),
    CertInForceMonths INT,
    CommissionRate DECIMAL(18,4),
    RealCommissionRate DECIMAL(18,4),
    PaidBrokerId NVARCHAR(50),
    CommissionsType NVARCHAR(100),
    CommissionsSchedule NVARCHAR(100),
    CommissionsDriver NVARCHAR(100),
    CommissionsLevel INT,
    CreditCardType NVARCHAR(100),
    TransactionId NVARCHAR(100)
);
PRINT 'Created [$(ETL_SCHEMA)].[input_commission_details]';

CREATE NONCLUSTERED INDEX IX_input_cd_CertificateId 
ON [$(ETL_SCHEMA)].[input_commission_details] (CertificateId, SplitBrokerId);

CREATE NONCLUSTERED INDEX IX_input_cd_PmtPostedDate 
ON [$(ETL_SCHEMA)].[input_commission_details] (PmtPostedDate);

-- ============================================================================
-- Step 6: Create nonconformant_certificate_info (quarantined records)
-- ============================================================================
CREATE TABLE [$(ETL_SCHEMA)].[nonconformant_certificate_info] (
    Company NVARCHAR(100),
    ProductMasterCategory NVARCHAR(100),
    ProductCategory NVARCHAR(100),
    GroupId NVARCHAR(100),
    Product NVARCHAR(100),
    PlanCode NVARCHAR(100),
    CertificateId BIGINT,
    CertEffectiveDate DATE,
    CertIssuedState CHAR(2),
    CertStatus NVARCHAR(10),
    CertPremium DECIMAL(18,2),
    CertSplitSeq INT,
    CertSplitPercent DECIMAL(18,2),
    CustomerId NVARCHAR(100),
    RecStatus NVARCHAR(10),
    HierDriver NVARCHAR(100),
    HierVersion NVARCHAR(100),
    CommissionsSchedule NVARCHAR(100),
    CommissionType NVARCHAR(100),
    WritingBrokerID NVARCHAR(50),
    SplitBrokerId NVARCHAR(50),
    SplitBrokerSeq INT,
    ReassignedType NVARCHAR(100),
    PaidBrokerId NVARCHAR(50),
    -- Additional conformance tracking columns
    nonconformant_code NVARCHAR(10),
    nonconformant_description NVARCHAR(2000)
);
PRINT 'Created [$(ETL_SCHEMA)].[nonconformant_certificate_info]';

CREATE NONCLUSTERED INDEX IX_nonconf_cert_code 
ON [$(ETL_SCHEMA)].[nonconformant_certificate_info] (nonconformant_code);

-- ============================================================================
-- Step 7: Create nonconformant_commission_details (quarantined records)
-- ============================================================================
CREATE TABLE [$(ETL_SCHEMA)].[nonconformant_commission_details] (
    Company NVARCHAR(100),
    CertificateId BIGINT,
    CertEffectiveDate DATE,
    SplitBrokerId NVARCHAR(50),
    PmtPostedDate DATE,
    PaidToDate DATE,
    PaidAmount DECIMAL(18,2),
    TransActionType NVARCHAR(100),
    InvoiceNumber NVARCHAR(100),
    CertInForceMonths INT,
    CommissionRate DECIMAL(18,4),
    RealCommissionRate DECIMAL(18,4),
    PaidBrokerId NVARCHAR(50),
    CommissionsType NVARCHAR(100),
    CommissionsSchedule NVARCHAR(100),
    CommissionsDriver NVARCHAR(100),
    CommissionsLevel INT,
    CreditCardType NVARCHAR(100),
    TransactionId NVARCHAR(100),
    -- Additional conformance tracking columns
    nonconformant_code NVARCHAR(10),
    nonconformant_description NVARCHAR(2000)
);
PRINT 'Created [$(ETL_SCHEMA)].[nonconformant_commission_details]';

CREATE NONCLUSTERED INDEX IX_nonconf_cd_code 
ON [$(ETL_SCHEMA)].[nonconformant_commission_details] (nonconformant_code);

GO

-- ============================================================================
-- Step 8: Populate prep_certificate_info from raw_certificate_info
-- ============================================================================
PRINT '';
PRINT 'Populating prep_certificate_info...';

INSERT INTO [$(ETL_SCHEMA)].[prep_certificate_info]
SELECT
    LTRIM(RTRIM(r.Company)) AS Company,
    LTRIM(RTRIM(r.ProductMasterCategory)) AS ProductMasterCategory,
    LTRIM(RTRIM(r.ProductCategory)) AS ProductCategory,
    LTRIM(RTRIM(r.GroupId)) AS GroupId,
    LTRIM(RTRIM(r.Product)) AS Product,
    LTRIM(RTRIM(r.PlanCode)) AS PlanCode,
    TRY_CAST(NULLIF(LTRIM(RTRIM(r.CertificateId)), '') AS BIGINT) AS CertificateId,
    TRY_CONVERT(DATE, NULLIF(LTRIM(RTRIM(r.CertEffectiveDate)), '')) AS CertEffectiveDate,
    LEFT(LTRIM(RTRIM(r.CertIssuedState)), 2) AS CertIssuedState,
    LTRIM(RTRIM(r.CertStatus)) AS CertStatus,
    TRY_CAST(NULLIF(LTRIM(RTRIM(r.CertPremium)), '') AS DECIMAL(18,2)) AS CertPremium,
    TRY_CAST(NULLIF(LTRIM(RTRIM(r.CertSplitSeq)), '') AS INT) AS CertSplitSeq,
    TRY_CAST(NULLIF(LTRIM(RTRIM(r.CertSplitPercent)), '') AS DECIMAL(18,2)) AS CertSplitPercent,
    LTRIM(RTRIM(r.CustomerId)) AS CustomerId,
    LTRIM(RTRIM(r.RecStatus)) AS RecStatus,
    LTRIM(RTRIM(r.HierDriver)) AS HierDriver,
    LTRIM(RTRIM(r.HierVersion)) AS HierVersion,
    LTRIM(RTRIM(r.CommissionsSchedule)) AS CommissionsSchedule,
    LTRIM(RTRIM(r.CommissionType)) AS CommissionType,
    LTRIM(RTRIM(r.WritingBrokerID)) AS WritingBrokerID,
    LTRIM(RTRIM(r.SplitBrokerId)) AS SplitBrokerId,
    TRY_CAST(NULLIF(LTRIM(RTRIM(r.SplitBrokerSeq)), '') AS INT) AS SplitBrokerSeq,
    LTRIM(RTRIM(r.ReassignedType)) AS ReassignedType,
    LTRIM(RTRIM(r.PaidBrokerId)) AS PaidBrokerId
FROM [$(ETL_SCHEMA)].[raw_certificate_info] r
WHERE LTRIM(RTRIM(r.CertificateId)) <> ''
  AND LTRIM(RTRIM(r.CertEffectiveDate)) <> ''
  AND LTRIM(RTRIM(r.RecStatus)) = 'A'
  AND LTRIM(RTRIM(r.CertStatus)) = 'A';

PRINT 'prep_certificate_info populated: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

GO

-- ============================================================================
-- Step 9: Populate prep_commission_details from raw_commissions_detail
-- ============================================================================
PRINT '';
PRINT 'Populating prep_commission_details...';

INSERT INTO [$(ETL_SCHEMA)].[prep_commission_details]
SELECT
    LTRIM(RTRIM(r.Company)) AS Company,
    TRY_CAST(NULLIF(LTRIM(RTRIM(r.CertificateId)), '') AS BIGINT) AS CertificateId,
    TRY_CONVERT(DATE, NULLIF(LTRIM(RTRIM(r.CertEffectiveDate)), '')) AS CertEffectiveDate,
    LTRIM(RTRIM(r.SplitBrokerId)) AS SplitBrokerId,
    TRY_CONVERT(DATE, NULLIF(LTRIM(RTRIM(r.PmtPostedDate)), '')) AS PmtPostedDate,
    TRY_CONVERT(DATE, NULLIF(LTRIM(RTRIM(r.PaidToDate)), '')) AS PaidToDate,
    TRY_CAST(NULLIF(LTRIM(RTRIM(r.PaidAmount)), '') AS DECIMAL(18,2)) AS PaidAmount,
    LTRIM(RTRIM(r.TransActionType)) AS TransActionType,
    LTRIM(RTRIM(r.InvoiceNumber)) AS InvoiceNumber,
    TRY_CAST(NULLIF(LTRIM(RTRIM(r.CertInForceMonths)), '') AS INT) AS CertInForceMonths,
    TRY_CAST(NULLIF(LTRIM(RTRIM(r.CommissionRate)), '') AS DECIMAL(18,4)) AS CommissionRate,
    TRY_CAST(NULLIF(LTRIM(RTRIM(r.RealCommissionRate)), '') AS DECIMAL(18,4)) AS RealCommissionRate,
    LTRIM(RTRIM(r.PaidBrokerId)) AS PaidBrokerId,
    COALESCE(LTRIM(RTRIM(ci.CommissionType)), '') AS CommissionsType,
    COALESCE(LTRIM(RTRIM(ci.CommissionsSchedule)), '') AS CommissionsSchedule,
    COALESCE(LTRIM(RTRIM(ci.HierDriver)), '') AS CommissionsDriver,
    TRY_CAST(NULLIF(LTRIM(RTRIM(ci.HierVersion)), '') AS INT) AS CommissionsLevel,
    LTRIM(RTRIM(r.CreditCardType)) AS CreditCardType,
    LTRIM(RTRIM(r.TransactionId)) AS TransactionId
FROM [$(ETL_SCHEMA)].[raw_commissions_detail] r
LEFT JOIN [$(ETL_SCHEMA)].[raw_certificate_info] ci 
    ON LTRIM(RTRIM(r.CertificateId)) = LTRIM(RTRIM(ci.CertificateId))
    AND LTRIM(RTRIM(r.SplitBrokerId)) = LTRIM(RTRIM(ci.SplitBrokerId))
WHERE LTRIM(RTRIM(r.CertificateId)) <> ''
  AND LTRIM(RTRIM(r.PmtPostedDate)) <> ''
  AND LTRIM(RTRIM(r.PaidAmount)) <> '';

PRINT 'prep_commission_details populated: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

GO

-- ============================================================================
-- Step 10: Populate input tables (skip conformance analysis for simplicity)
-- In production, 01a-conformance-analysis.sql would handle this
-- ============================================================================
PRINT '';
PRINT 'Populating input_certificate_info (conformant records)...';

INSERT INTO [$(ETL_SCHEMA)].[input_certificate_info]
SELECT 
    Company, ProductMasterCategory, ProductCategory, GroupId, Product, PlanCode,
    CertificateId, CertEffectiveDate, CertIssuedState, CertStatus, CertPremium,
    CertSplitSeq, CertSplitPercent, CustomerId, RecStatus, HierDriver, HierVersion,
    CommissionsSchedule, CommissionType, WritingBrokerID, SplitBrokerId,
    SplitBrokerSeq, ReassignedType, PaidBrokerId
FROM [$(ETL_SCHEMA)].[prep_certificate_info]
WHERE CertificateId IS NOT NULL
  AND CertEffectiveDate IS NOT NULL
  AND CertSplitSeq IS NOT NULL
  AND SplitBrokerSeq IS NOT NULL;

PRINT 'input_certificate_info populated: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

GO

PRINT '';
PRINT 'Populating input_commission_details (conformant records)...';

INSERT INTO [$(ETL_SCHEMA)].[input_commission_details]
SELECT 
    Company, CertificateId, CertEffectiveDate, SplitBrokerId, PmtPostedDate,
    PaidToDate, PaidAmount, TransActionType, InvoiceNumber, CertInForceMonths,
    CommissionRate, RealCommissionRate, PaidBrokerId, CommissionsType,
    CommissionsSchedule, CommissionsDriver, CommissionsLevel, CreditCardType, TransactionId
FROM [$(ETL_SCHEMA)].[prep_commission_details]
WHERE CertificateId IS NOT NULL
  AND PmtPostedDate IS NOT NULL
  AND PaidAmount IS NOT NULL;

PRINT 'input_commission_details populated: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

GO

-- ============================================================================
-- Step 11: Validation Summary
-- ============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'INPUT TABLE POPULATION SUMMARY';
PRINT '============================================================';

SELECT 'Row Counts' AS Validation,
       (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[raw_commissions_detail]) AS RawCDCount,
       (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[prep_commission_details]) AS PrepCDCount,
       (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[input_commission_details]) AS InputCDCount,
       (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[raw_certificate_info]) AS RawCICount,
       (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[prep_certificate_info]) AS PrepCICount,
       (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[input_certificate_info]) AS InputCICount;

PRINT '';
PRINT '============================================================';
PRINT 'INPUT TABLES CREATED SUCCESSFULLY';
PRINT '============================================================';

GO

