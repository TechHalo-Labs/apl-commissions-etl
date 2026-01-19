-- =============================================================================
-- Raw Tables for SQL Server ETL Pipeline
-- =============================================================================
-- These tables hold data directly from CSV files without transformation
-- All columns are NVARCHAR to match CSV import behavior
-- Usage: sqlcmd -S server -d database -i 01-raw-tables.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'CREATING RAW TABLES';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Premium Payments (from premiums.csv)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[raw_premiums];
CREATE TABLE [etl].[raw_premiums] (
    Company NVARCHAR(100),
    GroupNumber NVARCHAR(100),
    Policy NVARCHAR(100),
    OldPolicy NVARCHAR(100),
    LastName NVARCHAR(200),
    FirstName NVARCHAR(200),
    Product NVARCHAR(100),
    MasterCategory NVARCHAR(100),
    Category NVARCHAR(100),
    PayMode NVARCHAR(50),
    StateIssued NVARCHAR(10),
    Division NVARCHAR(100),
    CertificateEffectiveDate NVARCHAR(50),
    DatePost NVARCHAR(50),
    DatePaidTo NVARCHAR(50),
    Amount NVARCHAR(50),
    TransactionType NVARCHAR(100),
    InvoiceNumber NVARCHAR(100),
    CommissionType NVARCHAR(100),
    GroupName NVARCHAR(500),
    SplitPercentage NVARCHAR(50),
    SplitCommissionHierarchy NVARCHAR(500),
    SplitSalesHierarchy NVARCHAR(500),
    LionRecNo NVARCHAR(50)
);
PRINT 'Created [etl].[raw_premiums]';

-- Create index for common lookups
CREATE NONCLUSTERED INDEX IX_raw_premiums_GroupNumber_Policy 
ON [etl].[raw_premiums] (GroupNumber, Policy);

-- =============================================================================
-- Commission Details (from CommissionsDetail*.csv files)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[raw_commissions_detail];
CREATE TABLE [etl].[raw_commissions_detail] (
    Company NVARCHAR(100),
    CertificateId NVARCHAR(100),
    CertEffectiveDate NVARCHAR(50),
    SplitBrokerId NVARCHAR(50),
    PmtPostedDate NVARCHAR(50),
    PaidToDate NVARCHAR(50),
    PaidAmount NVARCHAR(50),
    TransActionType NVARCHAR(100),
    InvoiceNumber NVARCHAR(100),
    CertInForceMonths NVARCHAR(50),
    CommissionRate NVARCHAR(50),
    RealCommissionRate NVARCHAR(50),
    PaidBrokerId NVARCHAR(50),
    CreaditCardType NVARCHAR(100),
    TransactionId NVARCHAR(100)
);
PRINT 'Created [etl].[raw_commissions_detail]';

CREATE NONCLUSTERED INDEX IX_raw_commissions_detail_CertificateId 
ON [etl].[raw_commissions_detail] (CertificateId);

CREATE NONCLUSTERED INDEX IX_raw_commissions_detail_SplitBrokerId 
ON [etl].[raw_commissions_detail] (SplitBrokerId);

-- =============================================================================
-- Certificate Info (from CertificateInfo.csv)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[raw_certificate_info];
CREATE TABLE [etl].[raw_certificate_info] (
    Company NVARCHAR(100),
    ProductMasterCategory NVARCHAR(100),
    ProductCategory NVARCHAR(100),
    GroupId NVARCHAR(100),
    Product NVARCHAR(100),
    PlanCode NVARCHAR(100),
    CertificateId NVARCHAR(100),
    CertEffectiveDate NVARCHAR(50),
    CertIssuedState NVARCHAR(10),
    CertStatus NVARCHAR(10),
    CertPremium NVARCHAR(50),
    CertSplitSeq NVARCHAR(50),
    CertSplitPercent NVARCHAR(50),
    CustomerId NVARCHAR(100),
    RecStatus NVARCHAR(10),
    HierDriver NVARCHAR(100),
    HierVersion NVARCHAR(100),
    CommissionsSchedule NVARCHAR(100),
    CommissionType NVARCHAR(100),
    WritingBrokerID NVARCHAR(50),
    SplitBrokerId NVARCHAR(50),
    SplitBrokerSeq NVARCHAR(50),
    ReassignedType NVARCHAR(100),
    PaidBrokerId NVARCHAR(50)
);
PRINT 'Created [etl].[raw_certificate_info]';

CREATE NONCLUSTERED INDEX IX_raw_certificate_info_CertificateId 
ON [etl].[raw_certificate_info] (CertificateId);

CREATE NONCLUSTERED INDEX IX_raw_certificate_info_GroupId 
ON [etl].[raw_certificate_info] (GroupId);

-- =============================================================================
-- Individual Brokers (from individual-roster.csv)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[raw_individual_brokers];
CREATE TABLE [etl].[raw_individual_brokers] (
    PartyUniqueId NVARCHAR(50),
    IndividualLastName NVARCHAR(200),
    IndividualFirstName NVARCHAR(200),
    HireDate NVARCHAR(50),
    EmailAddress NVARCHAR(500),
    CurrentStatus NVARCHAR(50),
    BrokerType NVARCHAR(100)
);
PRINT 'Created [etl].[raw_individual_brokers]';

CREATE NONCLUSTERED INDEX IX_raw_individual_brokers_PartyUniqueId 
ON [etl].[raw_individual_brokers] (PartyUniqueId);

-- =============================================================================
-- Organization Brokers (from org.csv)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[raw_org_brokers];
CREATE TABLE [etl].[raw_org_brokers] (
    PartyUniqueId NVARCHAR(50),
    OrganizationName NVARCHAR(500),
    HireDate NVARCHAR(50),
    EmailAddress NVARCHAR(500),
    CurrentStatus NVARCHAR(50)
);
PRINT 'Created [etl].[raw_org_brokers]';

CREATE NONCLUSTERED INDEX IX_raw_org_brokers_PartyUniqueId 
ON [etl].[raw_org_brokers] (PartyUniqueId);

-- =============================================================================
-- Broker Licenses (from license.csv)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[raw_licenses];
CREATE TABLE [etl].[raw_licenses] (
    PartyUniqueId NVARCHAR(50),
    StateCode NVARCHAR(10),
    CurrentStatus NVARCHAR(50),
    LicenseCode NVARCHAR(100),
    LicenseEffectiveDate NVARCHAR(50),
    LicenseExpirationDate NVARCHAR(50),
    IsResidenceLicense NVARCHAR(10),
    LicenseNumber NVARCHAR(100),
    ApplicableCounty NVARCHAR(200)
);
PRINT 'Created [etl].[raw_licenses]';

CREATE NONCLUSTERED INDEX IX_raw_licenses_PartyUniqueId 
ON [etl].[raw_licenses] (PartyUniqueId);

-- =============================================================================
-- E&O Insurance (from eo.csv)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[raw_eo_insurance];
CREATE TABLE [etl].[raw_eo_insurance] (
    PartyUniqueId NVARCHAR(50),
    CarrierName NVARCHAR(500),
    PolicyId NVARCHAR(100),
    FromDate NVARCHAR(50),
    ToDate NVARCHAR(50),
    DeductibleAmount NVARCHAR(50),
    ClaimMaxAmount NVARCHAR(50),
    AnnualMaxAmount NVARCHAR(50),
    PolicyMaxAmount NVARCHAR(50),
    LiabilityLimit NVARCHAR(50)
);
PRINT 'Created [etl].[raw_eo_insurance]';

CREATE NONCLUSTERED INDEX IX_raw_eo_insurance_PartyUniqueId 
ON [etl].[raw_eo_insurance] (PartyUniqueId);

-- =============================================================================
-- Schedule Rates (from perf.csv)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[raw_schedule_rates];
CREATE TABLE [etl].[raw_schedule_rates] (
    ScheduleName NVARCHAR(200),
    ScheduleDescription NVARCHAR(1000),
    Category NVARCHAR(100),
    ProductCode NVARCHAR(100),
    OffGroupLetterDescription NVARCHAR(500),
    [State] NVARCHAR(10),
    GroupSizeFrom NVARCHAR(50),
    GroupSizeTo NVARCHAR(50),
    GroupSize NVARCHAR(100),
    EffectiveStartDate NVARCHAR(50),
    EffectiveEndDate NVARCHAR(50),
    SeriesType NVARCHAR(100),
    SpecialOffer NVARCHAR(200),
    Year1 NVARCHAR(50),
    Year2 NVARCHAR(50),
    Year3 NVARCHAR(50),
    Year4 NVARCHAR(50),
    Year5 NVARCHAR(50),
    Year6 NVARCHAR(50),
    Year7 NVARCHAR(50),
    Year8 NVARCHAR(50),
    Year9 NVARCHAR(50),
    Year10 NVARCHAR(50),
    Year11 NVARCHAR(50),
    Year12 NVARCHAR(50),
    Year13 NVARCHAR(50),
    Year14 NVARCHAR(50),
    Year15 NVARCHAR(50),
    Year16 NVARCHAR(50),
    Year66 NVARCHAR(50),
    Year99 NVARCHAR(50),
    [Level] NVARCHAR(50)
);
PRINT 'Created [etl].[raw_schedule_rates]';

CREATE NONCLUSTERED INDEX IX_raw_schedule_rates_ScheduleName_ProductCode 
ON [etl].[raw_schedule_rates] (ScheduleName, ProductCode);

-- =============================================================================
-- Fees (from Fees_20260107.csv)
-- Column names match the CSV headers exactly
-- =============================================================================
DROP TABLE IF EXISTS [etl].[raw_fees];
CREATE TABLE [etl].[raw_fees] (
    PRDNUM NVARCHAR(100),                    -- Group number
    PRODUCTCAT NVARCHAR(100),                -- Product category
    FREQ NVARCHAR(50),                       -- Frequency
    AMOUNT NVARCHAR(50),                     -- Raw amount
    FormattedAmount NVARCHAR(50),            -- Decimal amount (e.g., 0.030)
    AMTKIND NVARCHAR(20),                    -- Amount kind (PCT, FLAT, etc.)
    DATEEND NVARCHAR(50),                    -- End date
    DATESTART NVARCHAR(50),                  -- Start date
    MAINT NVARCHAR(10),                      -- Maintenance flag
    FEECALCMETHOD NVARCHAR(100),             -- Fee calc method code
    FormattedFeeCalcMethod NVARCHAR(500),    -- Fee calc method description
    PERSISTPERIOD NVARCHAR(50),              -- Persist period
    RECNOFEEPERSON NVARCHAR(50),             -- Record number
    FEETYPE NVARCHAR(200),                   -- Legacy fee type
    PartyUniqueId NVARCHAR(50)               -- Broker external ID (e.g., P19690)
);
PRINT 'Created [etl].[raw_fees]';

CREATE NONCLUSTERED INDEX IX_raw_fees_PRDNUM 
ON [etl].[raw_fees] (PRDNUM);

CREATE NONCLUSTERED INDEX IX_raw_fees_PartyUniqueId 
ON [etl].[raw_fees] (PartyUniqueId);

-- =============================================================================
-- Performance Groups (from perf-group.csv) - Group master data with names
-- =============================================================================
DROP TABLE IF EXISTS [etl].[raw_perf_groups];
CREATE TABLE [etl].[raw_perf_groups] (
    GroupNum NVARCHAR(100),
    GroupName NVARCHAR(500),
    StateAbbreviation NVARCHAR(10),
    GroupSize NVARCHAR(50),
    BrokerUniqueId NVARCHAR(50),
    BrokerReportsToUniqueId NVARCHAR(50),
    AgencyName NVARCHAR(500),
    Product NVARCHAR(100),
    EffectiveDate NVARCHAR(50),
    FundingType NVARCHAR(100),
    CommissionTable NVARCHAR(200),
    FeeAgreement NVARCHAR(200)
);
PRINT 'Created [etl].[raw_perf_groups]';

CREATE NONCLUSTERED INDEX IX_raw_perf_groups_GroupNum 
ON [etl].[raw_perf_groups] (GroupNum);

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT 'Raw tables created:';
SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('etl') AND name LIKE 'raw_%' ORDER BY name;

PRINT '';
PRINT '============================================================';
PRINT 'RAW TABLES CREATED SUCCESSFULLY';
PRINT '============================================================';

GO

