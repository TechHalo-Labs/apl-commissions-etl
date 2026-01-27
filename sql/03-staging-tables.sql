-- =============================================================================
-- Staging Tables for SQL Server ETL Pipeline
-- =============================================================================
-- These tables mirror the production schema structure for export
-- No foreign key constraints to allow flexible ETL operations
-- Usage: sqlcmd -S server -d database -i 03-staging-tables.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'CREATING STAGING TABLES';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Brokers
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_brokers];
CREATE TABLE [etl].[stg_brokers] (
    Id BIGINT NOT NULL,
    ExternalPartyId NVARCHAR(100),
    Name NVARCHAR(500),
    FirstName NVARCHAR(200),
    LastName NVARCHAR(200),
    MiddleName NVARCHAR(100),
    Suffix NVARCHAR(50),
    [Type] NVARCHAR(50),
    [Status] NVARCHAR(50),
    Email NVARCHAR(500),
    Phone NVARCHAR(50),
    Npn NVARCHAR(50),
    TaxId NVARCHAR(50),
    DateOfBirth DATE,
    AppointmentDate DATE,
    HireDate DATE,
    BrokerClassification NVARCHAR(100),
    HierarchyLevel NVARCHAR(50),
    UplineId BIGINT,
    UplineName NVARCHAR(500),
    DownlineCount INT DEFAULT 0,
    AddressLine1 NVARCHAR(500),
    AddressLine2 NVARCHAR(500),
    City NVARCHAR(200),
    [State] NVARCHAR(10),
    ZipCode NVARCHAR(20),
    Country NVARCHAR(100),
    PrimaryContactName NVARCHAR(500),
    PrimaryContactRole NVARCHAR(200),
    DateContracted DATE,
    Ssn NVARCHAR(50),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_brokers PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_brokers]';

-- =============================================================================
-- Broker Licenses
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_broker_licenses];
CREATE TABLE [etl].[stg_broker_licenses] (
    Id BIGINT NOT NULL,
    BrokerId BIGINT,
    [State] NVARCHAR(10),
    LicenseNumber NVARCHAR(100),
    [Type] INT DEFAULT 0,
    [Status] INT DEFAULT 0,
    EffectiveDate DATETIME2,
    ExpirationDate DATETIME2,
    LicenseCode NVARCHAR(100),
    IsResidentLicense BIT DEFAULT 0,
    ApplicableCounty NVARCHAR(200),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_broker_licenses PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_broker_licenses]';

-- =============================================================================
-- Broker E&O Insurance
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_broker_eo_insurances];
CREATE TABLE [etl].[stg_broker_eo_insurances] (
    Id BIGINT NOT NULL,
    BrokerId BIGINT,
    PolicyNumber NVARCHAR(100),
    Carrier NVARCHAR(500),
    CoverageAmount DECIMAL(18,2),
    MinimumRequired DECIMAL(18,2),
    DeductibleAmount DECIMAL(18,2),
    ClaimMaxAmount DECIMAL(18,2),
    AnnualMaxAmount DECIMAL(18,2),
    PolicyMaxAmount DECIMAL(18,2),
    LiabilityLimit DECIMAL(18,2),
    EffectiveDate DATETIME2,
    ExpirationDate DATETIME2,
    RenewalDate DATETIME2,
    [Status] INT DEFAULT 0,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_broker_eo_insurances PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_broker_eo_insurances]';

-- =============================================================================
-- Products
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_products];
CREATE TABLE [etl].[stg_products] (
    Id NVARCHAR(100) NOT NULL,
    ProductCode NVARCHAR(100) NOT NULL,
    ProductName NVARCHAR(500) NOT NULL,
    MasterCategory NVARCHAR(100),
    Category NVARCHAR(100),
    OffGroupLetterDescription NVARCHAR(500),
    CommissionType NVARCHAR(100),
    DefaultCommissionTable NVARCHAR(200),
    IsActive BIT DEFAULT 1,
    IsArchived BIT DEFAULT 0,
    SeriesType NVARCHAR(100),
    SpecialOffer NVARCHAR(200),
    [Description] NVARCHAR(2000),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_products PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_products]';

-- =============================================================================
-- Product Codes - Individual codes linked to their parent category
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_product_codes];
CREATE TABLE [etl].[stg_product_codes] (
    Id BIGINT NOT NULL,
    ProductId NVARCHAR(100) NOT NULL,
    Code NVARCHAR(100) NOT NULL,
    [Description] NVARCHAR(500),
    AllowedStates NVARCHAR(MAX) DEFAULT '',
    [Status] NVARCHAR(50) DEFAULT 'Active',
    GroupsCount INT DEFAULT 0,
    SchedulesCount INT DEFAULT 0,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_product_codes PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_product_codes]';

CREATE NONCLUSTERED INDEX IX_stg_product_codes_ProductId ON [etl].[stg_product_codes] (ProductId);
CREATE NONCLUSTERED INDEX IX_stg_product_codes_Code ON [etl].[stg_product_codes] (Code);

-- =============================================================================
-- Plans - Unique plan codes linked to their parent product category
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_plans];
CREATE TABLE [etl].[stg_plans] (
    Id NVARCHAR(100) NOT NULL,
    ProductId NVARCHAR(100) NOT NULL,
    PlanCode NVARCHAR(100) NOT NULL,
    Name NVARCHAR(500) NOT NULL,
    [Description] NVARCHAR(2000),
    [Status] INT DEFAULT 1,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_plans PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_plans]';

CREATE NONCLUSTERED INDEX IX_stg_plans_ProductId ON [etl].[stg_plans] (ProductId);
CREATE NONCLUSTERED INDEX IX_stg_plans_PlanCode ON [etl].[stg_plans] (PlanCode);

-- =============================================================================
-- Groups (employer groups with policies)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_groups];
CREATE TABLE [etl].[stg_groups] (
    Id NVARCHAR(100) NOT NULL,
    Name NVARCHAR(500),
    [Description] NVARCHAR(2000),
    Code NVARCHAR(100),
    IsActive BIT DEFAULT 1,
    AddressLine1 NVARCHAR(500),
    AddressLine2 NVARCHAR(500),
    City NVARCHAR(200),
    [State] NVARCHAR(10),
    ZipCode NVARCHAR(20),
    Country NVARCHAR(100),
    Phone NVARCHAR(50),
    TaxId NVARCHAR(50),
    [Status] INT DEFAULT 0,
    [Type] INT DEFAULT 0,
    IsNonConformant BIT DEFAULT 0,
    NonConformantDescription NVARCHAR(2000),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_groups PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_groups]';

-- =============================================================================
-- Schedules
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_schedules];
CREATE TABLE [etl].[stg_schedules] (
    Id BIGINT NOT NULL,
    ExternalId NVARCHAR(200),
    Name NVARCHAR(500),
    [Description] NVARCHAR(2000),
    [Status] NVARCHAR(50),
    CommissionType NVARCHAR(100),
    RateStructure NVARCHAR(100),
    EffectiveDate DATE,
    EndDate DATE,
    ProductLines NVARCHAR(MAX),  -- JSON array
    ProductCodes NVARCHAR(MAX),  -- JSON array
    [Owner] NVARCHAR(200),
    ContractCount INT DEFAULT 0,
    ProductCount INT DEFAULT 0,
    CurrentVersionId BIGINT,
    CurrentVersionNumber NVARCHAR(50),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_schedules PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_schedules]';

-- =============================================================================
-- Schedule Versions
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_schedule_versions];
CREATE TABLE [etl].[stg_schedule_versions] (
    Id BIGINT NOT NULL,
    ScheduleId BIGINT NOT NULL,
    VersionNumber NVARCHAR(50) DEFAULT '1.0',
    [Status] INT DEFAULT 1,
    EffectiveDate DATE,
    EndDate DATE,
    ChangeReason NVARCHAR(2000),
    ApprovedBy NVARCHAR(200),
    ApprovedAt DATETIME2,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_schedule_versions PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_schedule_versions]';

-- =============================================================================
-- Schedule Rates
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_schedule_rates];
CREATE TABLE [etl].[stg_schedule_rates] (
    Id BIGINT NOT NULL,
    ScheduleVersionId BIGINT,
    CoverageType NVARCHAR(100),
    ProductCode NVARCHAR(100),
    ProductName NVARCHAR(500),
    RateValue DECIMAL(18,4),
    FirstYearRate DECIMAL(18,4),
    RenewalRate DECIMAL(18,4),
    BonusRate DECIMAL(18,4),
    OverrideRate DECIMAL(18,4),
    MinCoverage DECIMAL(18,2),
    MaxCoverage DECIMAL(18,2),
    RateType INT DEFAULT 0,
    RateTypeString NVARCHAR(50),
    Category NVARCHAR(100),
    GroupSize NVARCHAR(100),
    GroupSizeFrom INT,
    GroupSizeTo INT,
    [Level] NVARCHAR(50),
    [State] NVARCHAR(10),
    OffGroupLetterDescription NVARCHAR(500),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_schedule_rates PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_schedule_rates]';

CREATE NONCLUSTERED INDEX IX_stg_schedule_rates_ProductCode_State
ON [etl].[stg_schedule_rates] (ProductCode, [State]);

-- =============================================================================
-- Policies
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_policies];
CREATE TABLE [etl].[stg_policies] (
    Id NVARCHAR(100) NOT NULL,
    PolicyNumber NVARCHAR(100) NOT NULL,
    CertificateNumber NVARCHAR(100),
    OldPolicyNumber NVARCHAR(100),
    PolicyType INT DEFAULT 0,
    [Status] INT DEFAULT 0,
    StatusDate DATETIME2,
    BrokerId BIGINT NOT NULL,
    ContractId NVARCHAR(100),
    GroupId NVARCHAR(100),
    CarrierName NVARCHAR(500) NOT NULL,
    CarrierId NVARCHAR(100),
    ProductCode NVARCHAR(100) NOT NULL,
    ProductName NVARCHAR(500) NOT NULL,
    PlanCode NVARCHAR(100),
    PlanName NVARCHAR(500),
    MasterCategory NVARCHAR(100),
    Category NVARCHAR(100),
    InsuredName NVARCHAR(500) NOT NULL,
    InsuredFirstName NVARCHAR(200),
    InsuredLastName NVARCHAR(200),
    Premium DECIMAL(18,2),
    FaceAmount DECIMAL(18,2),
    PayMode NVARCHAR(50),
    Frequency NVARCHAR(50),
    EffectiveDate DATE NOT NULL,
    IssueDate DATE,
    ExpirationDate DATE,
    [State] NVARCHAR(10),
    Division NVARCHAR(100),
    CompanyCode NVARCHAR(50),
    LionRecordNumber BIGINT,
    CustomerId NVARCHAR(100),
    PaidThroughDate DATE,
    ProposalId NVARCHAR(100),
    ProposalAssignedAt DATETIME2,
    ProposalAssignmentSource NVARCHAR(50),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_policies PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_policies]';

CREATE NONCLUSTERED INDEX IX_stg_policies_GroupId ON [etl].[stg_policies] (GroupId);
CREATE NONCLUSTERED INDEX IX_stg_policies_BrokerId ON [etl].[stg_policies] (BrokerId);

-- =============================================================================
-- Split Configurations (unique commission structures)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_split_configs];
CREATE TABLE [etl].[stg_split_configs] (
    SplitConfigHash NVARCHAR(64) NOT NULL,
    TotalSplitPercent DECIMAL(5,2),
    ParticipantCount INT,
    ConfigJson NVARCHAR(MAX),  -- JSON of participants/schedules/rates
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    CONSTRAINT PK_stg_split_configs PRIMARY KEY (SplitConfigHash)
);
PRINT 'Created [etl].[stg_split_configs]';

-- =============================================================================
-- Proposals (consolidated by minimal differentiating keys)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_proposals];
CREATE TABLE [etl].[stg_proposals] (
    Id NVARCHAR(100) NOT NULL,
    ProposalNumber NVARCHAR(100) NOT NULL,
    [Status] INT DEFAULT 0,
    SubmittedDate DATETIME2 NOT NULL,
    ProposedEffectiveDate DATETIME2 NOT NULL,
    SpecialCase BIT DEFAULT 0,
    SpecialCaseCode INT DEFAULT 0,
    SitusState NVARCHAR(10),
    ProductId NVARCHAR(100),
    ProductName NVARCHAR(500),
    BrokerId BIGINT,  -- DEPRECATED: Will be removed after migration
    BrokerUniquePartyId NVARCHAR(50),  -- NEW: Primary broker reference (ExternalPartyId)
    BrokerName NVARCHAR(500),
    GroupId NVARCHAR(100),
    GroupName NVARCHAR(500),
    ContractId NVARCHAR(100),
    RejectionReason NVARCHAR(2000),
    Notes NVARCHAR(MAX),
    -- Consolidated key dimensions (new)
    ProductCodes NVARCHAR(MAX),           -- JSON array of products, or '*' for all
    PlanCodes NVARCHAR(MAX),              -- JSON array of plans, or '*' for all
    SplitConfigHash NVARCHAR(64),         -- FK to stg_split_configs
    DateRangeFrom INT,                    -- Start year of effective range
    DateRangeTo INT,                      -- End year (NULL = open-ended/current)
    -- Legacy fields
    PlanCodeConstraints NVARCHAR(MAX),
    EnablePlanCodeFiltering BIT DEFAULT 0,
    EffectiveDateFrom DATETIME2,
    EffectiveDateTo DATETIME2,
    EnableEffectiveDateFiltering BIT DEFAULT 0,
    ConstrainingEffectiveDateFrom DATETIME2,
    ConstrainingEffectiveDateTo DATETIME2,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_proposals PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_proposals]';

CREATE NONCLUSTERED INDEX IX_stg_proposals_GroupId ON [etl].[stg_proposals] (GroupId);
CREATE NONCLUSTERED INDEX IX_stg_proposals_SplitConfigHash ON [etl].[stg_proposals] (SplitConfigHash);

-- =============================================================================
-- Proposal Key Mapping (fine-grain to proposal lookup)
-- Enables deterministic F(Group, Year, Product, Plan) -> ProposalId
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_proposal_key_mapping];
CREATE TABLE [etl].[stg_proposal_key_mapping] (
    GroupId NVARCHAR(100) NOT NULL,
    EffectiveYear INT NOT NULL,
    ProductCode NVARCHAR(100) NOT NULL,
    PlanCode NVARCHAR(100) NOT NULL,      -- '*' = all plans (wildcard)
    ProposalId NVARCHAR(100) NOT NULL,
    SplitConfigHash NVARCHAR(64),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    CONSTRAINT PK_stg_proposal_key_mapping PRIMARY KEY (GroupId, EffectiveYear, ProductCode, PlanCode)
);
PRINT 'Created [etl].[stg_proposal_key_mapping]';

CREATE NONCLUSTERED INDEX IX_stg_proposal_key_mapping_ProposalId ON [etl].[stg_proposal_key_mapping] (ProposalId);

-- =============================================================================
-- Proposal Products
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_proposal_products];
CREATE TABLE [etl].[stg_proposal_products] (
    Id BIGINT NOT NULL,
    ProposalId NVARCHAR(100) NOT NULL,
    ProductCode NVARCHAR(100) NOT NULL,
    ProductName NVARCHAR(500),
    CommissionStructure NVARCHAR(100),
    ResolvedScheduleId NVARCHAR(100),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_proposal_products PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_proposal_products]';

-- =============================================================================
-- Hierarchies
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_hierarchies];
CREATE TABLE [etl].[stg_hierarchies] (
    Id NVARCHAR(100) NOT NULL,
    Name NVARCHAR(500),
    [Description] NVARCHAR(2000),
    [Type] INT DEFAULT 0,
    [Status] INT DEFAULT 0,
    ProposalId NVARCHAR(100),
    ProposalNumber NVARCHAR(100),
    GroupId NVARCHAR(100),
    GroupName NVARCHAR(500),
    GroupNumber NVARCHAR(100),
    BrokerId BIGINT,
    BrokerName NVARCHAR(500),
    BrokerLevel INT,
    ContractId NVARCHAR(100),
    ContractNumber NVARCHAR(100),
    ContractType NVARCHAR(100),
    ContractStatus NVARCHAR(50),
    SourceType NVARCHAR(100),
    HasOverrides BIT DEFAULT 0,
    DeviationCount INT DEFAULT 0,
    SitusState NVARCHAR(10),
    EffectiveDate DATE,
    CurrentVersionId NVARCHAR(100),
    CurrentVersionNumber INT,
    TemplateId NVARCHAR(100),
    TemplateVersion NVARCHAR(50),
    TemplateSyncStatus INT,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_hierarchies PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_hierarchies]';

CREATE NONCLUSTERED INDEX IX_stg_hierarchies_GroupId ON [etl].[stg_hierarchies] (GroupId);
CREATE NONCLUSTERED INDEX IX_stg_hierarchies_BrokerId ON [etl].[stg_hierarchies] (BrokerId);

-- =============================================================================
-- SplitSeq to Hierarchy Mapping
-- Maps each (GroupId, CertSplitSeq) to its corresponding HierarchyId
-- Used by split participants to link to the correct hierarchy
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_splitseq_hierarchy_map];
CREATE TABLE [etl].[stg_splitseq_hierarchy_map] (
    GroupId NVARCHAR(100) NOT NULL,
    CertSplitSeq INT NOT NULL,
    WritingBrokerId BIGINT NOT NULL,
    HierarchyId NVARCHAR(100) NOT NULL,
    StructureSignature NVARCHAR(MAX),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    CONSTRAINT PK_stg_splitseq_hierarchy_map PRIMARY KEY (GroupId, CertSplitSeq, WritingBrokerId)
);
PRINT 'Created [etl].[stg_splitseq_hierarchy_map]';

CREATE NONCLUSTERED INDEX IX_stg_splitseq_hierarchy_map_HierarchyId 
ON [etl].[stg_splitseq_hierarchy_map] (HierarchyId);

-- =============================================================================
-- Hierarchy Transform Work Tables (persist across GO batches)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[work_hierarchy_data];
CREATE TABLE [etl].[work_hierarchy_data] (
    GroupId NVARCHAR(100) NOT NULL,
    WritingBrokerId BIGINT NOT NULL,
    FirstUplineId BIGINT,
    MinEffDate DATETIME2,
    HierarchyId NVARCHAR(100) NOT NULL
);
PRINT 'Created [etl].[work_hierarchy_data]';

DROP TABLE IF EXISTS [etl].[work_unique_hierarchies];
CREATE TABLE [etl].[work_unique_hierarchies] (
    GroupId NVARCHAR(100) NOT NULL,
    WritingBrokerId BIGINT NOT NULL,
    StructureSignature NVARCHAR(MAX),
    RepresentativeSplitSeq INT,
    MinEffDate DATETIME2,
    HierarchySeq INT
);
PRINT 'Created [etl].[work_unique_hierarchies]';

DROP TABLE IF EXISTS [etl].[work_split_participants];
CREATE TABLE [etl].[work_split_participants] (
    GroupId NVARCHAR(100) NOT NULL,
    CertSplitSeq INT,
    WritingBrokerId BIGINT,
    [Level] INT,
    BrokerId BIGINT,
    ScheduleCode NVARCHAR(200),
    SplitPercent DECIMAL(18,4),
    MinEffDate DATETIME2
);
PRINT 'Created [etl].[work_split_participants]';

DROP TABLE IF EXISTS [etl].[work_split_signatures];
CREATE TABLE [etl].[work_split_signatures] (
    GroupId NVARCHAR(100) NOT NULL,
    CertSplitSeq INT,
    WritingBrokerId BIGINT,
    MinEffDate DATETIME2,
    StructureSignature NVARCHAR(MAX)
);
PRINT 'Created [etl].[work_split_signatures]';

DROP TABLE IF EXISTS [etl].[work_hierarchy_id_map];
CREATE TABLE [etl].[work_hierarchy_id_map] (
    GroupId NVARCHAR(100) NOT NULL,
    WritingBrokerId BIGINT,
    StructureSignature NVARCHAR(MAX),
    MinEffDate DATETIME2,
    RepresentativeSplitSeq INT,
    HierarchyId NVARCHAR(100)
);
PRINT 'Created [etl].[work_hierarchy_id_map]';

DROP TABLE IF EXISTS [etl].[work_splitseq_to_hierarchy];
CREATE TABLE [etl].[work_splitseq_to_hierarchy] (
    GroupId NVARCHAR(100) NOT NULL,
    CertSplitSeq INT,
    WritingBrokerId BIGINT,
    HierarchyId NVARCHAR(100),
    MinEffDate DATETIME2
);
PRINT 'Created [etl].[work_splitseq_to_hierarchy]';

-- =============================================================================
-- Hierarchy Versions
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_hierarchy_versions];
CREATE TABLE [etl].[stg_hierarchy_versions] (
    Id NVARCHAR(100) NOT NULL,
    HierarchyId NVARCHAR(100),
    [Version] INT DEFAULT 1,
    [Status] INT DEFAULT 0,
    EffectiveFrom DATETIME2,
    EffectiveTo DATETIME2,
    ChangeReason NVARCHAR(2000),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_hierarchy_versions PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_hierarchy_versions]';

CREATE NONCLUSTERED INDEX IX_stg_hierarchy_versions_HierarchyId 
ON [etl].[stg_hierarchy_versions] (HierarchyId);

-- =============================================================================
-- Hierarchy Participants
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_hierarchy_participants];
CREATE TABLE [etl].[stg_hierarchy_participants] (
    Id NVARCHAR(100) NOT NULL,
    HierarchyVersionId NVARCHAR(100) NOT NULL,
    EntityId BIGINT NOT NULL,
    EntityName NVARCHAR(500),
    [Level] INT DEFAULT 0,
    SortOrder INT DEFAULT 0,
    SplitPercent DECIMAL(18,4),
    ScheduleCode NVARCHAR(200),
    ScheduleId BIGINT,  -- Links to stg_schedules.Id via ScheduleCode = ExternalId
    CommissionRate DECIMAL(18,4),
    PaidBrokerId BIGINT,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_hierarchy_participants PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_hierarchy_participants]';

CREATE NONCLUSTERED INDEX IX_stg_hierarchy_participants_HierarchyVersionId 
ON [etl].[stg_hierarchy_participants] (HierarchyVersionId);

CREATE NONCLUSTERED INDEX IX_stg_hierarchy_participants_EntityId 
ON [etl].[stg_hierarchy_participants] (EntityId);

-- =============================================================================
-- State Rules - Groups products by state within a hierarchy version
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_state_rules];
CREATE TABLE [etl].[stg_state_rules] (
    Id NVARCHAR(200) NOT NULL,
    HierarchyVersionId NVARCHAR(100) NOT NULL,
    ShortName NVARCHAR(10) NOT NULL,
    Name NVARCHAR(200) NOT NULL,
    [Description] NVARCHAR(500),
    [Type] INT DEFAULT 0,
    SortOrder INT DEFAULT 0,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_state_rules PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_state_rules]';

CREATE NONCLUSTERED INDEX IX_stg_state_rules_HierarchyVersionId 
ON [etl].[stg_state_rules] (HierarchyVersionId);

-- =============================================================================
-- State Rule States - Which state codes belong to each state rule
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_state_rule_states];
CREATE TABLE [etl].[stg_state_rule_states] (
    Id NVARCHAR(200) NOT NULL,
    StateRuleId NVARCHAR(200) NOT NULL,
    StateCode NVARCHAR(10) NOT NULL,
    StateName NVARCHAR(100) NOT NULL,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_state_rule_states PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_state_rule_states]';

CREATE NONCLUSTERED INDEX IX_stg_state_rule_states_StateRuleId 
ON [etl].[stg_state_rule_states] (StateRuleId);

-- =============================================================================
-- Hierarchy Splits - Products within each state rule
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_hierarchy_splits];
CREATE TABLE [etl].[stg_hierarchy_splits] (
    Id NVARCHAR(200) NOT NULL,
    StateRuleId NVARCHAR(200) NOT NULL,
    ProductId NVARCHAR(100),
    ProductCode NVARCHAR(100) NOT NULL,
    ProductName NVARCHAR(500),
    SortOrder INT DEFAULT 0,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_hierarchy_splits PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_hierarchy_splits]';

CREATE NONCLUSTERED INDEX IX_stg_hierarchy_splits_StateRuleId 
ON [etl].[stg_hierarchy_splits] (StateRuleId);

CREATE NONCLUSTERED INDEX IX_stg_hierarchy_splits_ProductCode 
ON [etl].[stg_hierarchy_splits] (ProductCode);

-- =============================================================================
-- Split Distributions - Links participants to products with schedules
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_split_distributions];
CREATE TABLE [etl].[stg_split_distributions] (
    Id NVARCHAR(200) NOT NULL,
    HierarchySplitId NVARCHAR(200) NOT NULL,
    HierarchyParticipantId NVARCHAR(200) NOT NULL,
    ParticipantEntityId BIGINT NOT NULL,
    Percentage DECIMAL(18,4) NOT NULL DEFAULT 100,
    ScheduleId NVARCHAR(100),
    ScheduleName NVARCHAR(500),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_split_distributions PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_split_distributions]';

CREATE NONCLUSTERED INDEX IX_stg_split_distributions_HierarchySplitId 
ON [etl].[stg_split_distributions] (HierarchySplitId);

CREATE NONCLUSTERED INDEX IX_stg_split_distributions_HierarchyParticipantId 
ON [etl].[stg_split_distributions] (HierarchyParticipantId);

-- =============================================================================
-- Premium Split Versions
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_premium_split_versions];
CREATE TABLE [etl].[stg_premium_split_versions] (
    Id NVARCHAR(100) NOT NULL,
    GroupId NVARCHAR(100) NOT NULL,
    GroupName NVARCHAR(500),
    ProposalId NVARCHAR(100) NOT NULL,
    ProposalNumber NVARCHAR(100),
    ProductId NVARCHAR(100),
    VersionNumber NVARCHAR(50),
    EffectiveFrom DATETIME2 NOT NULL,
    EffectiveTo DATETIME2,
    ChangeDescription NVARCHAR(2000),
    TotalSplitPercent DECIMAL(18,4) NOT NULL,
    [Status] INT DEFAULT 0,
    [Source] INT DEFAULT 0,
    HubspotDealId NVARCHAR(100),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_premium_split_versions PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_premium_split_versions]';

CREATE NONCLUSTERED INDEX IX_stg_premium_split_versions_ProposalId 
ON [etl].[stg_premium_split_versions] (ProposalId);

-- =============================================================================
-- Premium Split Participants
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_premium_split_participants];
CREATE TABLE [etl].[stg_premium_split_participants] (
    Id NVARCHAR(100) NOT NULL,
    VersionId NVARCHAR(100) NOT NULL,
    BrokerId BIGINT NOT NULL,  -- DEPRECATED: Will be removed after migration
    BrokerUniquePartyId NVARCHAR(50),  -- NEW: Primary broker reference (ExternalPartyId)
    BrokerName NVARCHAR(500),
    BrokerNPN NVARCHAR(50),
    SplitPercent DECIMAL(18,4) NOT NULL,
    IsWritingAgent BIT DEFAULT 0,
    HierarchyId NVARCHAR(100),
    HierarchyName NVARCHAR(500),
    TemplateId NVARCHAR(100),
    TemplateName NVARCHAR(500),
    EffectiveFrom DATETIME2 NOT NULL,
    EffectiveTo DATETIME2,
    Notes NVARCHAR(MAX),
    Sequence INT DEFAULT 1,
    WritingBrokerId BIGINT,
    GroupId NVARCHAR(100),  -- Used for linking to hierarchy via splitseq map
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_premium_split_participants PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_premium_split_participants]';

CREATE NONCLUSTERED INDEX IX_stg_premium_split_participants_VersionId 
ON [etl].[stg_premium_split_participants] (VersionId);

-- =============================================================================
-- Commission Assignment Versions
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_commission_assignment_versions];
CREATE TABLE [etl].[stg_commission_assignment_versions] (
    Id NVARCHAR(100) NOT NULL,
    BrokerId BIGINT NOT NULL,
    BrokerName NVARCHAR(500),
    ProposalId NVARCHAR(100) NOT NULL,
    GroupId BIGINT,
    HierarchyId NVARCHAR(100),
    HierarchyVersionId NVARCHAR(100),
    HierarchyParticipantId NVARCHAR(100),
    VersionNumber NVARCHAR(50),
    EffectiveFrom DATETIME2 NOT NULL,
    EffectiveTo DATETIME2,
    [Status] INT DEFAULT 0,
    [Type] INT DEFAULT 0,
    ChangeDescription NVARCHAR(2000),
    TotalAssignedPercent DECIMAL(18,4) DEFAULT 100.0,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_commission_assignment_versions PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_commission_assignment_versions]';

-- =============================================================================
-- Commission Assignment Recipients
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_commission_assignment_recipients];
CREATE TABLE [etl].[stg_commission_assignment_recipients] (
    Id NVARCHAR(100) NOT NULL,
    AssignmentVersionId NVARCHAR(100) NOT NULL,
    RecipientBrokerId BIGINT NOT NULL,
    RecipientBrokerName NVARCHAR(500),
    [Percent] DECIMAL(18,4) DEFAULT 100.0,
    RecipientType INT DEFAULT 1,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_commission_assignment_recipients PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_commission_assignment_recipients]';

-- =============================================================================
-- Premium Transactions (for commission calculation)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_premium_transactions];
CREATE TABLE [etl].[stg_premium_transactions] (
    Id NVARCHAR(100) NOT NULL,
    CertificateId BIGINT NOT NULL,
    TransactionDate DATE NOT NULL,
    PremiumAmount DECIMAL(18,2) NOT NULL,
    BillingPeriodStart DATE,
    BillingPeriodEnd DATE,
    PaymentStatus NVARCHAR(50) DEFAULT 'Paid',
    SourceSystem NVARCHAR(100) DEFAULT 'LEGACY_MIGRATION',
    CreatedDate DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_premium_transactions PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_premium_transactions]';

CREATE NONCLUSTERED INDEX IX_stg_premium_transactions_CertificateId 
ON [etl].[stg_premium_transactions] (CertificateId);

CREATE NONCLUSTERED INDEX IX_stg_premium_transactions_TransactionDate 
ON [etl].[stg_premium_transactions] (TransactionDate);

-- =============================================================================
-- Fee Schedules
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_fee_schedules];
CREATE TABLE [etl].[stg_fee_schedules] (
    Id NVARCHAR(100) NOT NULL,
    GroupId NVARCHAR(100) NOT NULL,
    GroupName NVARCHAR(500),
    FeeType NVARCHAR(100) NOT NULL,
    Amount DECIMAL(18,2) NOT NULL,
    EffectiveDate DATETIME2 NOT NULL,
    EndDate DATETIME2,
    [Description] NVARCHAR(2000),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_fee_schedules PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_fee_schedules]';

-- =============================================================================
-- Policy Hierarchy Assignments
-- Links non-conformant policies (DTC, overlapping proposals) to their hierarchy data
-- One row per (Policy, CertSplitSeq, WritingBroker) combination
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_policy_hierarchy_assignments];
CREATE TABLE [etl].[stg_policy_hierarchy_assignments] (
    Id NVARCHAR(100) NOT NULL,
    PolicyId NVARCHAR(100) NOT NULL,
    CertificateId BIGINT,
    HierarchyId NVARCHAR(100),  -- Links to stg_hierarchies if exists
    SplitPercent DECIMAL(5,2) NOT NULL,
    WritingBrokerId BIGINT NOT NULL,
    SplitSequence INT NOT NULL,
    IsNonConforming BIT DEFAULT 1,
    NonConformantReason NVARCHAR(500),
    SourceTraceabilityReportId BIGINT,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_policy_hierarchy_assignments PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_policy_hierarchy_assignments]';

CREATE NONCLUSTERED INDEX IX_stg_policy_hierarchy_assignments_PolicyId 
ON [etl].[stg_policy_hierarchy_assignments] (PolicyId);

CREATE NONCLUSTERED INDEX IX_stg_policy_hierarchy_assignments_HierarchyId 
ON [etl].[stg_policy_hierarchy_assignments] (HierarchyId);

-- =============================================================================
-- Policy Hierarchy Participants
-- Hierarchy participants embedded per policy assignment
-- Captures the actual commission chain from raw data
-- =============================================================================
DROP TABLE IF EXISTS [etl].[stg_policy_hierarchy_participants];
CREATE TABLE [etl].[stg_policy_hierarchy_participants] (
    Id NVARCHAR(100) NOT NULL,
    PolicyHierarchyAssignmentId NVARCHAR(100) NOT NULL,
    BrokerId BIGINT NOT NULL,
    BrokerName NVARCHAR(500),
    [Level] INT NOT NULL,
    CommissionRate DECIMAL(18,4),
    ScheduleCode NVARCHAR(200),
    ScheduleId BIGINT,
    ReassignedType NVARCHAR(50),
    PaidBrokerId BIGINT,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_stg_policy_hierarchy_participants PRIMARY KEY (Id)
);
PRINT 'Created [etl].[stg_policy_hierarchy_participants]';

CREATE NONCLUSTERED INDEX IX_stg_policy_hierarchy_participants_AssignmentId 
ON [etl].[stg_policy_hierarchy_participants] (PolicyHierarchyAssignmentId);

CREATE NONCLUSTERED INDEX IX_stg_policy_hierarchy_participants_BrokerId 
ON [etl].[stg_policy_hierarchy_participants] (BrokerId);

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT 'Staging tables created:';
SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('etl') AND name LIKE 'stg_%' ORDER BY name;

PRINT '';
PRINT '============================================================';
PRINT 'STAGING TABLES CREATED SUCCESSFULLY';
PRINT '============================================================';

GO

