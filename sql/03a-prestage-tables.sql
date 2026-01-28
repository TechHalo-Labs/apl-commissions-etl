-- =============================================================================
-- Pre-Stage Tables for SQL Server ETL Pipeline
-- =============================================================================
-- These tables retain unconsolidated proposals with full split configuration JSON
-- Used for consolidation audit trail before moving to staging
-- Usage: sqlcmd -S server -d database -i 03a-prestage-tables.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'CREATING PRE-STAGE TABLES';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Create prestage schema if not exists
-- =============================================================================
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'prestage')
BEGIN
    EXEC('CREATE SCHEMA prestage');
    PRINT 'Created prestage schema';
END
ELSE
BEGIN
    PRINT 'prestage schema already exists';
END

-- =============================================================================
-- Pre-Stage Proposals (unconsolidated with split configuration JSON)
-- =============================================================================
DROP TABLE IF EXISTS [prestage].[prestage_proposals];
CREATE TABLE [prestage].[prestage_proposals] (
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
    BrokerId BIGINT,
    BrokerUniquePartyId NVARCHAR(50),
    BrokerName NVARCHAR(500),
    GroupId NVARCHAR(100),
    GroupName NVARCHAR(500),
    ContractId NVARCHAR(100),
    RejectionReason NVARCHAR(2000),
    Notes NVARCHAR(MAX),
    ProductCodes NVARCHAR(MAX),
    PlanCodes NVARCHAR(MAX),
    SplitConfigHash NVARCHAR(64),
    DateRangeFrom INT,
    DateRangeTo INT,
    PlanCodeConstraints NVARCHAR(MAX),
    EnablePlanCodeFiltering BIT DEFAULT 0,
    EffectiveDateFrom DATETIME2,
    EffectiveDateTo DATETIME2,
    EnableEffectiveDateFiltering BIT DEFAULT 0,
    ConstrainingEffectiveDateFrom DATETIME2,
    ConstrainingEffectiveDateTo DATETIME2,
    -- NEW: Consolidation fields
    SplitConfigurationJSON NVARCHAR(MAX),     -- Full split config with hierarchies
    SplitConfigurationMD5 CHAR(32),           -- MD5 hash for grouping
    IsRetained BIT DEFAULT 0,                 -- Consolidation flag
    ConsumedByProposalId NVARCHAR(100),       -- Link to retained proposal
    ConsolidationReason NVARCHAR(500),        -- Why consumed
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_prestage_proposals PRIMARY KEY (Id)
);
PRINT 'Created [prestage].[prestage_proposals]';

CREATE NONCLUSTERED INDEX IX_prestage_proposals_GroupId 
ON [prestage].[prestage_proposals] (GroupId);

CREATE NONCLUSTERED INDEX IX_prestage_proposals_SplitConfigHash 
ON [prestage].[prestage_proposals] (SplitConfigHash);

CREATE NONCLUSTERED INDEX IX_prestage_proposals_SplitConfigMD5 
ON [prestage].[prestage_proposals] (SplitConfigurationMD5);

CREATE NONCLUSTERED INDEX IX_prestage_proposals_IsRetained 
ON [prestage].[prestage_proposals] (IsRetained);

CREATE NONCLUSTERED INDEX IX_prestage_proposals_ConsumedBy 
ON [prestage].[prestage_proposals] (ConsumedByProposalId);

-- =============================================================================
-- Pre-Stage Hierarchies (unconsolidated)
-- =============================================================================
DROP TABLE IF EXISTS [prestage].[prestage_hierarchies];
CREATE TABLE [prestage].[prestage_hierarchies] (
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
    CONSTRAINT PK_prestage_hierarchies PRIMARY KEY (Id)
);
PRINT 'Created [prestage].[prestage_hierarchies]';

CREATE NONCLUSTERED INDEX IX_prestage_hierarchies_GroupId 
ON [prestage].[prestage_hierarchies] (GroupId);

CREATE NONCLUSTERED INDEX IX_prestage_hierarchies_BrokerId 
ON [prestage].[prestage_hierarchies] (BrokerId);

-- =============================================================================
-- Pre-Stage Hierarchy Versions
-- =============================================================================
DROP TABLE IF EXISTS [prestage].[prestage_hierarchy_versions];
CREATE TABLE [prestage].[prestage_hierarchy_versions] (
    Id NVARCHAR(100) NOT NULL,
    HierarchyId NVARCHAR(100),
    [Version] INT DEFAULT 1,
    [Status] INT DEFAULT 0,
    EffectiveFrom DATETIME2,
    EffectiveTo DATETIME2,
    ChangeReason NVARCHAR(2000),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_prestage_hierarchy_versions PRIMARY KEY (Id)
);
PRINT 'Created [prestage].[prestage_hierarchy_versions]';

CREATE NONCLUSTERED INDEX IX_prestage_hierarchy_versions_HierarchyId 
ON [prestage].[prestage_hierarchy_versions] (HierarchyId);

-- =============================================================================
-- Pre-Stage Hierarchy Participants
-- =============================================================================
DROP TABLE IF EXISTS [prestage].[prestage_hierarchy_participants];
CREATE TABLE [prestage].[prestage_hierarchy_participants] (
    Id NVARCHAR(100) NOT NULL,
    HierarchyVersionId NVARCHAR(100) NOT NULL,
    EntityId BIGINT NOT NULL,
    EntityName NVARCHAR(500),
    [Level] INT DEFAULT 0,
    SortOrder INT DEFAULT 0,
    SplitPercent DECIMAL(18,4),
    ScheduleCode NVARCHAR(200),
    ScheduleId BIGINT,
    CommissionRate DECIMAL(18,4),
    PaidBrokerId BIGINT,
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_prestage_hierarchy_participants PRIMARY KEY (Id)
);
PRINT 'Created [prestage].[prestage_hierarchy_participants]';

CREATE NONCLUSTERED INDEX IX_prestage_hierarchy_participants_HierarchyVersionId 
ON [prestage].[prestage_hierarchy_participants] (HierarchyVersionId);

CREATE NONCLUSTERED INDEX IX_prestage_hierarchy_participants_EntityId 
ON [prestage].[prestage_hierarchy_participants] (EntityId);

-- =============================================================================
-- Pre-Stage Premium Split Versions
-- =============================================================================
DROP TABLE IF EXISTS [prestage].[prestage_premium_split_versions];
CREATE TABLE [prestage].[prestage_premium_split_versions] (
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
    CONSTRAINT PK_prestage_premium_split_versions PRIMARY KEY (Id)
);
PRINT 'Created [prestage].[prestage_premium_split_versions]';

CREATE NONCLUSTERED INDEX IX_prestage_premium_split_versions_ProposalId 
ON [prestage].[prestage_premium_split_versions] (ProposalId);

-- =============================================================================
-- Pre-Stage Premium Split Participants
-- =============================================================================
DROP TABLE IF EXISTS [prestage].[prestage_premium_split_participants];
CREATE TABLE [prestage].[prestage_premium_split_participants] (
    Id NVARCHAR(100) NOT NULL,
    VersionId NVARCHAR(100) NOT NULL,
    BrokerId BIGINT NOT NULL,
    BrokerUniquePartyId NVARCHAR(50),
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
    GroupId NVARCHAR(100),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_prestage_premium_split_participants PRIMARY KEY (Id)
);
PRINT 'Created [prestage].[prestage_premium_split_participants]';

CREATE NONCLUSTERED INDEX IX_prestage_premium_split_participants_VersionId 
ON [prestage].[prestage_premium_split_participants] (VersionId);

CREATE NONCLUSTERED INDEX IX_prestage_premium_split_participants_HierarchyId 
ON [prestage].[prestage_premium_split_participants] (HierarchyId);

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT 'Pre-stage tables created:';
SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('prestage') ORDER BY name;

PRINT '';
PRINT '============================================================';
PRINT 'PRE-STAGE TABLES CREATED SUCCESSFULLY';
PRINT '============================================================';

GO
