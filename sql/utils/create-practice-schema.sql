-- ============================================================
-- Create [practice] schema with proposal chain tables
-- For testing safe export scripts
-- ============================================================

-- Create schema if not exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'practice')
BEGIN
    EXEC('CREATE SCHEMA [practice]');
    PRINT 'Created schema [practice]';
END
GO

-- Drop existing tables in reverse FK order
IF OBJECT_ID('practice.PolicyHierarchyAssignments', 'U') IS NOT NULL DROP TABLE [practice].[PolicyHierarchyAssignments];
IF OBJECT_ID('practice.HierarchyParticipants', 'U') IS NOT NULL DROP TABLE [practice].[HierarchyParticipants];
IF OBJECT_ID('practice.HierarchyVersions', 'U') IS NOT NULL DROP TABLE [practice].[HierarchyVersions];
IF OBJECT_ID('practice.Hierarchies', 'U') IS NOT NULL DROP TABLE [practice].[Hierarchies];
IF OBJECT_ID('practice.PremiumSplitParticipants', 'U') IS NOT NULL DROP TABLE [practice].[PremiumSplitParticipants];
IF OBJECT_ID('practice.PremiumSplitVersions', 'U') IS NOT NULL DROP TABLE [practice].[PremiumSplitVersions];
IF OBJECT_ID('practice.Policies', 'U') IS NOT NULL DROP TABLE [practice].[Policies];
IF OBJECT_ID('practice.ProposalProducts', 'U') IS NOT NULL DROP TABLE [practice].[ProposalProducts];
IF OBJECT_ID('practice.Proposals', 'U') IS NOT NULL DROP TABLE [practice].[Proposals];
PRINT 'Dropped existing practice tables';
GO

-- Create Proposals
CREATE TABLE [practice].[Proposals] (
    Id NVARCHAR(100) NOT NULL PRIMARY KEY,
    ProposalNumber NVARCHAR(100) NULL,
    Status INT NULL,
    SubmittedDate DATE NULL,
    ProposedEffectiveDate DATE NULL,
    SitusState NVARCHAR(10) NULL,
    GroupId NVARCHAR(50) NULL,
    GroupName NVARCHAR(255) NULL,
    ProductCodes NVARCHAR(MAX) NULL,
    PlanCodes NVARCHAR(MAX) NULL,
    SplitConfigHash NVARCHAR(100) NULL,
    DateRangeFrom DATE NULL,
    DateRangeTo DATE NULL,
    EffectiveDateFrom DATE NULL,
    EffectiveDateTo DATE NULL,
    Notes NVARCHAR(MAX) NULL,
    CreationTime DATETIME2 NULL,
    IsDeleted BIT NULL DEFAULT 0
);
CREATE INDEX IX_Proposals_GroupId ON [practice].[Proposals](GroupId);
PRINT 'Created practice.Proposals';
GO

-- Create ProposalProducts
CREATE TABLE [practice].[ProposalProducts] (
    Id NVARCHAR(100) NOT NULL PRIMARY KEY,
    ProposalId NVARCHAR(100) NULL,
    ProductId NVARCHAR(50) NULL,
    ProductCode NVARCHAR(50) NULL,
    ProductName NVARCHAR(255) NULL,
    PlanCode NVARCHAR(50) NULL,
    PlanName NVARCHAR(255) NULL,
    EffectiveDate DATE NULL,
    CreationTime DATETIME2 NULL,
    IsDeleted BIT NULL DEFAULT 0
);
CREATE INDEX IX_ProposalProducts_ProposalId ON [practice].[ProposalProducts](ProposalId);
PRINT 'Created practice.ProposalProducts';
GO

-- Create PremiumSplitVersions
CREATE TABLE [practice].[PremiumSplitVersions] (
    Id NVARCHAR(100) NOT NULL PRIMARY KEY,
    GroupId NVARCHAR(50) NULL,
    GroupName NVARCHAR(255) NULL,
    ProposalId NVARCHAR(100) NULL,
    ProposalNumber NVARCHAR(100) NULL,
    VersionNumber INT NULL,
    EffectiveFrom DATE NULL,
    EffectiveTo DATE NULL,
    TotalSplitPercent DECIMAL(10,4) NULL,
    Status INT NULL,
    CreationTime DATETIME2 NULL,
    IsDeleted BIT NULL DEFAULT 0
);
CREATE INDEX IX_PremiumSplitVersions_GroupId ON [practice].[PremiumSplitVersions](GroupId);
PRINT 'Created practice.PremiumSplitVersions';
GO

-- Create PremiumSplitParticipants
CREATE TABLE [practice].[PremiumSplitParticipants] (
    Id NVARCHAR(100) NOT NULL PRIMARY KEY,
    VersionId NVARCHAR(100) NULL,
    BrokerId NVARCHAR(50) NULL,
    BrokerName NVARCHAR(255) NULL,
    SplitPercent DECIMAL(10,4) NULL,
    IsWritingAgent BIT NULL,
    HierarchyId NVARCHAR(100) NULL,
    Sequence INT NULL,
    WritingBrokerId NVARCHAR(50) NULL,
    GroupId NVARCHAR(50) NULL,
    EffectiveFrom DATE NULL,
    EffectiveTo DATE NULL,
    CreationTime DATETIME2 NULL,
    IsDeleted BIT NULL DEFAULT 0
);
CREATE INDEX IX_PremiumSplitParticipants_GroupId ON [practice].[PremiumSplitParticipants](GroupId);
PRINT 'Created practice.PremiumSplitParticipants';
GO

-- Create Hierarchies
CREATE TABLE [practice].[Hierarchies] (
    Id NVARCHAR(100) NOT NULL PRIMARY KEY,
    Name NVARCHAR(255) NULL,
    GroupId NVARCHAR(50) NULL,
    GroupName NVARCHAR(255) NULL,
    BrokerId NVARCHAR(50) NULL,
    BrokerName NVARCHAR(255) NULL,
    ProposalId NVARCHAR(100) NULL,
    SitusState NVARCHAR(10) NULL,
    CurrentVersionId NVARCHAR(100) NULL,
    Status INT NULL,
    CreationTime DATETIME2 NULL,
    IsDeleted BIT NULL DEFAULT 0
);
CREATE INDEX IX_Hierarchies_GroupId ON [practice].[Hierarchies](GroupId);
PRINT 'Created practice.Hierarchies';
GO

-- Create HierarchyVersions
CREATE TABLE [practice].[HierarchyVersions] (
    Id NVARCHAR(100) NOT NULL PRIMARY KEY,
    HierarchyId NVARCHAR(100) NULL,
    VersionNumber INT NULL,
    EffectiveFrom DATE NULL,
    EffectiveTo DATE NULL,
    Status INT NULL,
    CreationTime DATETIME2 NULL,
    IsDeleted BIT NULL DEFAULT 0
);
CREATE INDEX IX_HierarchyVersions_HierarchyId ON [practice].[HierarchyVersions](HierarchyId);
PRINT 'Created practice.HierarchyVersions';
GO

-- Create HierarchyParticipants
CREATE TABLE [practice].[HierarchyParticipants] (
    Id NVARCHAR(100) NOT NULL PRIMARY KEY,
    HierarchyId NVARCHAR(100) NULL,
    HierarchyVersionId NVARCHAR(100) NULL,
    BrokerId NVARCHAR(50) NULL,
    BrokerName NVARCHAR(255) NULL,
    Level INT NULL,
    SplitPercent DECIMAL(10,4) NULL,
    ScheduleId NVARCHAR(100) NULL,
    ScheduleCode NVARCHAR(50) NULL,
    CreationTime DATETIME2 NULL,
    IsDeleted BIT NULL DEFAULT 0
);
CREATE INDEX IX_HierarchyParticipants_HierarchyId ON [practice].[HierarchyParticipants](HierarchyId);
PRINT 'Created practice.HierarchyParticipants';
GO

-- Create Policies
CREATE TABLE [practice].[Policies] (
    Id NVARCHAR(100) NOT NULL PRIMARY KEY,
    PolicyNumber NVARCHAR(100) NULL,
    GroupId NVARCHAR(50) NULL,
    GroupName NVARCHAR(255) NULL,
    ProductCode NVARCHAR(50) NULL,
    PlanCode NVARCHAR(50) NULL,
    EffectiveDate DATE NULL,
    TerminationDate DATE NULL,
    Status INT NULL,
    BrokerId NVARCHAR(50) NULL,
    ProposalId NVARCHAR(100) NULL,
    SitusState NVARCHAR(10) NULL,
    ProposalAssignmentSource NVARCHAR(100) NULL,
    CreationTime DATETIME2 NULL,
    IsDeleted BIT NULL DEFAULT 0
);
CREATE INDEX IX_Policies_GroupId ON [practice].[Policies](GroupId);
PRINT 'Created practice.Policies';
GO

-- Create PolicyHierarchyAssignments
CREATE TABLE [practice].[PolicyHierarchyAssignments] (
    Id NVARCHAR(100) NOT NULL PRIMARY KEY,
    PolicyId NVARCHAR(100) NULL,
    HierarchyId NVARCHAR(100) NULL,
    SplitSequence INT NULL,
    SplitPercent DECIMAL(10,4) NULL,
    WritingBrokerId NVARCHAR(50) NULL,
    IsNonConforming BIT NULL,
    NonConformantReason NVARCHAR(MAX) NULL,
    CreationTime DATETIME2 NULL,
    IsDeleted BIT NULL DEFAULT 0
);
CREATE INDEX IX_PolicyHierarchyAssignments_HierarchyId ON [practice].[PolicyHierarchyAssignments](HierarchyId);
CREATE INDEX IX_PolicyHierarchyAssignments_PolicyId ON [practice].[PolicyHierarchyAssignments](PolicyId);
PRINT 'Created practice.PolicyHierarchyAssignments';
GO

PRINT '';
PRINT '✅ Practice schema created successfully!';
PRINT '';
PRINT 'Tables created:';
PRINT '  - practice.Proposals';
PRINT '  - practice.ProposalProducts';
PRINT '  - practice.PremiumSplitVersions';
PRINT '  - practice.PremiumSplitParticipants';
PRINT '  - practice.Hierarchies';
PRINT '  - practice.HierarchyVersions';
PRINT '  - practice.HierarchyParticipants';
PRINT '  - practice.Policies';
PRINT '  - practice.PolicyHierarchyAssignments';
GO
