-- =============================================================================
-- Commission Calculation Pipeline - Table Definitions (SQL Server)
-- =============================================================================
-- Creates all 8 calculation stage tables plus output tables
-- Each stage preserves all columns from previous stages plus new resolved columns
-- Usage: sqlcmd -S server -d database -i calc/00-calc-tables.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'CREATING CALCULATION PIPELINE TABLES';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Stage 1: Premium Context
-- Enriches premium transactions with policy and group context
-- =============================================================================
DROP TABLE IF EXISTS [etl].[calc_1_premium_context];
CREATE TABLE [etl].[calc_1_premium_context] (
    -- Premium Transaction fields
    PremiumTransactionId NVARCHAR(100) NOT NULL,
    CertificateId BIGINT NOT NULL,
    TransactionDate DATE NOT NULL,
    PremiumAmount DECIMAL(18,2) NOT NULL,
    
    -- Policy context
    GroupId NVARCHAR(100),
    ProductCode NVARCHAR(100),
    [State] NVARCHAR(10),
    CertificateEffectiveDate DATE,
    
    -- Group context
    GroupSize INT,
    
    -- Calculated fields
    IsFirstYear BIT NOT NULL,
    BasisYear INT NOT NULL,
    
    -- Audit
    CreationTime DATETIME2 DEFAULT GETUTCDATE()
);
PRINT 'Created [etl].[calc_1_premium_context]';

CREATE NONCLUSTERED INDEX IX_calc_1_CertificateId 
ON [etl].[calc_1_premium_context] (CertificateId, TransactionDate);

-- =============================================================================
-- Stage 2: Proposals Resolved
-- Adds proposal resolution for each premium
-- =============================================================================
DROP TABLE IF EXISTS [etl].[calc_2_proposals_resolved];
CREATE TABLE [etl].[calc_2_proposals_resolved] (
    -- From Stage 1
    PremiumTransactionId NVARCHAR(100) NOT NULL,
    CertificateId BIGINT NOT NULL,
    TransactionDate DATE NOT NULL,
    PremiumAmount DECIMAL(18,2) NOT NULL,
    GroupId NVARCHAR(100),
    ProductCode NVARCHAR(100),
    [State] NVARCHAR(10),
    CertificateEffectiveDate DATE,
    GroupSize INT,
    IsFirstYear BIT NOT NULL,
    BasisYear INT NOT NULL,
    
    -- Proposal resolution
    ProposalId NVARCHAR(100),
    ProposalBrokerId BIGINT,
    SpecialCaseCode INT,
    ErrorMessage NVARCHAR(2000),
    
    -- Audit
    CreationTime DATETIME2 DEFAULT GETUTCDATE()
);
PRINT 'Created [etl].[calc_2_proposals_resolved]';

CREATE NONCLUSTERED INDEX IX_calc_2_ProposalId 
ON [etl].[calc_2_proposals_resolved] (ProposalId);

CREATE NONCLUSTERED INDEX IX_calc_2_PremiumTransactionId 
ON [etl].[calc_2_proposals_resolved] (PremiumTransactionId);

-- =============================================================================
-- Stage 3: Splits Applied
-- Explodes premiums into split participants (1 row -> N rows)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[calc_3_splits_applied];
CREATE TABLE [etl].[calc_3_splits_applied] (
    -- From Stage 2
    PremiumTransactionId NVARCHAR(100) NOT NULL,
    CertificateId BIGINT NOT NULL,
    TransactionDate DATE NOT NULL,
    PremiumAmount DECIMAL(18,2) NOT NULL,
    GroupId NVARCHAR(100),
    ProductCode NVARCHAR(100),
    [State] NVARCHAR(10),
    CertificateEffectiveDate DATE,
    GroupSize INT,
    IsFirstYear BIT NOT NULL,
    BasisYear INT NOT NULL,
    ProposalId NVARCHAR(100) NOT NULL,
    ProposalBrokerId BIGINT,
    SpecialCaseCode INT,
    
    -- Split resolution
    SplitSequence INT NOT NULL,
    SplitPercent DECIMAL(18,4) NOT NULL,
    HierarchyId NVARCHAR(100),
    WritingBrokerId BIGINT,
    SplitPremiumAmount DECIMAL(18,2) NOT NULL,
    
    -- Audit
    CreationTime DATETIME2 DEFAULT GETUTCDATE()
);
PRINT 'Created [etl].[calc_3_splits_applied]';

CREATE NONCLUSTERED INDEX IX_calc_3_PremiumTransactionId 
ON [etl].[calc_3_splits_applied] (PremiumTransactionId);

-- =============================================================================
-- Stage 4: Hierarchies Resolved
-- Resolves active hierarchy version for each split
-- =============================================================================
DROP TABLE IF EXISTS [etl].[calc_4_hierarchies_resolved];
CREATE TABLE [etl].[calc_4_hierarchies_resolved] (
    -- From Stage 3
    PremiumTransactionId NVARCHAR(100) NOT NULL,
    CertificateId BIGINT NOT NULL,
    TransactionDate DATE NOT NULL,
    PremiumAmount DECIMAL(18,2) NOT NULL,
    GroupId NVARCHAR(100),
    ProductCode NVARCHAR(100),
    [State] NVARCHAR(10),
    CertificateEffectiveDate DATE,
    GroupSize INT,
    IsFirstYear BIT NOT NULL,
    BasisYear INT NOT NULL,
    ProposalId NVARCHAR(100) NOT NULL,
    ProposalBrokerId BIGINT,
    SpecialCaseCode INT,
    SplitSequence INT NOT NULL,
    SplitPercent DECIMAL(18,4) NOT NULL,
    HierarchyId NVARCHAR(100),
    WritingBrokerId BIGINT,
    SplitPremiumAmount DECIMAL(18,2) NOT NULL,
    
    -- Hierarchy resolution
    HierarchyVersionId NVARCHAR(100),
    HierarchyEffectiveFrom DATE,
    
    -- Audit
    CreationTime DATETIME2 DEFAULT GETUTCDATE()
);
PRINT 'Created [etl].[calc_4_hierarchies_resolved]';

CREATE NONCLUSTERED INDEX IX_calc_4_HierarchyVersionId 
ON [etl].[calc_4_hierarchies_resolved] (HierarchyVersionId);

CREATE NONCLUSTERED INDEX IX_calc_4_PremiumTransactionId 
ON [etl].[calc_4_hierarchies_resolved] (PremiumTransactionId);

-- =============================================================================
-- Stage 5: Participants Expanded
-- Expands hierarchy participants (another row explosion: 1 split -> N tiers)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[calc_5_participants_expanded];
CREATE TABLE [etl].[calc_5_participants_expanded] (
    -- From Stage 4
    PremiumTransactionId NVARCHAR(100) NOT NULL,
    CertificateId BIGINT NOT NULL,
    TransactionDate DATE NOT NULL,
    PremiumAmount DECIMAL(18,2) NOT NULL,
    GroupId NVARCHAR(100),
    ProductCode NVARCHAR(100),
    [State] NVARCHAR(10),
    CertificateEffectiveDate DATE,
    GroupSize INT,
    IsFirstYear BIT NOT NULL,
    BasisYear INT NOT NULL,
    ProposalId NVARCHAR(100) NOT NULL,
    ProposalBrokerId BIGINT,
    SpecialCaseCode INT,
    SplitSequence INT NOT NULL,
    SplitPercent DECIMAL(18,4) NOT NULL,
    HierarchyId NVARCHAR(100),
    WritingBrokerId BIGINT,
    SplitPremiumAmount DECIMAL(18,2) NOT NULL,
    HierarchyVersionId NVARCHAR(100) NOT NULL,
    HierarchyEffectiveFrom DATE,
    
    -- Participant expansion
    HierarchyParticipantId NVARCHAR(100) NOT NULL,
    BrokerId BIGINT NOT NULL,
    BrokerName NVARCHAR(500),
    TierLevel INT NOT NULL,
    ScheduleCode NVARCHAR(200),
    PaidBrokerId BIGINT,
    ParticipantCommissionRate DECIMAL(18,4),
    
    -- Audit
    CreationTime DATETIME2 DEFAULT GETUTCDATE()
);
PRINT 'Created [etl].[calc_5_participants_expanded]';

CREATE NONCLUSTERED INDEX IX_calc_5_PremiumTransactionId 
ON [etl].[calc_5_participants_expanded] (PremiumTransactionId);

CREATE NONCLUSTERED INDEX IX_calc_5_BrokerId 
ON [etl].[calc_5_participants_expanded] (BrokerId);

-- =============================================================================
-- Stage 6: Rates Applied
-- Looks up commission rate with fallback priority
-- =============================================================================
DROP TABLE IF EXISTS [etl].[calc_6_rates_applied];
CREATE TABLE [etl].[calc_6_rates_applied] (
    -- From Stage 5
    PremiumTransactionId NVARCHAR(100) NOT NULL,
    CertificateId BIGINT NOT NULL,
    TransactionDate DATE NOT NULL,
    PremiumAmount DECIMAL(18,2) NOT NULL,
    GroupId NVARCHAR(100),
    ProductCode NVARCHAR(100),
    [State] NVARCHAR(10),
    CertificateEffectiveDate DATE,
    GroupSize INT,
    IsFirstYear BIT NOT NULL,
    BasisYear INT NOT NULL,
    ProposalId NVARCHAR(100) NOT NULL,
    ProposalBrokerId BIGINT,
    SpecialCaseCode INT,
    SplitSequence INT NOT NULL,
    SplitPercent DECIMAL(18,4) NOT NULL,
    HierarchyId NVARCHAR(100),
    WritingBrokerId BIGINT,
    SplitPremiumAmount DECIMAL(18,2) NOT NULL,
    HierarchyVersionId NVARCHAR(100) NOT NULL,
    HierarchyEffectiveFrom DATE,
    HierarchyParticipantId NVARCHAR(100) NOT NULL,
    BrokerId BIGINT NOT NULL,
    BrokerName NVARCHAR(500),
    TierLevel INT NOT NULL,
    ScheduleCode NVARCHAR(200),
    PaidBrokerId BIGINT,
    ParticipantCommissionRate DECIMAL(18,4),
    
    -- Rate resolution
    RatePercent DECIMAL(18,4) NOT NULL,
    RateSource NVARCHAR(50) NOT NULL,
    ScheduleId BIGINT,
    ScheduleVersionId BIGINT,
    
    -- Audit
    CreationTime DATETIME2 DEFAULT GETUTCDATE()
);
PRINT 'Created [etl].[calc_6_rates_applied]';

CREATE NONCLUSTERED INDEX IX_calc_6_PremiumTransactionId 
ON [etl].[calc_6_rates_applied] (PremiumTransactionId);

-- =============================================================================
-- Stage 7: Commissions Calculated
-- Calculates commission amounts
-- =============================================================================
DROP TABLE IF EXISTS [etl].[calc_7_commissions_calculated];
CREATE TABLE [etl].[calc_7_commissions_calculated] (
    -- From Stage 6
    PremiumTransactionId NVARCHAR(100) NOT NULL,
    CertificateId BIGINT NOT NULL,
    TransactionDate DATE NOT NULL,
    PremiumAmount DECIMAL(18,2) NOT NULL,
    GroupId NVARCHAR(100),
    ProductCode NVARCHAR(100),
    [State] NVARCHAR(10),
    CertificateEffectiveDate DATE,
    GroupSize INT,
    IsFirstYear BIT NOT NULL,
    BasisYear INT NOT NULL,
    ProposalId NVARCHAR(100) NOT NULL,
    ProposalBrokerId BIGINT,
    SpecialCaseCode INT,
    SplitSequence INT NOT NULL,
    SplitPercent DECIMAL(18,4) NOT NULL,
    HierarchyId NVARCHAR(100),
    WritingBrokerId BIGINT,
    SplitPremiumAmount DECIMAL(18,2) NOT NULL,
    HierarchyVersionId NVARCHAR(100) NOT NULL,
    HierarchyEffectiveFrom DATE,
    HierarchyParticipantId NVARCHAR(100) NOT NULL,
    BrokerId BIGINT NOT NULL,
    BrokerName NVARCHAR(500),
    TierLevel INT NOT NULL,
    ScheduleCode NVARCHAR(200),
    PaidBrokerId BIGINT,
    ParticipantCommissionRate DECIMAL(18,4),
    RatePercent DECIMAL(18,4) NOT NULL,
    RateSource NVARCHAR(50) NOT NULL,
    ScheduleId BIGINT,
    ScheduleVersionId BIGINT,
    
    -- Commission calculation
    CommissionAmount DECIMAL(18,2) NOT NULL,
    
    -- Audit
    CreationTime DATETIME2 DEFAULT GETUTCDATE()
);
PRINT 'Created [etl].[calc_7_commissions_calculated]';

CREATE NONCLUSTERED INDEX IX_calc_7_PremiumTransactionId 
ON [etl].[calc_7_commissions_calculated] (PremiumTransactionId);

-- =============================================================================
-- Stage 8: Assignments Applied
-- Applies commission assignment redirections
-- =============================================================================
DROP TABLE IF EXISTS [etl].[calc_8_assignments_applied];
CREATE TABLE [etl].[calc_8_assignments_applied] (
    -- From Stage 7
    PremiumTransactionId NVARCHAR(100) NOT NULL,
    CertificateId BIGINT NOT NULL,
    TransactionDate DATE NOT NULL,
    PremiumAmount DECIMAL(18,2) NOT NULL,
    GroupId NVARCHAR(100),
    ProductCode NVARCHAR(100),
    [State] NVARCHAR(10),
    CertificateEffectiveDate DATE,
    GroupSize INT,
    IsFirstYear BIT NOT NULL,
    BasisYear INT NOT NULL,
    ProposalId NVARCHAR(100) NOT NULL,
    ProposalBrokerId BIGINT,
    SpecialCaseCode INT,
    SplitSequence INT NOT NULL,
    SplitPercent DECIMAL(18,4) NOT NULL,
    HierarchyId NVARCHAR(100),
    WritingBrokerId BIGINT,
    SplitPremiumAmount DECIMAL(18,2) NOT NULL,
    HierarchyVersionId NVARCHAR(100) NOT NULL,
    HierarchyEffectiveFrom DATE,
    HierarchyParticipantId NVARCHAR(100) NOT NULL,
    BrokerId BIGINT NOT NULL,
    BrokerName NVARCHAR(500),
    TierLevel INT NOT NULL,
    ScheduleCode NVARCHAR(200),
    PaidBrokerId BIGINT,
    ParticipantCommissionRate DECIMAL(18,4),
    RatePercent DECIMAL(18,4) NOT NULL,
    RateSource NVARCHAR(50) NOT NULL,
    ScheduleId BIGINT,
    ScheduleVersionId BIGINT,
    CommissionAmount DECIMAL(18,2) NOT NULL,
    
    -- Assignment resolution
    AssignmentVersionId NVARCHAR(100),
    AssigneeBrokerId BIGINT,
    AssigneeBrokerName NVARCHAR(500),
    TotalAssignedPercent DECIMAL(18,4) DEFAULT 0,
    AssignedAmount DECIMAL(18,2) DEFAULT 0,
    RetainedAmount DECIMAL(18,2) NOT NULL,
    
    -- Audit
    CreationTime DATETIME2 DEFAULT GETUTCDATE()
);
PRINT 'Created [etl].[calc_8_assignments_applied]';

CREATE NONCLUSTERED INDEX IX_calc_8_PremiumTransactionId 
ON [etl].[calc_8_assignments_applied] (PremiumTransactionId);

CREATE NONCLUSTERED INDEX IX_calc_8_BrokerId 
ON [etl].[calc_8_assignments_applied] (BrokerId);

-- =============================================================================
-- Output: GL Journal Entries
-- Final commission GL entries ready for export
-- =============================================================================
DROP TABLE IF EXISTS [etl].[calc_gl_journal_entries];
CREATE TABLE [etl].[calc_gl_journal_entries] (
    Id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
    PremiumTransactionId NVARCHAR(100) NOT NULL,
    PolicyId BIGINT NOT NULL,
    BrokerId BIGINT NOT NULL,
    BrokerName NVARCHAR(500),
    PremiumAmount DECIMAL(18,2) NOT NULL,
    CommissionAmount DECIMAL(18,2) NOT NULL,
    RatePercent DECIMAL(18,4) NOT NULL,
    TransactionDate DATE NOT NULL,
    ProductCode NVARCHAR(100),
    [State] NVARCHAR(10),
    GroupId NVARCHAR(100),
    HierarchyId NVARCHAR(100),
    HierarchyVersionId NVARCHAR(100),
    SplitSequence INT,
    SplitPercent DECIMAL(18,4),
    TierLevel INT,
    IsFirstYear BIT,
    BasisYear INT,
    RateSource NVARCHAR(50),
    EntryType NVARCHAR(20) NOT NULL,  -- 'Original' or 'Assigned'
    SourceBrokerId BIGINT,  -- For assigned entries, the original broker
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    CONSTRAINT PK_calc_gl_journal_entries PRIMARY KEY (Id)
);
PRINT 'Created [etl].[calc_gl_journal_entries]';

CREATE NONCLUSTERED INDEX IX_calc_gl_TransactionDate 
ON [etl].[calc_gl_journal_entries] (TransactionDate);

CREATE NONCLUSTERED INDEX IX_calc_gl_BrokerId 
ON [etl].[calc_gl_journal_entries] (BrokerId);

CREATE NONCLUSTERED INDEX IX_calc_gl_PolicyId 
ON [etl].[calc_gl_journal_entries] (PolicyId);

-- =============================================================================
-- Output: Commission Traceability Reports (One per Premium Payment)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[calc_traceability];
CREATE TABLE [etl].[calc_traceability] (
    Id NVARCHAR(100) NOT NULL,
    PremiumTransactionId NVARCHAR(100) NOT NULL,
    PolicyId BIGINT NOT NULL,
    TransactionDate DATE NOT NULL,
    PremiumAmount DECIMAL(18,2) NOT NULL,
    TotalCommission DECIMAL(18,2) NOT NULL,
    
    -- Traceability JSON (contains full pipeline audit trail)
    TraceabilityJson NVARCHAR(MAX),
    TraceabilityMarkdown NVARCHAR(MAX),
    
    -- Summary fields for quick filtering
    ProposalId NVARCHAR(100),
    GroupId NVARCHAR(100),
    ProductCode NVARCHAR(100),
    [State] NVARCHAR(10),
    IsFirstYear BIT,
    BasisYear INT,
    HierarchyCount INT,
    ParticipantCount INT,
    HasAssignments BIT,
    HasErrors BIT,
    ErrorMessages NVARCHAR(MAX),
    
    -- Source tracking
    IsBootstrap BIT DEFAULT 0,
    IsClean BIT DEFAULT 1,
    SourceType NVARCHAR(50) DEFAULT 'SqlCalculation',
    
    -- Audit
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    CONSTRAINT PK_calc_traceability PRIMARY KEY (Id)
);
PRINT 'Created [etl].[calc_traceability]';

CREATE NONCLUSTERED INDEX IX_calc_traceability_TransactionDate 
ON [etl].[calc_traceability] (TransactionDate);

CREATE NONCLUSTERED INDEX IX_calc_traceability_PolicyId 
ON [etl].[calc_traceability] (PolicyId);

-- =============================================================================
-- Output: Broker Traceabilities (One per GL Entry / Broker Payment)
-- =============================================================================
DROP TABLE IF EXISTS [etl].[calc_broker_traceabilities];
CREATE TABLE [etl].[calc_broker_traceabilities] (
    Id NVARCHAR(100) NOT NULL,
    CommissionTraceabilityReportId NVARCHAR(100) NOT NULL,
    
    -- Broker info
    BrokerId BIGINT NOT NULL,
    BrokerName NVARCHAR(500),
    
    -- Position in hierarchy
    [Level] INT,
    LevelName NVARCHAR(100),
    SplitSequence INT,
    SplitPercent DECIMAL(18,4),
    
    -- Commission calculation
    RatePercent DECIMAL(18,4),
    RateSource NVARCHAR(50),
    CommissionAmount DECIMAL(18,2) NOT NULL,
    
    -- Hierarchy reference
    HierarchyId NVARCHAR(100),
    HierarchyVersionId NVARCHAR(100),
    HierarchyParticipantId NVARCHAR(100),
    
    -- Schedule reference
    ScheduleId BIGINT,
    ScheduleVersionId BIGINT,
    ScheduleCode NVARCHAR(200),
    
    -- Assignment info
    IsAssigned BIT DEFAULT 0,
    AssignedFromBrokerId BIGINT,
    AssignmentVersionId NVARCHAR(100),
    
    -- Entry type
    EntryType NVARCHAR(20) NOT NULL,  -- 'Original' or 'Assigned'
    
    -- Audit
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    CONSTRAINT PK_calc_broker_traceabilities PRIMARY KEY (Id)
);
PRINT 'Created [etl].[calc_broker_traceabilities]';

CREATE NONCLUSTERED INDEX IX_calc_broker_traceabilities_ReportId 
ON [etl].[calc_broker_traceabilities] (CommissionTraceabilityReportId);

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT 'Calculation pipeline tables created:';
SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('etl') AND name LIKE 'calc_%' ORDER BY name;

PRINT '';
PRINT '============================================================';
PRINT 'CALCULATION PIPELINE TABLES CREATED SUCCESSFULLY';
PRINT '============================================================';

GO

