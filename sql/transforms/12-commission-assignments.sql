-- ================================================================
-- Transform: Commission Assignments
-- ================================================================
-- Captures commission redirections where SplitBrokerId != PaidBrokerId
-- Creates CommissionAssignmentVersions and CommissionAssignmentRecipients
-- ================================================================

SET NOCOUNT ON;
GO

PRINT '================================================================';
PRINT 'Transform: Commission Assignments';
PRINT '================================================================';
PRINT '';

-- ================================================================
-- Step 1: Create staging tables for assignments
-- ================================================================

PRINT 'Step 1: Creating staging tables...';

IF OBJECT_ID('$(ETL_SCHEMA).stg_commission_assignment_versions', 'U') IS NOT NULL
    DROP TABLE $(ETL_SCHEMA).stg_commission_assignment_versions;

CREATE TABLE $(ETL_SCHEMA).stg_commission_assignment_versions (
    Id NVARCHAR(100) NOT NULL PRIMARY KEY,
    BrokerId BIGINT NOT NULL,
    BrokerName NVARCHAR(255),
    ProposalId NVARCHAR(100),
    GroupId NVARCHAR(100),
    HierarchyId NVARCHAR(100),
    HierarchyVersionId NVARCHAR(100),
    HierarchyParticipantId NVARCHAR(100),
    VersionNumber NVARCHAR(40),
    EffectiveFrom DATETIME2 NOT NULL,
    EffectiveTo DATETIME2,
    Status INT NOT NULL DEFAULT 1, -- Active
    Type INT NOT NULL DEFAULT 1, -- Full assignment
    ChangeDescription NVARCHAR(1000),
    TotalAssignedPercent DECIMAL(5,2) NOT NULL DEFAULT 100.00,
    CreationTime DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

IF OBJECT_ID('$(ETL_SCHEMA).stg_commission_assignment_recipients', 'U') IS NOT NULL
    DROP TABLE $(ETL_SCHEMA).stg_commission_assignment_recipients;

CREATE TABLE $(ETL_SCHEMA).stg_commission_assignment_recipients (
    Id NVARCHAR(200) NOT NULL PRIMARY KEY, -- Shortened to avoid index warning
    VersionId NVARCHAR(100) NOT NULL,
    RecipientBrokerId BIGINT NOT NULL,
    RecipientName NVARCHAR(255),
    RecipientNPN NVARCHAR(40),
    Percentage DECIMAL(5,2) NOT NULL DEFAULT 100.00,
    RecipientHierarchyId NVARCHAR(100),
    Notes NVARCHAR(1000)
);

PRINT '  ✓ Staging tables created';
PRINT '';

-- ================================================================
-- Step 2: Extract unique assignments from source data
-- ================================================================

PRINT 'Step 2: Extracting assignments from source...';

-- Create temp table for assignments
IF OBJECT_ID('tempdb..#assignments', 'U') IS NOT NULL DROP TABLE #assignments;

SELECT 
    ci.SplitBrokerId,
    ci.PaidBrokerId,
    MIN(ci.CertEffectiveDate) as MinEffectiveDate,
    MAX(ci.CertEffectiveDate) as MaxEffectiveDate,
    COUNT(*) as AssignmentCount,
    COUNT(DISTINCT ci.CertificateId) as AffectedCertificates
INTO #assignments
FROM new_data.CertificateInfo ci
WHERE ci.CertStatus = 'A'
  AND ci.RecStatus = 'A'
  AND ci.SplitBrokerId IS NOT NULL
  AND ci.PaidBrokerId IS NOT NULL
  AND ci.SplitBrokerId <> ci.PaidBrokerId
GROUP BY ci.SplitBrokerId, ci.PaidBrokerId;

DECLARE @assignmentCount INT = @@ROWCOUNT;
PRINT '  ✓ Found ' + CAST(@assignmentCount AS VARCHAR) + ' unique assignments';
PRINT '';

-- ================================================================
-- Step 3: Generate CommissionAssignmentVersions
-- ================================================================

PRINT 'Step 3: Generating CommissionAssignmentVersions...';

INSERT INTO $(ETL_SCHEMA).stg_commission_assignment_versions (
    Id,
    BrokerId,
    BrokerName,
    ProposalId,
    GroupId,
    HierarchyId,
    HierarchyVersionId,
    HierarchyParticipantId,
    VersionNumber,
    EffectiveFrom,
    EffectiveTo,
    Status,
    Type,
    ChangeDescription,
    TotalAssignedPercent,
    CreationTime
)
SELECT 
    CONCAT('CAV-', bs.Id, '-', br.Id) as Id,
    bs.Id as BrokerId,
    bs.Name as BrokerName,
    'BROKER-LEVEL' as ProposalId, -- Broker-level assignment (not proposal-specific)
    NULL as GroupId,
    NULL as HierarchyId,
    NULL as HierarchyVersionId,
    NULL as HierarchyParticipantId,
    '1' as VersionNumber,
    a.MinEffectiveDate as EffectiveFrom,
    CASE 
        WHEN a.MaxEffectiveDate < DATEADD(YEAR, -1, GETDATE()) THEN a.MaxEffectiveDate
        ELSE NULL -- Still active
    END as EffectiveTo,
    1 as Status, -- Active
    1 as Type, -- Full assignment
    CONCAT('Commission assignment from ', bs.Name, ' to ', br.Name, 
           ' affecting ', a.AffectedCertificates, ' certificates') as ChangeDescription,
    100.00 as TotalAssignedPercent,
    GETUTCDATE() as CreationTime
FROM #assignments a
INNER JOIN dbo.Brokers bs ON bs.ExternalPartyId = a.SplitBrokerId -- Source broker
LEFT JOIN dbo.Brokers br ON br.ExternalPartyId = a.PaidBrokerId; -- Recipient broker

DECLARE @versionCount INT = @@ROWCOUNT;
PRINT '  ✓ Generated ' + CAST(@versionCount AS VARCHAR) + ' CommissionAssignmentVersions';
PRINT '';

-- ================================================================
-- Step 4: Generate CommissionAssignmentRecipients
-- ================================================================

PRINT 'Step 4: Generating CommissionAssignmentRecipients...';

INSERT INTO $(ETL_SCHEMA).stg_commission_assignment_recipients (
    Id,
    VersionId,
    RecipientBrokerId,
    RecipientName,
    RecipientNPN,
    Percentage,
    RecipientHierarchyId,
    Notes
)
SELECT 
    CONCAT('CAR-', bs.Id, '-', br.Id) as Id,
    CONCAT('CAV-', bs.Id, '-', br.Id) as VersionId,
    br.Id as RecipientBrokerId,
    br.Name as RecipientName,
    br.NPN as RecipientNPN,
    100.00 as Percentage, -- Full assignment (100%)
    NULL as RecipientHierarchyId,
    CONCAT('Receives commissions from ', bs.Name, ' for ', 
           a.AffectedCertificates, ' certificates') as Notes
FROM #assignments a
INNER JOIN dbo.Brokers bs ON bs.ExternalPartyId = a.SplitBrokerId
INNER JOIN dbo.Brokers br ON br.ExternalPartyId = a.PaidBrokerId
WHERE br.Id IS NOT NULL; -- Only if recipient broker exists

DECLARE @recipientCount INT = @@ROWCOUNT;
PRINT '  ✓ Generated ' + CAST(@recipientCount AS VARCHAR) + ' CommissionAssignmentRecipients';
PRINT '';

-- ================================================================
-- Step 5: Summary statistics
-- ================================================================

PRINT '================================================================';
PRINT 'Summary Statistics:';
PRINT '================================================================';

SELECT 
    'Unique Source Brokers' as Metric,
    COUNT(DISTINCT BrokerId) as Count
FROM $(ETL_SCHEMA).stg_commission_assignment_versions
UNION ALL
SELECT 
    'Unique Recipient Brokers',
    COUNT(DISTINCT RecipientBrokerId)
FROM $(ETL_SCHEMA).stg_commission_assignment_recipients
UNION ALL
SELECT 
    'Total Assignment Versions',
    COUNT(*)
FROM $(ETL_SCHEMA).stg_commission_assignment_versions
UNION ALL
SELECT 
    'Total Recipients',
    COUNT(*)
FROM $(ETL_SCHEMA).stg_commission_assignment_recipients;

PRINT '';
PRINT '✓ Commission Assignments Transform Complete';
PRINT '================================================================';

-- Cleanup
DROP TABLE #assignments;
GO
