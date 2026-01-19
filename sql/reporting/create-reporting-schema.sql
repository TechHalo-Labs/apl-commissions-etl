-- ============================================================================
-- APL Commissions Reporting Schema
-- Creates curated views for Stimulsoft Reports
-- ============================================================================

-- Create the reporting schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'reporting')
BEGIN
    EXEC('CREATE SCHEMA reporting');
END
GO

-- ============================================================================
-- 1. EarningBrokers - Brokers with recent commission activity
-- ============================================================================
IF OBJECT_ID('reporting.EarningBrokers', 'V') IS NOT NULL DROP VIEW reporting.EarningBrokers;
GO

CREATE VIEW reporting.EarningBrokers AS
SELECT 
    b.Id AS BrokerId,
    b.ExternalPartyId,
    b.Name AS BrokerName,
    b.FirstName,
    b.LastName,
    b.Email,
    b.Phone,
    b.Npn,
    b.Status,
    CASE b.Status
        WHEN 0 THEN 'Pending'
        WHEN 1 THEN 'Active'
        WHEN 2 THEN 'Inactive'
        WHEN 3 THEN 'Terminated'
        ELSE 'Unknown'
    END AS StatusName,
    b.Type,
    CASE b.Type
        WHEN 0 THEN 'Individual'
        WHEN 1 THEN 'Organization'
        ELSE 'Unknown'
    END AS TypeName,
    b.State,
    b.City,
    b.HierarchyLevel,
    b.AppointmentDate,
    b.DateContracted,
    earnings.DateMostRecentEarnedCommission,
    earnings.EarnedCommissionLast3Months,
    earnings.TotalEarnedCommission,
    earnings.TransactionCountLast3Months
FROM dbo.Brokers b
INNER JOIN (
    SELECT 
        BrokerId,
        MAX(EntryDate) AS DateMostRecentEarnedCommission,
        SUM(CASE WHEN EntryDate >= DATEADD(MONTH, -3, GETDATE()) THEN PremiumAmount * CommissionRate / 100.0 ELSE 0 END) AS EarnedCommissionLast3Months,
        SUM(PremiumAmount * CommissionRate / 100.0) AS TotalEarnedCommission,
        COUNT(CASE WHEN EntryDate >= DATEADD(MONTH, -3, GETDATE()) THEN 1 END) AS TransactionCountLast3Months
    FROM dbo.GLJournalEntries
    WHERE CommissionRate > 0
    GROUP BY BrokerId
) earnings ON b.Id = earnings.BrokerId
WHERE b.IsDeleted = 0;
GO

-- ============================================================================
-- 2. Brokers - All active brokers
-- ============================================================================
IF OBJECT_ID('reporting.Brokers', 'V') IS NOT NULL DROP VIEW reporting.Brokers;
GO

CREATE VIEW reporting.Brokers AS
SELECT 
    b.Id AS BrokerId,
    b.ExternalPartyId,
    b.Name AS BrokerName,
    b.FirstName,
    b.LastName,
    b.MiddleName,
    b.Suffix,
    b.Email,
    b.Phone,
    b.Npn,
    b.TaxId,
    b.Status,
    CASE b.Status
        WHEN 0 THEN 'Pending'
        WHEN 1 THEN 'Active'
        WHEN 2 THEN 'Inactive'
        WHEN 3 THEN 'Terminated'
        ELSE 'Unknown'
    END AS StatusName,
    b.Type,
    CASE b.Type
        WHEN 0 THEN 'Individual'
        WHEN 1 THEN 'Organization'
        ELSE 'Unknown'
    END AS TypeName,
    b.AddressLine1,
    b.AddressLine2,
    b.City,
    b.State,
    b.ZipCode,
    b.Country,
    b.HierarchyLevel,
    b.BrokerClassification,
    b.AppointmentDate,
    b.HireDate,
    b.DateContracted,
    b.UplineId,
    b.UplineName,
    b.DownlineCount,
    b.CreationTime,
    b.LastModificationTime
FROM dbo.Brokers b
WHERE b.IsDeleted = 0;
GO

-- ============================================================================
-- 3. BrokerEOInsurances - E&O Insurance details
-- ============================================================================
IF OBJECT_ID('reporting.BrokerEOInsurances', 'V') IS NOT NULL DROP VIEW reporting.BrokerEOInsurances;
GO

CREATE VIEW reporting.BrokerEOInsurances AS
SELECT 
    eo.Id AS EOInsuranceId,
    eo.BrokerId,
    b.Name AS BrokerName,
    b.Npn AS BrokerNPN,
    eo.Carrier AS InsuranceCompany,
    eo.PolicyNumber,
    eo.CoverageAmount,
    eo.MinimumRequired,
    eo.DeductibleAmount,
    eo.EffectiveDate,
    eo.ExpirationDate,
    eo.Status,
    CASE 
        WHEN eo.ExpirationDate < GETDATE() THEN 'Expired'
        WHEN eo.ExpirationDate < DATEADD(DAY, 30, GETDATE()) THEN 'Expiring Soon'
        ELSE 'Active'
    END AS EOStatusCalculated,
    eo.LastVerifiedDate,
    eo.CreationTime,
    eo.LastModificationTime
FROM dbo.BrokerEOInsurances eo
INNER JOIN dbo.Brokers b ON b.Id = eo.BrokerId
WHERE eo.IsDeleted = 0 AND b.IsDeleted = 0;
GO

-- ============================================================================
-- 4. BrokerLicenses - License details
-- ============================================================================
IF OBJECT_ID('reporting.BrokerLicenses', 'V') IS NOT NULL DROP VIEW reporting.BrokerLicenses;
GO

CREATE VIEW reporting.BrokerLicenses AS
SELECT 
    bl.Id AS LicenseId,
    bl.BrokerId,
    b.Name AS BrokerName,
    b.Npn AS BrokerNPN,
    bl.State AS LicenseState,
    bl.LicenseNumber,
    bl.Type AS LicenseType,
    bl.EffectiveDate,
    bl.ExpirationDate,
    CASE 
        WHEN bl.ExpirationDate < GETDATE() THEN 'Expired'
        WHEN bl.ExpirationDate < DATEADD(DAY, 30, GETDATE()) THEN 'Expiring Soon'
        ELSE 'Active'
    END AS LicenseStatus,
    bl.CreationTime,
    bl.LastModificationTime
FROM dbo.BrokerLicenses bl
INNER JOIN dbo.Brokers b ON b.Id = bl.BrokerId
WHERE bl.IsDeleted = 0 AND b.IsDeleted = 0;
GO

-- ============================================================================
-- 5. GroupCommissionRules (Proposals)
-- ============================================================================
IF OBJECT_ID('reporting.GroupCommissionRules', 'V') IS NOT NULL DROP VIEW reporting.GroupCommissionRules;
GO

CREATE VIEW reporting.GroupCommissionRules AS
SELECT 
    p.Id AS ProposalId,
    p.ProposalNumber,
    p.GroupId,
    g.GroupName,
    g.GroupNumber,
    g.GroupSize,
    g.SitusState AS GroupState,
    p.Status,
    CASE p.Status
        WHEN 0 THEN 'Draft'
        WHEN 1 THEN 'Active'
        WHEN 2 THEN 'Inactive'
        WHEN 3 THEN 'Expired'
        ELSE 'Unknown'
    END AS StatusName,
    p.EffectiveDate,
    p.ExpirationDate,
    p.SitusState AS ProposalState,
    p.SpecialCaseCode,
    CASE p.SpecialCaseCode
        WHEN 0 THEN 'Standard'
        WHEN 99 THEN 'Catch-All'
        ELSE 'Special Case'
    END AS SpecialCaseDescription,
    p.Notes,
    p.CreationTime,
    p.LastModificationTime
FROM dbo.Proposals p
LEFT JOIN dbo.[Group] g ON g.Id = p.GroupId
WHERE p.IsDeleted = 0;
GO

-- ============================================================================
-- 6. EarningBrokersWithLicenses - Earning brokers with license details
-- ============================================================================
IF OBJECT_ID('reporting.EarningBrokersWithLicenses', 'V') IS NOT NULL DROP VIEW reporting.EarningBrokersWithLicenses;
GO

CREATE VIEW reporting.EarningBrokersWithLicenses AS
SELECT 
    eb.BrokerId,
    eb.ExternalPartyId,
    eb.BrokerName,
    eb.FirstName,
    eb.LastName,
    eb.Email,
    eb.Npn,
    eb.StatusName,
    eb.TypeName,
    eb.State AS BrokerState,
    eb.City,
    eb.DateMostRecentEarnedCommission,
    eb.EarnedCommissionLast3Months,
    eb.TotalEarnedCommission,
    bl.State AS LicenseState,
    bl.LicenseNumber,
    bl.Type AS LicenseType,
    bl.EffectiveDate AS LicenseEffectiveDate,
    bl.ExpirationDate AS LicenseExpirationDate,
    CASE 
        WHEN bl.ExpirationDate < GETDATE() THEN 'Expired'
        WHEN bl.ExpirationDate < DATEADD(DAY, 30, GETDATE()) THEN 'Expiring Soon'
        ELSE 'Active'
    END AS LicenseStatus
FROM reporting.EarningBrokers eb
LEFT JOIN dbo.BrokerLicenses bl ON bl.BrokerId = eb.BrokerId AND bl.IsDeleted = 0;
GO

-- ============================================================================
-- 7. EarningBrokersWithEO - Earning brokers with E&O Insurance details
-- ============================================================================
IF OBJECT_ID('reporting.EarningBrokersWithEO', 'V') IS NOT NULL DROP VIEW reporting.EarningBrokersWithEO;
GO

CREATE VIEW reporting.EarningBrokersWithEO AS
SELECT 
    eb.BrokerId,
    eb.ExternalPartyId,
    eb.BrokerName,
    eb.FirstName,
    eb.LastName,
    eb.Email,
    eb.Npn,
    eb.StatusName,
    eb.TypeName,
    eb.State AS BrokerState,
    eb.City,
    eb.DateMostRecentEarnedCommission,
    eb.EarnedCommissionLast3Months,
    eb.TotalEarnedCommission,
    eo.Carrier AS InsuranceCompany,
    eo.PolicyNumber AS EOPolicyNumber,
    eo.CoverageAmount AS EOCoverageAmount,
    eo.EffectiveDate AS EOEffectiveDate,
    eo.ExpirationDate AS EOExpirationDate,
    CASE 
        WHEN eo.ExpirationDate < GETDATE() THEN 'Expired'
        WHEN eo.ExpirationDate < DATEADD(DAY, 30, GETDATE()) THEN 'Expiring Soon'
        WHEN eo.Id IS NULL THEN 'No E&O On File'
        ELSE 'Active'
    END AS EOStatus
FROM reporting.EarningBrokers eb
LEFT JOIN dbo.BrokerEOInsurances eo ON eo.BrokerId = eb.BrokerId AND eo.IsDeleted = 0;
GO

-- ============================================================================
-- 8. EarningBrokersWithAppointments - Earning brokers with appointment details
-- ============================================================================
IF OBJECT_ID('reporting.EarningBrokersWithAppointments', 'V') IS NOT NULL DROP VIEW reporting.EarningBrokersWithAppointments;
GO

CREATE VIEW reporting.EarningBrokersWithAppointments AS
SELECT 
    eb.BrokerId,
    eb.ExternalPartyId,
    eb.BrokerName,
    eb.FirstName,
    eb.LastName,
    eb.Email,
    eb.Npn,
    eb.StatusName,
    eb.TypeName,
    eb.State AS BrokerState,
    eb.City,
    eb.AppointmentDate,
    eb.DateContracted,
    eb.HierarchyLevel,
    eb.DateMostRecentEarnedCommission,
    eb.EarnedCommissionLast3Months,
    eb.TotalEarnedCommission,
    eb.TransactionCountLast3Months,
    DATEDIFF(YEAR, eb.AppointmentDate, GETDATE()) AS YearsWithCompany
FROM reporting.EarningBrokers eb;
GO

-- ============================================================================
-- 9. GLJournalEntriesDryRun - Commission calculation dry run results
-- ============================================================================
IF OBJECT_ID('reporting.GLJournalEntriesDryRun', 'V') IS NOT NULL DROP VIEW reporting.GLJournalEntriesDryRun;
GO

CREATE VIEW reporting.GLJournalEntriesDryRun AS
SELECT 
    gl.Id AS JournalEntryId,
    gl.JournalNumber,
    gl.BrokerId,
    b.Name AS BrokerName,
    b.Npn AS BrokerNPN,
    gl.PolicyId,
    pol.PolicyNumber,
    gl.GroupId,
    g.GroupName,
    gl.State,
    gl.EntryDate,
    gl.PostingDate,
    gl.PremiumAmount,
    gl.CommissionRate,
    gl.PremiumAmount * gl.CommissionRate / 100.0 AS CommissionAmount,
    gl.HierarchyTier AS TierLevel,
    gl.SplitSequence,
    gl.SplitPercentage AS SplitPercent,
    gl.HierarchyId,
    gl.ScheduleId,
    gl.MacApplied,
    gl.MacRate,
    gl.Description,
    gl.CreationTime
FROM dbo.GLJournalEntriesDryRun gl
LEFT JOIN dbo.Brokers b ON b.Id = gl.BrokerId
LEFT JOIN dbo.[Group] g ON g.Id = gl.GroupId
LEFT JOIN dbo.Policies pol ON pol.Id = gl.PolicyId;
GO

-- ============================================================================
-- 10. GLJournalEntries - Commission journal entries (production)
-- ============================================================================
IF OBJECT_ID('reporting.GLJournalEntries', 'V') IS NOT NULL DROP VIEW reporting.GLJournalEntries;
GO

CREATE VIEW reporting.GLJournalEntries AS
SELECT 
    gl.Id AS JournalEntryId,
    gl.JournalNumber,
    gl.BrokerId,
    b.Name AS BrokerName,
    b.Npn AS BrokerNPN,
    gl.PolicyId,
    pol.PolicyNumber,
    gl.GroupId,
    g.GroupName,
    gl.State,
    gl.EntryDate,
    gl.PostingDate,
    gl.PremiumAmount,
    gl.CommissionRate,
    gl.PremiumAmount * gl.CommissionRate / 100.0 AS CommissionAmount,
    gl.HierarchyTier AS TierLevel,
    gl.SplitSequence,
    gl.SplitPercentage AS SplitPercent,
    gl.HierarchyId,
    gl.ScheduleId,
    gl.MacApplied,
    gl.MacRate,
    gl.Description,
    gl.PaymentId,
    gl.CreationTime
FROM dbo.GLJournalEntries gl
LEFT JOIN dbo.Brokers b ON b.Id = gl.BrokerId
LEFT JOIN dbo.[Group] g ON g.Id = gl.GroupId
LEFT JOIN dbo.Policies pol ON pol.Id = gl.PolicyId;
GO

-- ============================================================================
-- 11. PremiumTransactions - Premium payment transactions
-- ============================================================================
IF OBJECT_ID('reporting.PremiumTransactions', 'V') IS NOT NULL DROP VIEW reporting.PremiumTransactions;
GO

CREATE VIEW reporting.PremiumTransactions AS
SELECT 
    pt.Id AS TransactionId,
    pt.sourcePolicyId AS PolicyId,
    pol.PolicyNumber,
    pt.certificateId AS CertificateId,
    pt.transactionDate AS TransactionDate,
    pt.premiumAmount AS PremiumAmount,
    pt.paymentStatus AS PaymentStatus,
    pol.GroupId,
    g.GroupName,
    pol.ProductCode,
    pol.State,
    pt.CreationTime
FROM dbo.PremiumTransactions pt
LEFT JOIN dbo.Policies pol ON pol.Id = pt.sourcePolicyId
LEFT JOIN dbo.[Group] g ON g.Id = pol.GroupId;
GO

-- ============================================================================
-- 12. Payments - Payment records
-- ============================================================================
IF OBJECT_ID('reporting.Payments', 'V') IS NOT NULL DROP VIEW reporting.Payments;
GO

CREATE VIEW reporting.Payments AS
SELECT 
    p.Id AS PaymentId,
    p.BrokerId,
    b.Name AS BrokerName,
    b.Npn AS BrokerNPN,
    p.ScheduledDate,
    p.PaidDate AS PaymentDate,
    p.Amount AS PaymentAmount,
    p.PaymentMethod,
    CASE p.PaymentMethod
        WHEN 0 THEN 'Check'
        WHEN 1 THEN 'ACH'
        WHEN 2 THEN 'Wire'
        ELSE 'Unknown'
    END AS PaymentMethodName,
    p.Status AS PaymentStatus,
    CASE p.Status
        WHEN 0 THEN 'Pending'
        WHEN 1 THEN 'Processed'
        WHEN 2 THEN 'Failed'
        WHEN 3 THEN 'Cancelled'
        ELSE 'Unknown'
    END AS PaymentStatusName,
    p.ReferenceNumber,
    p.AccountLastFour,
    p.Notes,
    p.PaymentBatchId,
    p.CommissionRunId,
    p.CreationTime
FROM dbo.Payments p
LEFT JOIN dbo.Brokers b ON b.Id = p.BrokerId
WHERE p.IsDeleted = 0;
GO

-- ============================================================================
-- 13. Schedules - Commission rate schedules
-- ============================================================================
IF OBJECT_ID('reporting.Schedules', 'V') IS NOT NULL DROP VIEW reporting.Schedules;
GO

CREATE VIEW reporting.Schedules AS
SELECT 
    s.Id AS ScheduleId,
    s.ExternalId,
    s.Name AS ScheduleName,
    s.Description,
    s.Status,
    CASE s.Status
        WHEN 0 THEN 'Draft'
        WHEN 1 THEN 'Active'
        WHEN 2 THEN 'Inactive'
        ELSE 'Unknown'
    END AS StatusName,
    s.CommissionType,
    s.RateStructure,
    s.EffectiveDate,
    s.EndDate AS ExpirationDate,
    s.ProductLines,
    s.ProductCodes,
    s.BrokerSegment,
    s.ContractCount,
    s.ProductCount,
    sv.versionNumber AS CurrentVersion,
    sr.ProductCode,
    sr.State,
    sr.GroupSize,
    sr.GroupSizeFrom,
    sr.GroupSizeTo,
    sr.FirstYearRate,
    sr.RenewalRate,
    sr.BonusRate,
    sr.OverrideRate,
    s.CreationTime,
    s.LastModificationTime
FROM dbo.Schedules s
LEFT JOIN dbo.ScheduleVersions sv ON sv.scheduleId = s.Id AND sv.IsDeleted = 0 AND sv.Id = s.CurrentVersionId
LEFT JOIN dbo.ScheduleRates sr ON sr.ScheduleVersionId = sv.Id AND sr.IsDeleted = 0
WHERE s.IsDeleted = 0;
GO

-- ============================================================================
-- 14. GroupCommissionRulesWithEarners - Full commission hierarchy details
-- ============================================================================
IF OBJECT_ID('reporting.GroupCommissionRulesWithEarners', 'V') IS NOT NULL DROP VIEW reporting.GroupCommissionRulesWithEarners;
GO

CREATE VIEW reporting.GroupCommissionRulesWithEarners AS
SELECT 
    -- Proposal/Group Info
    p.Id AS ProposalId,
    p.ProposalNumber,
    p.GroupId,
    g.GroupName,
    g.GroupNumber,
    g.GroupSize,
    g.SitusState AS GroupState,
    CASE p.Status
        WHEN 0 THEN 'Draft'
        WHEN 1 THEN 'Active'
        WHEN 2 THEN 'Inactive'
        WHEN 3 THEN 'Expired'
        ELSE 'Unknown'
    END AS ProposalStatus,
    p.EffectiveDate AS ProposalEffectiveDate,
    p.ExpirationDate AS ProposalExpirationDate,
    
    -- Split Configuration
    psv.Id AS SplitVersionId,
    psv.TotalSplitPercent,
    psp.SplitPercent AS ParticipantSplitPercent,
    
    -- Hierarchy Info
    h.Id AS HierarchyId,
    h.Name AS HierarchyName,
    hv.effectiveDate AS HierarchyEffectiveDate,
    
    -- Hierarchy Participant (Earning Broker)
    hp.Id AS ParticipantId,
    hp.EntityId AS BrokerId,
    hp.EntityName AS BrokerName,
    b.Npn AS BrokerNPN,
    hp.Level AS TierLevel,
    hp.CommissionRate AS ParticipantRate,
    
    -- Schedule Info
    hp.ScheduleId,
    s.Name AS ScheduleName
    
FROM dbo.Proposals p
LEFT JOIN dbo.[Group] g ON g.Id = p.GroupId
LEFT JOIN dbo.PremiumSplitVersions psv ON psv.ProposalId = p.Id AND psv.IsDeleted = 0
LEFT JOIN dbo.PremiumSplitParticipants psp ON psp.VersionId = psv.Id
LEFT JOIN dbo.Hierarchies h ON h.Id = psp.HierarchyId AND h.IsDeleted = 0
LEFT JOIN dbo.HierarchyVersions hv ON hv.HierarchyId = h.Id AND hv.IsDeleted = 0
LEFT JOIN dbo.HierarchyParticipants hp ON hp.HierarchyVersionId = hv.Id AND hp.IsDeleted = 0
LEFT JOIN dbo.Brokers b ON b.Id = hp.EntityId
LEFT JOIN dbo.Schedules s ON s.Id = hp.ScheduleId
WHERE p.IsDeleted = 0;
GO

-- ============================================================================
-- 15. BrokersWithAssignments - Brokers with their commission assignments
-- ============================================================================
IF OBJECT_ID('reporting.BrokersWithAssignments', 'V') IS NOT NULL DROP VIEW reporting.BrokersWithAssignments;
GO

CREATE VIEW reporting.BrokersWithAssignments AS
SELECT 
    b.Id AS BrokerId,
    b.ExternalPartyId,
    b.Name AS BrokerName,
    b.FirstName,
    b.LastName,
    b.Email,
    b.Npn,
    b.Status,
    CASE b.Status
        WHEN 0 THEN 'Pending'
        WHEN 1 THEN 'Active'
        WHEN 2 THEN 'Inactive'
        WHEN 3 THEN 'Terminated'
        ELSE 'Unknown'
    END AS StatusName,
    b.State AS BrokerState,
    b.City,
    
    -- Assignment Details
    cav.Id AS AssignmentId,
    cav.BrokerName AS AssignedFromBroker,
    cav.ProposalId,
    cav.GroupId,
    cav.HierarchyId,
    cav.VersionNumber AS AssignmentVersion,
    cav.EffectiveFrom AS AssignmentEffectiveDate,
    cav.EffectiveTo AS AssignmentExpirationDate,
    cav.Status AS AssignmentStatus,
    cav.Type AS AssignmentType,
    cav.TotalAssignedPercent,
    cav.ChangeDescription AS AssignmentNotes,
    
    -- Incoming Assignments Count
    incoming.IncomingAssignmentCount,
    incoming.TotalIncomingPercent
    
FROM dbo.Brokers b
LEFT JOIN dbo.CommissionAssignmentVersions cav ON cav.BrokerId = b.Id AND cav.IsDeleted = 0
LEFT JOIN (
    SELECT 
        BrokerId,
        COUNT(*) AS IncomingAssignmentCount,
        SUM(TotalAssignedPercent) AS TotalIncomingPercent
    FROM dbo.CommissionAssignmentVersions
    WHERE IsDeleted = 0 
    GROUP BY BrokerId
) incoming ON incoming.BrokerId = b.Id
WHERE b.IsDeleted = 0;
GO

-- ============================================================================
-- Summary View: Available Report Views
-- ============================================================================
IF OBJECT_ID('reporting.AvailableViews', 'V') IS NOT NULL DROP VIEW reporting.AvailableViews;
GO

CREATE VIEW reporting.AvailableViews AS
SELECT TOP 100
    TABLE_NAME AS ViewName,
    'reporting' AS SchemaName,
    CASE TABLE_NAME
        WHEN 'EarningBrokers' THEN 'Brokers with recent commission activity (last earned date, 3-month totals)'
        WHEN 'Brokers' THEN 'All active brokers with contact and status information'
        WHEN 'BrokerEOInsurances' THEN 'E&O Insurance details for all brokers'
        WHEN 'BrokerLicenses' THEN 'License details for all brokers'
        WHEN 'GroupCommissionRules' THEN 'Commission proposals/rules by group'
        WHEN 'EarningBrokersWithLicenses' THEN 'Earning brokers joined with their license details'
        WHEN 'EarningBrokersWithEO' THEN 'Earning brokers joined with E&O insurance details'
        WHEN 'EarningBrokersWithAppointments' THEN 'Earning brokers with appointment/tenure information'
        WHEN 'GLJournalEntriesDryRun' THEN 'Commission calculation dry run results'
        WHEN 'GLJournalEntries' THEN 'Production commission journal entries'
        WHEN 'PremiumTransactions' THEN 'Premium payment transactions'
        WHEN 'Payments' THEN 'Payment records to brokers'
        WHEN 'Schedules' THEN 'Commission rate schedules with rates'
        WHEN 'GroupCommissionRulesWithEarners' THEN 'Full commission hierarchy: proposals to splits to hierarchies to participants'
        WHEN 'BrokersWithAssignments' THEN 'Brokers with their commission assignment details'
        WHEN 'AvailableViews' THEN 'This list of available reporting views'
        ELSE 'No description available'
    END AS Description
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'reporting';
GO

-- ============================================================================
-- Grant SELECT on reporting schema to reporting user (run separately if needed)
-- ============================================================================
-- CREATE USER ReportingUser FOR LOGIN ReportingLogin;
-- GRANT SELECT ON SCHEMA::reporting TO ReportingUser;
-- GO

PRINT 'Reporting schema created successfully!';
GO
