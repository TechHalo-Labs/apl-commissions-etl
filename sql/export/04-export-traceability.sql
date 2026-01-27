-- =====================================================
-- Export Traceability from etl calc to dbo
-- =====================================================

-- Clear existing traceability for this run
PRINT 'Clearing existing traceability for SQL-ETL-V5...';
DELETE FROM [$(PRODUCTION_SCHEMA)].[BrokerTraceabilities] WHERE CommissionRunId = 'SQL-ETL-V5';
DELETE FROM [$(PRODUCTION_SCHEMA)].[CommissionTraceabilityReports] WHERE CommissionRunId = 'SQL-ETL-V5';
PRINT 'Cleared existing traceability';
GO

-- Export CommissionTraceabilityReports
PRINT 'Exporting CommissionTraceabilityReports...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[CommissionTraceabilityReports] (
    CommissionRunId, CertificateId, PolicyId, PremiumTransactionId,
    IsBootstrap, TraceabilityData, MarkdownReport, PremiumAmount, TotalCommission,
    SplitCount, PayeeCount, HasAssignments, [Status], MacViolation, FailureReason,
    ProcessedAt, IsClean, GLGenerationStatus, BasisYear, CreationTime, IsDeleted
)
SELECT 
    'SQL-ETL-V5' AS CommissionRunId,
    t.PolicyId AS CertificateId,
    CAST(t.PolicyId AS NVARCHAR(100)) AS PolicyId,
    NULL AS PremiumTransactionId,  -- Avoid FK issues
    t.IsBootstrap,
    t.TraceabilityJson AS TraceabilityData,
    t.TraceabilityMarkdown AS MarkdownReport,
    t.PremiumAmount,
    t.TotalCommission,
    t.HierarchyCount AS SplitCount,
    t.ParticipantCount AS PayeeCount,
    t.HasAssignments,
    CASE WHEN t.HasErrors = 1 THEN 'Failed' ELSE 'Success' END AS [Status],
    0 AS MacViolation,
    t.ErrorMessages AS FailureReason,
    GETUTCDATE() AS ProcessedAt,
    t.IsClean,
    'Completed' AS GLGenerationStatus,
    t.BasisYear,
    t.CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[calc_traceability] t
WHERE t.PolicyId IS NOT NULL;

DECLARE @traceCount INT;
SELECT @traceCount = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[CommissionTraceabilityReports] WHERE CommissionRunId = 'SQL-ETL-V5';
PRINT 'Traceability Reports exported: ' + CAST(@traceCount AS VARCHAR);
GO

-- Export BrokerTraceabilities (from GL entries directly)
PRINT 'Exporting BrokerTraceabilities...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[BrokerTraceabilities] (
    BrokerId, CommissionTraceabilityReportId, CommissionRunId, GroupId,
    [Level], LevelName, SplitSequence, SplitPercent, RatePercent, CommissionAmount,
    EarnerBrokerId, PaidBrokerId, PolicyId,
    HierarchyId, HierarchyVersionId, BrokerName,
    CreationTime, IsDeleted
)
SELECT 
    gl.BrokerId,
    ctr.Id AS CommissionTraceabilityReportId,
    'SQL-ETL-V5' AS CommissionRunId,
    gl.GroupId,
    gl.TierLevel AS [Level],
    CONCAT('Level ', gl.TierLevel) AS LevelName,
    gl.SplitSequence,
    gl.SplitPercent,
    gl.RatePercent,
    gl.CommissionAmount,
    CASE WHEN gl.EntryType = 'Assigned' THEN gl.SourceBrokerId ELSE gl.BrokerId END AS EarnerBrokerId,
    gl.BrokerId AS PaidBrokerId,
    CAST(gl.PolicyId AS NVARCHAR(100)) AS PolicyId,
    gl.HierarchyId,
    gl.HierarchyVersionId,
    gl.BrokerName,
    gl.CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[calc_gl_journal_entries] gl
INNER JOIN [$(PRODUCTION_SCHEMA)].[CommissionTraceabilityReports] ctr 
    ON CAST(ctr.CertificateId AS NVARCHAR) = CAST(gl.PolicyId AS NVARCHAR)
    AND ctr.CommissionRunId = 'SQL-ETL-V5'
WHERE gl.BrokerId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Brokers]);

DECLARE @btCount INT;
SELECT @btCount = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[BrokerTraceabilities] WHERE CommissionRunId = 'SQL-ETL-V5';
PRINT 'Broker Traceabilities exported: ' + CAST(@btCount AS VARCHAR);
GO

-- Update CommissionRun broker traceabilities count
UPDATE [$(PRODUCTION_SCHEMA)].[CommissionRuns]
SET BrokerTraceabilitiesCount = (SELECT COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[BrokerTraceabilities] WHERE CommissionRunId = 'SQL-ETL-V5')
WHERE Id = 'SQL-ETL-V5';
GO

PRINT '=== Traceability Export Complete ===';

