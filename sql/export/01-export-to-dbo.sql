-- =============================================================================
-- Export ETL Results to Production Tables (SQL Server)
-- =============================================================================
-- Exports calculated commission data from [etl] schema to [dbo] schema
-- This moves the ETL results into production tables
-- 
-- WARNING: This will INSERT new records. Ensure appropriate handling of 
-- duplicates/conflicts for your use case.
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'EXPORT TO PRODUCTION TABLES';
PRINT '============================================================';
PRINT 'Start Time: ' + CONVERT(VARCHAR, GETUTCDATE(), 121);
PRINT '';

-- =============================================================================
-- Pre-export validation
-- =============================================================================
PRINT 'Validating ETL data...';

DECLARE @glCount INT = (SELECT COUNT(*) FROM [etl].[calc_gl_journal_entries]);
DECLARE @traceCount INT = (SELECT COUNT(*) FROM [etl].[calc_traceability]);
DECLARE @brokerTraceCount INT = (SELECT COUNT(*) FROM [etl].[calc_broker_traceabilities]);

PRINT 'GL Journal Entries to export: ' + CAST(@glCount AS VARCHAR);
PRINT 'Traceability Reports to export: ' + CAST(@traceCount AS VARCHAR);
PRINT 'Broker Traceabilities to export: ' + CAST(@brokerTraceCount AS VARCHAR);

IF @glCount = 0
BEGIN
    PRINT 'WARNING: No GL entries to export. Run calculation pipeline first.';
END

PRINT '';

GO

-- =============================================================================
-- Export GL Journal Entries
-- =============================================================================
PRINT 'Exporting GL Journal Entries to [dbo].[GLJournalEntries]...';

-- Note: Adjust target table/column names to match your production schema
-- This is a template - update based on actual production table structure

/*
INSERT INTO [dbo].[GLJournalEntries] (
    Id,
    PremiumTransactionId,
    PolicyId,
    BrokerId,
    BrokerName,
    PremiumAmount,
    CommissionAmount,
    RatePercent,
    TransactionDate,
    ProductCode,
    [State],
    GroupId,
    HierarchyId,
    HierarchyVersionId,
    SplitSequence,
    SplitPercent,
    TierLevel,
    IsFirstYear,
    BasisYear,
    RateSource,
    EntryType,
    SourceBrokerId,
    CreationTime
)
SELECT 
    NEWID() AS Id,  -- Generate new ID if needed
    PremiumTransactionId,
    PolicyId,
    BrokerId,
    BrokerName,
    PremiumAmount,
    CommissionAmount,
    RatePercent,
    TransactionDate,
    ProductCode,
    [State],
    GroupId,
    HierarchyId,
    HierarchyVersionId,
    SplitSequence,
    SplitPercent,
    TierLevel,
    IsFirstYear,
    BasisYear,
    RateSource,
    EntryType,
    SourceBrokerId,
    GETUTCDATE() AS CreationTime
FROM [etl].[calc_gl_journal_entries];

PRINT 'GL Journal Entries exported: ' + CAST(@@ROWCOUNT AS VARCHAR);
*/

-- Placeholder - uncomment and customize above once production schema is confirmed
PRINT 'GL Journal Entries export: SKIPPED (uncomment when ready)';

GO

-- =============================================================================
-- Export Traceability Reports
-- =============================================================================
PRINT '';
PRINT 'Exporting Traceability Reports to [dbo].[CommissionTraceabilityReports]...';

/*
INSERT INTO [dbo].[CommissionTraceabilityReports] (
    Id,
    PremiumTransactionId,
    PolicyId,
    TransactionDate,
    PremiumAmount,
    TotalCommission,
    TraceabilityJson,
    TraceabilityMarkdown,
    ProposalId,
    GroupId,
    ProductCode,
    [State],
    IsFirstYear,
    BasisYear,
    HierarchyCount,
    ParticipantCount,
    HasAssignments,
    HasErrors,
    ErrorMessages,
    IsBootstrap,
    IsClean,
    SourceType,
    CreationTime
)
SELECT 
    NEWID() AS Id,
    PremiumTransactionId,
    PolicyId,
    TransactionDate,
    PremiumAmount,
    TotalCommission,
    TraceabilityJson,
    TraceabilityMarkdown,
    ProposalId,
    GroupId,
    ProductCode,
    [State],
    IsFirstYear,
    BasisYear,
    HierarchyCount,
    ParticipantCount,
    HasAssignments,
    HasErrors,
    ErrorMessages,
    IsBootstrap,
    IsClean,
    SourceType,
    GETUTCDATE() AS CreationTime
FROM [etl].[calc_traceability];

PRINT 'Traceability Reports exported: ' + CAST(@@ROWCOUNT AS VARCHAR);
*/

PRINT 'Traceability Reports export: SKIPPED (uncomment when ready)';

GO

-- =============================================================================
-- Export Broker Traceabilities
-- =============================================================================
PRINT '';
PRINT 'Exporting Broker Traceabilities to [dbo].[BrokerTraceabilities]...';

/*
INSERT INTO [dbo].[BrokerTraceabilities] (
    Id,
    CommissionTraceabilityReportId,
    BrokerId,
    BrokerName,
    [Level],
    LevelName,
    SplitSequence,
    SplitPercent,
    RatePercent,
    RateSource,
    CommissionAmount,
    HierarchyId,
    HierarchyVersionId,
    HierarchyParticipantId,
    ScheduleId,
    ScheduleVersionId,
    ScheduleCode,
    IsAssigned,
    AssignedFromBrokerId,
    AssignmentVersionId,
    EntryType,
    CreationTime
)
SELECT 
    NEWID() AS Id,
    CommissionTraceabilityReportId,
    BrokerId,
    BrokerName,
    [Level],
    LevelName,
    SplitSequence,
    SplitPercent,
    RatePercent,
    RateSource,
    CommissionAmount,
    HierarchyId,
    HierarchyVersionId,
    HierarchyParticipantId,
    ScheduleId,
    ScheduleVersionId,
    ScheduleCode,
    IsAssigned,
    AssignedFromBrokerId,
    AssignmentVersionId,
    EntryType,
    GETUTCDATE() AS CreationTime
FROM [etl].[calc_broker_traceabilities];

PRINT 'Broker Traceabilities exported: ' + CAST(@@ROWCOUNT AS VARCHAR);
*/

PRINT 'Broker Traceabilities export: SKIPPED (uncomment when ready)';

GO

-- =============================================================================
-- Summary
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'EXPORT SUMMARY';
PRINT '============================================================';

PRINT 'ETL Data available for export:';
SELECT 'calc_gl_journal_entries' AS [Table], COUNT(*) AS [Rows] FROM [etl].[calc_gl_journal_entries]
UNION ALL SELECT 'calc_traceability', COUNT(*) FROM [etl].[calc_traceability]
UNION ALL SELECT 'calc_broker_traceabilities', COUNT(*) FROM [etl].[calc_broker_traceabilities];

PRINT '';
PRINT 'To enable export, uncomment the INSERT statements above';
PRINT 'and adjust column mappings to match production schema.';
PRINT '';
PRINT 'End Time: ' + CONVERT(VARCHAR, GETUTCDATE(), 121);
PRINT '============================================================';
PRINT 'EXPORT COMPLETED';
PRINT '============================================================';

GO

