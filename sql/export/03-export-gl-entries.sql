-- =====================================================
-- Export GL Journal Entries from etl calc to dbo
-- =====================================================

-- Create CommissionRun if not exists
IF NOT EXISTS (SELECT 1 FROM [dbo].[CommissionRuns] WHERE Id = 'SQL-ETL-V5')
BEGIN
    PRINT 'Creating CommissionRun...';
    
    DECLARE @TotalEntries INT, @TotalAmount DECIMAL(18,2), @TotalPremiums INT, @TotalPremiumAmount DECIMAL(18,2);
    
    SELECT 
        @TotalEntries = COUNT(*),
        @TotalAmount = COALESCE(SUM(CAST(CommissionAmount AS FLOAT)), 0),
        @TotalPremiums = COUNT(DISTINCT PremiumTransactionId),
        @TotalPremiumAmount = COALESCE(SUM(CAST(PremiumAmount AS FLOAT)), 0)
    FROM [etl].[calc_gl_journal_entries];
    
    INSERT INTO [dbo].[CommissionRuns] (
        Id, Name, PeriodStart, PeriodEnd, [Status], StartedAt, CompletedAt,
        TotalEntries, TotalAmount, ErrorCount, Notes, TotalPremiums,
        ProcessedCount, FailedCount, SkippedCount, TotalPremiumAmount,
        IsDryRun, IsAuthoritative, SourceType, Description, Phase,
        CallbackRetryCount, IsPromoted, TotalFeeAgreements, TotalFeeAmount,
        BrokerTraceabilitiesCount, CreationTime, IsDeleted
    )
    VALUES (
        'SQL-ETL-V5',
        'SQL Server ETL v5 - Full Commission Run',
        '2020-01-01',
        '2025-12-31',
        3, -- Completed
        DATEADD(HOUR, -1, GETUTCDATE()),
        GETUTCDATE(),
        @TotalEntries,
        @TotalAmount,
        0,
        'Full commission calculation via SQL Server ETL pipeline',
        @TotalPremiums,
        @TotalPremiums,
        0,
        0,
        @TotalPremiumAmount,
        0, -- IsDryRun
        1, -- IsAuthoritative
        'SqlETL',
        'Commission calculation using cascading SQL transforms in SQL Server',
        4, -- Phase (Complete)
        0,
        0,
        0,
        0,
        0,
        GETUTCDATE(),
        0
    );
    
    PRINT 'CommissionRun created';
END
ELSE
BEGIN
    PRINT 'CommissionRun SQL-ETL-V5 already exists';
END
GO

-- Clear existing GL entries for this run
PRINT 'Clearing existing GL entries for SQL-ETL-V5...';
DELETE FROM [dbo].[GLJournalEntries] WHERE CommissionRunId = 'SQL-ETL-V5';
PRINT 'Cleared existing GL entries';
GO

-- Export GL Journal Entries
PRINT 'Exporting GL Journal Entries...';

INSERT INTO [dbo].[GLJournalEntries] (
    SourceEejeId, JournalNumber, EntryDate, PostingDate, FiscalPeriod, Description,
    TotalDebits, TotalCredits, IsBalanced, StatusString, BrokerId, GroupId,
    PremiumPaymentId, PolicyId, HierarchyId, HierarchyTier, CommissionRate,
    PremiumAmount, [State], MacApplied, WasCompressed, SplitSequence, SplitPercentage,
    HierarchyLevel, CommissionRunId, EntryType, CreationTime, IsDeleted, CreatedDate
)
SELECT 
    0 AS SourceEejeId,
    CONCAT('GL-SQL-', CAST(ROW_NUMBER() OVER (ORDER BY gl.Id) AS VARCHAR)) AS JournalNumber,
    gl.TransactionDate AS EntryDate,
    gl.TransactionDate AS PostingDate,
    CONCAT(YEAR(gl.TransactionDate), '-', RIGHT('0' + CAST(MONTH(gl.TransactionDate) AS VARCHAR), 2)) AS FiscalPeriod,
    CONCAT('Commission: ', gl.BrokerName, ' - ', gl.ProductCode) AS Description,
    gl.CommissionAmount AS TotalDebits,
    0 AS TotalCredits,
    1 AS IsBalanced,
    'Posted' AS StatusString,
    gl.BrokerId,
    gl.GroupId,
    gl.PremiumTransactionId AS PremiumPaymentId,
    CAST(gl.PolicyId AS NVARCHAR(100)) AS PolicyId,
    gl.HierarchyId,
    CONCAT('L', gl.TierLevel) AS HierarchyTier,
    gl.RatePercent AS CommissionRate,
    gl.PremiumAmount,
    gl.[State],
    0 AS MacApplied,
    0 AS WasCompressed,
    gl.SplitSequence,
    gl.SplitPercent AS SplitPercentage,
    gl.TierLevel AS HierarchyLevel,
    'SQL-ETL-V5' AS CommissionRunId,
    gl.EntryType,
    gl.CreationTime,
    0 AS IsDeleted,
    gl.CreationTime AS CreatedDate
FROM [etl].[calc_gl_journal_entries] gl
WHERE gl.BrokerId IN (SELECT Id FROM [dbo].[Brokers]);

DECLARE @glCount INT;
SELECT @glCount = COUNT(*) FROM [dbo].[GLJournalEntries] WHERE CommissionRunId = 'SQL-ETL-V5';
PRINT 'GL entries exported: ' + CAST(@glCount AS VARCHAR);
GO

-- Update CommissionRun totals
PRINT 'Updating CommissionRun totals...';

UPDATE cr
SET 
    TotalEntries = gl.cnt,
    TotalAmount = gl.total
FROM [dbo].[CommissionRuns] cr
CROSS JOIN (
    SELECT COUNT(*) as cnt, COALESCE(SUM(CAST(TotalDebits AS FLOAT)), 0) as total
    FROM [dbo].[GLJournalEntries] 
    WHERE CommissionRunId = 'SQL-ETL-V5'
) gl
WHERE cr.Id = 'SQL-ETL-V5';

PRINT 'CommissionRun totals updated';
GO

PRINT '=== GL Entry Export Complete ===';

