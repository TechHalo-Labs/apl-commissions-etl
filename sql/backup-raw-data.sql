-- =============================================================================
-- Backup Raw Data Schema
-- =============================================================================
-- Creates a timestamped backup copy of all raw data tables
-- This allows for quick restore without re-ingesting CSV files
-- 
-- Usage:
--   sqlcmd -S server -d database -i sql/backup-raw-data.sql
-- =============================================================================

SET NOCOUNT ON;

DECLARE @BackupSchema NVARCHAR(50);
DECLARE @SourceSchema NVARCHAR(50) = 'etl';
DECLARE @SQL NVARCHAR(MAX);
DECLARE @TableName NVARCHAR(200);
DECLARE @RowCount BIGINT;
DECLARE @TotalRows BIGINT = 0;
DECLARE @StartTime DATETIME2 = GETUTCDATE();

-- Generate backup schema name with timestamp
SET @BackupSchema = 'etl_backup_' + FORMAT(GETUTCDATE(), 'yyyyMMdd_HHmmss');

PRINT '============================================================';
PRINT 'RAW DATA BACKUP';
PRINT '============================================================';
PRINT 'Source Schema: ' + @SourceSchema;
PRINT 'Backup Schema: ' + @BackupSchema;
PRINT 'Started: ' + CONVERT(VARCHAR, @StartTime, 120);
PRINT '';

-- Create backup schema
PRINT 'Creating backup schema...';
SET @SQL = 'CREATE SCHEMA [' + @BackupSchema + ']';
EXEC sp_executesql @SQL;
PRINT '✓ Schema created: ' + @BackupSchema;
PRINT '';

-- List of raw tables to backup
DECLARE @Tables TABLE (TableName NVARCHAR(200));
INSERT INTO @Tables VALUES
  ('raw_premiums'),
  ('raw_certificate_info'),
  ('raw_commissions_detail'),
  ('raw_individual_brokers'),
  ('raw_org_brokers'),
  ('raw_licenses'),
  ('raw_eo_insurance'),
  ('raw_schedule_rates'),
  ('raw_fees'),
  ('raw_perf_groups');

-- Backup each table
DECLARE table_cursor CURSOR FOR 
  SELECT TableName FROM @Tables;

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
  -- Check if source table exists
  IF OBJECT_ID('[' + @SourceSchema + '].[' + @TableName + ']', 'U') IS NOT NULL
  BEGIN
    PRINT 'Backing up table: ' + @TableName;
    
    -- Copy table structure and data
    SET @SQL = 'SELECT * INTO [' + @BackupSchema + '].[' + @TableName + '] FROM [' + @SourceSchema + '].[' + @TableName + ']';
    EXEC sp_executesql @SQL;
    
    SET @RowCount = @@ROWCOUNT;
    SET @TotalRows = @TotalRows + @RowCount;
    
    PRINT '  ✓ Copied ' + CAST(@RowCount AS VARCHAR) + ' rows';
  END
  ELSE
  BEGIN
    PRINT '  ⚠ Table does not exist: ' + @TableName;
  END
  
  FETCH NEXT FROM table_cursor INTO @TableName;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

-- Summary
DECLARE @EndTime DATETIME2 = GETUTCDATE();
DECLARE @DurationSeconds INT = DATEDIFF(SECOND, @StartTime, @EndTime);

PRINT '';
PRINT '============================================================';
PRINT 'BACKUP COMPLETE';
PRINT '============================================================';
PRINT 'Backup Schema: ' + @BackupSchema;
PRINT 'Total Rows: ' + CAST(@TotalRows AS VARCHAR);
PRINT 'Duration: ' + CAST(@DurationSeconds AS VARCHAR) + ' seconds';
PRINT 'Completed: ' + CONVERT(VARCHAR, @EndTime, 120);
PRINT '';
PRINT 'To restore this backup, run:';
PRINT '  sqlcmd -S server -d database -v BACKUP_SCHEMA=' + @BackupSchema + ' -i sql/restore-raw-data.sql';
PRINT '';
