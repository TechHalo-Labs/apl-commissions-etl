-- =============================================================================
-- Restore Raw Data Schema
-- =============================================================================
-- Restores raw data tables from a backup schema
-- Much faster than re-ingesting CSV files
-- 
-- Usage:
--   sqlcmd -S server -d database -v BACKUP_SCHEMA=etl_backup_20260127_143022 -i sql/restore-raw-data.sql
-- 
-- Or from most recent backup:
--   sqlcmd -S server -d database -i sql/restore-raw-data.sql
-- =============================================================================

SET NOCOUNT ON;

DECLARE @BackupSchema NVARCHAR(50);
DECLARE @TargetSchema NVARCHAR(50) = 'etl';
DECLARE @SQL NVARCHAR(MAX);
DECLARE @TableName NVARCHAR(200);
DECLARE @RowCount BIGINT;
DECLARE @TotalRows BIGINT = 0;
DECLARE @StartTime DATETIME2 = GETUTCDATE();

-- Get backup schema from parameter or find most recent
SET @BackupSchema = '$(BACKUP_SCHEMA)';

-- If not provided, find most recent backup
IF @BackupSchema = '$(BACKUP_SCHEMA)' OR @BackupSchema IS NULL OR @BackupSchema = ''
BEGIN
  SELECT TOP 1 @BackupSchema = name
  FROM sys.schemas
  WHERE name LIKE 'etl_backup_%'
  ORDER BY name DESC;
  
  IF @BackupSchema IS NULL
  BEGIN
    PRINT '❌ No backup schema found!';
    PRINT '';
    PRINT 'Available backup schemas:';
    SELECT name FROM sys.schemas WHERE name LIKE 'etl_backup_%' ORDER BY name DESC;
    RETURN;
  END
  
  PRINT 'Using most recent backup: ' + @BackupSchema;
END

PRINT '============================================================';
PRINT 'RAW DATA RESTORE';
PRINT '============================================================';
PRINT 'Backup Schema: ' + @BackupSchema;
PRINT 'Target Schema: ' + @TargetSchema;
PRINT 'Started: ' + CONVERT(VARCHAR, @StartTime, 120);
PRINT '';

-- Verify backup schema exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = @BackupSchema)
BEGIN
  PRINT '❌ Backup schema does not exist: ' + @BackupSchema;
  PRINT '';
  PRINT 'Available backup schemas:';
  SELECT name FROM sys.schemas WHERE name LIKE 'etl_backup_%' ORDER BY name DESC;
  RETURN;
END

-- List of raw tables to restore
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

-- Restore each table
DECLARE table_cursor CURSOR FOR 
  SELECT TableName FROM @Tables;

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
  -- Check if backup table exists
  IF OBJECT_ID('[' + @BackupSchema + '].[' + @TableName + ']', 'U') IS NOT NULL
  BEGIN
    PRINT 'Restoring table: ' + @TableName;
    
    -- Truncate target table
    IF OBJECT_ID('[' + @TargetSchema + '].[' + @TableName + ']', 'U') IS NOT NULL
    BEGIN
      SET @SQL = 'TRUNCATE TABLE [' + @TargetSchema + '].[' + @TableName + ']';
      EXEC sp_executesql @SQL;
    END
    
    -- Copy data from backup
    SET @SQL = 'INSERT INTO [' + @TargetSchema + '].[' + @TableName + '] SELECT * FROM [' + @BackupSchema + '].[' + @TableName + ']';
    EXEC sp_executesql @SQL;
    
    SET @RowCount = @@ROWCOUNT;
    SET @TotalRows = @TotalRows + @RowCount;
    
    PRINT '  ✓ Restored ' + CAST(@RowCount AS VARCHAR) + ' rows';
  END
  ELSE
  BEGIN
    PRINT '  ⚠ Backup table does not exist: ' + @TableName;
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
PRINT 'RESTORE COMPLETE';
PRINT '============================================================';
PRINT 'Backup Schema: ' + @BackupSchema;
PRINT 'Total Rows Restored: ' + CAST(@TotalRows AS VARCHAR);
PRINT 'Duration: ' + CAST(@DurationSeconds AS VARCHAR) + ' seconds';
PRINT 'Completed: ' + CONVERT(VARCHAR, @EndTime, 120);
PRINT '';
