-- =============================================================================
-- List Available Backups
-- =============================================================================
-- Shows all backup schemas with row counts and creation dates
-- 
-- Usage:
--   sqlcmd -S server -d database -i sql/list-backups.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'AVAILABLE BACKUPS';
PRINT '============================================================';
PRINT '';

-- Find all backup schemas
DECLARE @BackupSchemas TABLE (
  SchemaName NVARCHAR(50),
  CreatedDate DATETIME2,
  TotalRows BIGINT
);

DECLARE @SchemaName NVARCHAR(50);
DECLARE @SQL NVARCHAR(MAX);
DECLARE @TotalRows BIGINT;

DECLARE schema_cursor CURSOR FOR
  SELECT name
  FROM sys.schemas
  WHERE name LIKE 'etl_backup_%'
  ORDER BY name DESC;

OPEN schema_cursor;
FETCH NEXT FROM schema_cursor INTO @SchemaName;

WHILE @@FETCH_STATUS = 0
BEGIN
  -- Count rows in raw_premiums table as indicator
  SET @SQL = 'SELECT @count = COUNT(*) FROM [' + @SchemaName + '].[raw_premiums]';
  
  BEGIN TRY
    EXEC sp_executesql @SQL, N'@count BIGINT OUTPUT', @TotalRows OUTPUT;
  END TRY
  BEGIN CATCH
    SET @TotalRows = 0;
  END CATCH
  
  -- Parse date from schema name (etl_backup_YYYYMMDD_HHMMSS)
  DECLARE @DateStr NVARCHAR(20) = SUBSTRING(@SchemaName, 12, 15);
  DECLARE @CreatedDate DATETIME2;
  
  BEGIN TRY
    SET @CreatedDate = CONVERT(DATETIME2, 
      SUBSTRING(@DateStr, 1, 4) + '-' + 
      SUBSTRING(@DateStr, 5, 2) + '-' + 
      SUBSTRING(@DateStr, 7, 2) + ' ' +
      SUBSTRING(@DateStr, 10, 2) + ':' +
      SUBSTRING(@DateStr, 12, 2) + ':' +
      SUBSTRING(@DateStr, 14, 2)
    );
  END TRY
  BEGIN CATCH
    SET @CreatedDate = NULL;
  END CATCH
  
  INSERT INTO @BackupSchemas (SchemaName, CreatedDate, TotalRows)
  VALUES (@SchemaName, @CreatedDate, @TotalRows);
  
  FETCH NEXT FROM schema_cursor INTO @SchemaName;
END

CLOSE schema_cursor;
DEALLOCATE schema_cursor;

-- Display results
IF EXISTS (SELECT 1 FROM @BackupSchemas)
BEGIN
  SELECT 
    SchemaName,
    CONVERT(VARCHAR, CreatedDate, 120) AS CreatedDate,
    FORMAT(TotalRows, '#,##0') AS PremiumRows
  FROM @BackupSchemas
  ORDER BY CreatedDate DESC;
  
  PRINT '';
  PRINT 'To restore a backup:';
  PRINT '  sqlcmd -S server -d database -v BACKUP_SCHEMA=<schema_name> -i sql/restore-raw-data.sql';
  PRINT '';
END
ELSE
BEGIN
  PRINT 'No backups found.';
  PRINT '';
  PRINT 'To create a backup:';
  PRINT '  sqlcmd -S server -d database -i sql/backup-raw-data.sql';
  PRINT '';
END
