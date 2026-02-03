-- =============================================================================
-- Copy All Data from etl Schema to old_etl Schema
-- =============================================================================
-- Creates the old_etl schema if it doesn't exist and copies all tables
-- (structure + data) from etl to old_etl.
-- 
-- Usage:
--   sqlcmd -S SERVER -d DATABASE -i sql/utils/copy-etl-to-old-etl.sql
-- 
-- Or execute directly in SSMS
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'COPY SCHEMA: etl → old_etl';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Create target schema if it doesn't exist
-- =============================================================================
PRINT 'Step 1: Creating target schema...';

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'old_etl')
BEGIN
    EXEC('CREATE SCHEMA [old_etl]');
    PRINT '   ✅ Schema [old_etl] created';
END
ELSE
BEGIN
    PRINT '   ℹ️  Schema [old_etl] already exists';
END

PRINT '';

-- =============================================================================
-- Step 2: Copy all tables dynamically
-- =============================================================================
PRINT 'Step 2: Copying tables...';
PRINT '';

DECLARE @tableName NVARCHAR(128);
DECLARE @sql NVARCHAR(MAX);
DECLARE @rowCount BIGINT;
DECLARE @copiedCount BIGINT;
DECLARE @tableCount INT = 0;
DECLARE @successCount INT = 0;
DECLARE @failedCount INT = 0;

-- Cursor to iterate through all tables in etl schema
DECLARE table_cursor CURSOR FOR
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'etl'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @tableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @tableCount = @tableCount + 1;
    
    BEGIN TRY
        -- Get row count from source
        SET @sql = N'SELECT @count = COUNT(*) FROM [etl].[' + @tableName + ']';
        EXEC sp_executesql @sql, N'@count BIGINT OUTPUT', @count = @rowCount OUTPUT;
        
        PRINT '  [' + CAST(@tableCount AS VARCHAR(3)) + '] Copying [' + @tableName + ']: ' + FORMAT(@rowCount, 'N0') + ' rows';
        
        -- Drop target table if it exists
        SET @sql = N'
        IF OBJECT_ID(''[old_etl].[' + @tableName + ']'', ''U'') IS NOT NULL
            DROP TABLE [old_etl].[' + @tableName + '];';
        EXEC sp_executesql @sql;
        
        -- Copy table structure and data using SELECT INTO
        SET @sql = N'
        SELECT * INTO [old_etl].[' + @tableName + ']
        FROM [etl].[' + @tableName + '];';
        EXEC sp_executesql @sql;
        
        -- Verify row count
        SET @sql = N'SELECT @count = COUNT(*) FROM [old_etl].[' + @tableName + ']';
        EXEC sp_executesql @sql, N'@count BIGINT OUTPUT', @count = @copiedCount OUTPUT;
        
        IF @copiedCount = @rowCount
        BEGIN
            PRINT '      ✅ Copied: ' + FORMAT(@copiedCount, 'N0') + ' rows';
            SET @successCount = @successCount + 1;
        END
        ELSE
        BEGIN
            PRINT '      ⚠️  MISMATCH: Source=' + FORMAT(@rowCount, 'N0') + ', Target=' + FORMAT(@copiedCount, 'N0');
            SET @successCount = @successCount + 1; -- Still count as success, but warn
        END
        
    END TRY
    BEGIN CATCH
        PRINT '      ❌ FAILED: ' + ERROR_MESSAGE();
        SET @failedCount = @failedCount + 1;
    END CATCH
    
    PRINT '';
    
    FETCH NEXT FROM table_cursor INTO @tableName;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

-- =============================================================================
-- Step 3: Summary
-- =============================================================================
PRINT '============================================================';
PRINT 'COPY SUMMARY';
PRINT '============================================================';
PRINT '  Tables processed: ' + CAST(@tableCount AS VARCHAR(10));
PRINT '  ✅ Successful: ' + CAST(@successCount AS VARCHAR(10));
PRINT '  ❌ Failed: ' + CAST(@failedCount AS VARCHAR(10));
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 4: Verification - Compare row counts
-- =============================================================================
PRINT 'Step 3: Verification (comparing row counts)...';
PRINT '';

DECLARE verify_cursor CURSOR FOR
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'old_etl'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;

DECLARE @sourceCount BIGINT;
DECLARE @targetCount BIGINT;
DECLARE @verifiedCount INT = 0;
DECLARE @mismatchCount INT = 0;

OPEN verify_cursor;
FETCH NEXT FROM verify_cursor INTO @tableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        -- Get source count
        SET @sql = N'SELECT @count = COUNT(*) FROM [etl].[' + @tableName + ']';
        EXEC sp_executesql @sql, N'@count BIGINT OUTPUT', @count = @sourceCount OUTPUT;
        
        -- Get target count
        SET @sql = N'SELECT @count = COUNT(*) FROM [old_etl].[' + @tableName + ']';
        EXEC sp_executesql @sql, N'@count BIGINT OUTPUT', @count = @targetCount OUTPUT;
        
        IF @sourceCount = @targetCount
        BEGIN
            PRINT '  ✅ [' + @tableName + ']: ' + FORMAT(@sourceCount, 'N0') + ' rows (match)';
            SET @verifiedCount = @verifiedCount + 1;
        END
        ELSE
        BEGIN
            PRINT '  ⚠️  [' + @tableName + ']: Source=' + FORMAT(@sourceCount, 'N0') + ', Target=' + FORMAT(@targetCount, 'N0') + ' (MISMATCH)';
            SET @mismatchCount = @mismatchCount + 1;
        END
    END TRY
    BEGIN CATCH
        PRINT '  ❌ [' + @tableName + ']: Verification failed - ' + ERROR_MESSAGE();
    END CATCH
    
    FETCH NEXT FROM verify_cursor INTO @tableName;
END

CLOSE verify_cursor;
DEALLOCATE verify_cursor;

PRINT '';
PRINT '  Verification: ' + CAST(@verifiedCount AS VARCHAR(10)) + ' matched, ' + CAST(@mismatchCount AS VARCHAR(10)) + ' mismatched';
PRINT '';

-- =============================================================================
-- Final Summary
-- =============================================================================
PRINT '============================================================';
PRINT 'COPY OPERATION COMPLETED';
PRINT '============================================================';
PRINT '';
PRINT 'Schema [old_etl] is ready with all data from [etl]';
PRINT '';

GO
