-- =============================================================================
-- ETL Schema Setup for SQL Server
-- =============================================================================
-- Creates and resets the [etl] schema for commission calculation pipeline
-- Run this first to ensure a clean slate before ETL operations
-- Usage: sqlcmd -S server -d database -i 00-schema-setup.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'ETL SCHEMA SETUP';
PRINT '============================================================';
PRINT 'Start Time: ' + CONVERT(VARCHAR, GETUTCDATE(), 121);
PRINT '';

-- =============================================================================
-- Step 1: Drop all existing tables in etl schema (if exists)
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
BEGIN
    PRINT 'Dropping existing tables in [etl] schema...';
    
    DECLARE @sql NVARCHAR(MAX) = N'';
    
    -- Build DROP statements for all tables in etl schema
    SELECT @sql = @sql + N'DROP TABLE IF EXISTS [etl].[' + t.name + N']; '
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'etl';
    
    -- Execute the drops
    IF LEN(@sql) > 0
    BEGIN
        EXEC sp_executesql @sql;
        PRINT 'All tables dropped.';
    END
    ELSE
    BEGIN
        PRINT 'No tables found in [etl] schema.';
    END
    
    -- Drop the schema itself
    PRINT 'Dropping [etl] schema...';
    DROP SCHEMA [etl];
    PRINT '[etl] schema dropped.';
END
ELSE
BEGIN
    PRINT '[etl] schema does not exist, nothing to drop.';
END

GO

-- =============================================================================
-- Step 2: Create fresh etl schema
-- =============================================================================
CREATE SCHEMA [etl];
GO

PRINT '';
PRINT '[etl] schema created successfully.';

GO

-- =============================================================================
-- Step 3: Verify schema creation
-- =============================================================================
PRINT '';
PRINT 'Verifying schema...';

IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
BEGIN
    PRINT '[etl] schema verified - ready for ETL operations.';
END
ELSE
BEGIN
    RAISERROR('[etl] schema creation failed!', 16, 1);
END

GO

PRINT '';
PRINT '============================================================';
PRINT 'ETL SCHEMA SETUP COMPLETE';
PRINT '============================================================';
PRINT 'End Time: ' + CONVERT(VARCHAR, GETUTCDATE(), 121);

GO

