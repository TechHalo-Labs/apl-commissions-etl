-- =============================================================================
-- Create ETL Schema
-- =============================================================================
-- Creates the ETL processing schema if it doesn't exist
-- This must run BEFORE any table creation scripts
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'CREATING ETL SCHEMA';
PRINT '============================================================';
PRINT '';

-- Create schema if it doesn't exist
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '$(ETL_SCHEMA)')
BEGIN
    EXEC('CREATE SCHEMA [$(ETL_SCHEMA)]');
    PRINT '✅ Schema [$(ETL_SCHEMA)] created';
END
ELSE
BEGIN
    PRINT '⏭️  Schema [$(ETL_SCHEMA)] already exists';
END

PRINT '';
PRINT '============================================================';
PRINT 'SCHEMA SETUP COMPLETE';
PRINT '============================================================';
