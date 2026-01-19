-- =============================================================================
-- BULK INSERT Script for SQL Server ETL Pipeline
-- =============================================================================
-- Loads CSV data into raw tables using BULK INSERT
-- NOTE: BULK INSERT requires files to be accessible from SQL Server
-- For remote SQL Server (Azure), use TypeScript with mssql bulk operations instead
-- =============================================================================
-- This script is a TEMPLATE - Update paths before running
-- Usage: sqlcmd -S server -d database -i bulk-insert.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'BULK INSERT - Loading CSV Data into Raw Tables';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- IMPORTANT: Update these paths to match your data location
-- For Azure SQL, use Azure Blob Storage paths or client-side bulk insert
-- =============================================================================
DECLARE @DataPath NVARCHAR(500) = 'C:\data\etl\';  -- UPDATE THIS PATH

-- =============================================================================
-- Truncate raw tables before loading
-- =============================================================================
PRINT 'Truncating raw tables...';
TRUNCATE TABLE [etl].[raw_premiums];
TRUNCATE TABLE [etl].[raw_commissions_detail];
TRUNCATE TABLE [etl].[raw_certificate_info];
TRUNCATE TABLE [etl].[raw_individual_brokers];
TRUNCATE TABLE [etl].[raw_org_brokers];
TRUNCATE TABLE [etl].[raw_licenses];
TRUNCATE TABLE [etl].[raw_eo_insurance];
TRUNCATE TABLE [etl].[raw_schedule_rates];
TRUNCATE TABLE [etl].[raw_fees];
TRUNCATE TABLE [etl].[raw_perf_groups];
PRINT 'Raw tables truncated.';
PRINT '';

GO

-- =============================================================================
-- NOTE: The BULK INSERT statements below are templates
-- For Azure SQL Database, you'll need to use:
--   1. Azure Blob Storage with BULK INSERT FROM
--   2. Client-side bulk insert using mssql package
-- =============================================================================

/*
-- Example BULK INSERT for on-premises SQL Server:

PRINT 'Loading premiums.csv...';
BULK INSERT [etl].[raw_premiums]
FROM 'C:\data\etl\premiums.csv'
WITH (
    FIRSTROW = 2,           -- Skip header row
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    ERRORFILE = 'C:\data\etl\errors\premiums_errors.log',
    MAXERRORS = 100
);
PRINT 'Loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

PRINT 'Loading CommissionsDetail.csv...';
BULK INSERT [etl].[raw_commissions_detail]
FROM 'C:\data\etl\CommissionsDetail.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'Loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

PRINT 'Loading CertificateInfo.csv...';
BULK INSERT [etl].[raw_certificate_info]
FROM 'C:\data\etl\CertificateInfo.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'Loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

PRINT 'Loading individual-roster.csv...';
BULK INSERT [etl].[raw_individual_brokers]
FROM 'C:\data\etl\individual-roster.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'Loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

PRINT 'Loading org.csv...';
BULK INSERT [etl].[raw_org_brokers]
FROM 'C:\data\etl\org.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'Loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

PRINT 'Loading license.csv...';
BULK INSERT [etl].[raw_licenses]
FROM 'C:\data\etl\license.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'Loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

PRINT 'Loading eo.csv...';
BULK INSERT [etl].[raw_eo_insurance]
FROM 'C:\data\etl\eo.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'Loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

PRINT 'Loading perf.csv...';
BULK INSERT [etl].[raw_schedule_rates]
FROM 'C:\data\etl\perf.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'Loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

PRINT 'Loading fees.csv...';
BULK INSERT [etl].[raw_fees]
FROM 'C:\data\etl\fees.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'Loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

PRINT 'Loading perf-group.csv...';
BULK INSERT [etl].[raw_perf_groups]
FROM 'C:\data\etl\perf-group.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'Loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

*/

-- =============================================================================
-- Verification - Show row counts after loading
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'RAW TABLE ROW COUNTS';
PRINT '============================================================';

SELECT 'raw_premiums' AS TableName, COUNT(*) AS RowCount FROM [etl].[raw_premiums]
UNION ALL SELECT 'raw_commissions_detail', COUNT(*) FROM [etl].[raw_commissions_detail]
UNION ALL SELECT 'raw_certificate_info', COUNT(*) FROM [etl].[raw_certificate_info]
UNION ALL SELECT 'raw_individual_brokers', COUNT(*) FROM [etl].[raw_individual_brokers]
UNION ALL SELECT 'raw_org_brokers', COUNT(*) FROM [etl].[raw_org_brokers]
UNION ALL SELECT 'raw_licenses', COUNT(*) FROM [etl].[raw_licenses]
UNION ALL SELECT 'raw_eo_insurance', COUNT(*) FROM [etl].[raw_eo_insurance]
UNION ALL SELECT 'raw_schedule_rates', COUNT(*) FROM [etl].[raw_schedule_rates]
UNION ALL SELECT 'raw_fees', COUNT(*) FROM [etl].[raw_fees]
UNION ALL SELECT 'raw_perf_groups', COUNT(*) FROM [etl].[raw_perf_groups]
ORDER BY TableName;

GO

