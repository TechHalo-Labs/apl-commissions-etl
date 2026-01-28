-- =============================================================================
-- Diagnostic: Raw Certificate Data Structure
-- =============================================================================
SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'DIAGNOSING RAW CERTIFICATE DATA';
PRINT '============================================================';
PRINT '';

-- Step 1: Check if table exists
IF OBJECT_ID('poc_etl.raw_certificate_info', 'U') IS NOT NULL
BEGIN
    PRINT '✓ poc_etl.raw_certificate_info exists';
    PRINT '';
END
ELSE
BEGIN
    PRINT '❌ poc_etl.raw_certificate_info does NOT exist';
    PRINT '';
    PRINT 'Available tables in poc_etl schema:';
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'poc_etl'
    ORDER BY TABLE_NAME;
    
    GOTO EndScript;
END

-- Step 2: Total row count
DECLARE @totalCount INT = (SELECT COUNT(*) FROM [poc_etl].[raw_certificate_info]);
PRINT 'Total rows: ' + CAST(@totalCount AS VARCHAR);
PRINT '';

-- Step 3: Sample data (first 3 rows)
PRINT 'Sample rows (first 3):';
SELECT TOP 3 *
FROM [poc_etl].[raw_certificate_info];
PRINT '';

-- Step 4: Check key columns
PRINT 'Checking key columns:';
PRINT '';

-- GroupId
SELECT 
    'GroupId' AS ColumnName,
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN GroupId IS NULL THEN 1 ELSE 0 END) AS NullCount,
    SUM(CASE WHEN LTRIM(RTRIM(GroupId)) = '' THEN 1 ELSE 0 END) AS EmptyCount,
    SUM(CASE WHEN GroupId IS NOT NULL AND LTRIM(RTRIM(GroupId)) <> '' THEN 1 ELSE 0 END) AS ValidCount
FROM [poc_etl].[raw_certificate_info];

-- CertStatus
SELECT 
    'CertStatus' AS ColumnName,
    CertStatus,
    COUNT(*) AS CountRows
FROM [poc_etl].[raw_certificate_info]
GROUP BY CertStatus
ORDER BY COUNT(*) DESC;

-- RecStatus
SELECT 
    'RecStatus' AS ColumnName,
    RecStatus,
    COUNT(*) AS CountRows
FROM [poc_etl].[raw_certificate_info]
GROUP BY RecStatus
ORDER BY COUNT(*) DESC;

-- Product
SELECT 
    'Product' AS ColumnName,
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN Product IS NULL THEN 1 ELSE 0 END) AS NullCount,
    SUM(CASE WHEN LTRIM(RTRIM(Product)) = '' THEN 1 ELSE 0 END) AS EmptyCount,
    SUM(CASE WHEN Product IS NOT NULL AND LTRIM(RTRIM(Product)) <> '' THEN 1 ELSE 0 END) AS ValidCount
FROM [poc_etl].[raw_certificate_info];

PRINT '';
PRINT 'Top 10 distinct products:';
SELECT TOP 10 Product, COUNT(*) AS CountRows
FROM [poc_etl].[raw_certificate_info]
WHERE Product IS NOT NULL AND LTRIM(RTRIM(Product)) <> ''
GROUP BY Product
ORDER BY COUNT(*) DESC;

PRINT '';
PRINT 'Step 5: Test filter criteria';
PRINT '';

-- Test each filter individually
DECLARE @afterGroupIdFilter INT = (
    SELECT COUNT(*)
    FROM [poc_etl].[raw_certificate_info]
    WHERE GroupId IS NOT NULL AND LTRIM(RTRIM(GroupId)) <> ''
);
PRINT 'After GroupId filter: ' + CAST(@afterGroupIdFilter AS VARCHAR);

DECLARE @afterCertStatusFilter INT = (
    SELECT COUNT(*)
    FROM [poc_etl].[raw_certificate_info]
    WHERE GroupId IS NOT NULL 
      AND LTRIM(RTRIM(GroupId)) <> ''
      AND CertStatus IN ('A', 'Active')
);
PRINT 'After CertStatus filter: ' + CAST(@afterCertStatusFilter AS VARCHAR);

DECLARE @afterRecStatusFilter INT = (
    SELECT COUNT(*)
    FROM [poc_etl].[raw_certificate_info]
    WHERE GroupId IS NOT NULL 
      AND LTRIM(RTRIM(GroupId)) <> ''
      AND CertStatus IN ('A', 'Active')
      AND RecStatus = 'A'
);
PRINT 'After RecStatus filter: ' + CAST(@afterRecStatusFilter AS VARCHAR);

DECLARE @afterProductFilter INT = (
    SELECT COUNT(*)
    FROM [poc_etl].[raw_certificate_info]
    WHERE GroupId IS NOT NULL 
      AND LTRIM(RTRIM(GroupId)) <> ''
      AND CertStatus IN ('A', 'Active')
      AND RecStatus = 'A'
      AND Product IS NOT NULL
      AND LTRIM(RTRIM(Product)) <> ''
);
PRINT 'After Product filter: ' + CAST(@afterProductFilter AS VARCHAR);

PRINT '';
PRINT 'Step 6: Column names';
PRINT '';

SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'poc_etl' 
  AND TABLE_NAME = 'raw_certificate_info'
ORDER BY ORDINAL_POSITION;

EndScript:
PRINT '';
PRINT '============================================================';
PRINT 'DIAGNOSIS COMPLETE';
PRINT '============================================================';

GO
