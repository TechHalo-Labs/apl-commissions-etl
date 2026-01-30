-- =============================================================================
-- Diagnostic: Check certificate table existence and counts
-- =============================================================================
SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'CERTIFICATE TABLE DIAGNOSTICS';
PRINT '============================================================';
PRINT '';

-- Check if cert_split_configs exists in poc_etl
IF OBJECT_ID('poc_etl.cert_split_configs', 'U') IS NOT NULL
BEGIN
    DECLARE @poc_count INT = (SELECT COUNT(*) FROM [poc_etl].[cert_split_configs]);
    PRINT '✓ poc_etl.cert_split_configs EXISTS';
    PRINT '  Row count: ' + CAST(@poc_count AS VARCHAR);
    PRINT '';
    
    -- Sample data
    PRINT 'Sample rows (first 3):';
    SELECT TOP 3 
        GroupId, 
        EffectiveDate, 
        ProductCode, 
        PlanCode, 
        CertificateId
    FROM [poc_etl].[cert_split_configs];
    PRINT '';
END
ELSE
BEGIN
    PRINT '✗ poc_etl.cert_split_configs DOES NOT EXIST';
    PRINT '';
END

-- Check if cert_split_configs exists in etl
IF OBJECT_ID('etl.cert_split_configs', 'U') IS NOT NULL
BEGIN
    DECLARE @etl_count INT = (SELECT COUNT(*) FROM [etl].[cert_split_configs]);
    PRINT '✓ etl.cert_split_configs EXISTS';
    PRINT '  Row count: ' + CAST(@etl_count AS VARCHAR);
    PRINT '';
END
ELSE
BEGIN
    PRINT '✗ etl.cert_split_configs DOES NOT EXIST';
    PRINT '';
END

-- Check remainder tables
PRINT 'Checking remainder tables:';
IF OBJECT_ID('poc_etl.cert_split_configs_remainder2', 'U') IS NOT NULL
BEGIN
    DECLARE @r2_count INT = (SELECT COUNT(*) FROM [poc_etl].[cert_split_configs_remainder2]);
    PRINT '  poc_etl.cert_split_configs_remainder2: ' + CAST(@r2_count AS VARCHAR);
END
ELSE
    PRINT '  poc_etl.cert_split_configs_remainder2: DOES NOT EXIST';

IF OBJECT_ID('poc_etl.cert_split_configs_remainder3', 'U') IS NOT NULL
BEGIN
    DECLARE @r3_count INT = (SELECT COUNT(*) FROM [poc_etl].[cert_split_configs_remainder3]);
    PRINT '  poc_etl.cert_split_configs_remainder3: ' + CAST(@r3_count AS VARCHAR);
END
ELSE
    PRINT '  poc_etl.cert_split_configs_remainder3: DOES NOT EXIST';

PRINT '';

-- Check input_certificate_info (source table)
PRINT 'Checking source table:';
IF OBJECT_ID('poc_etl.input_certificate_info', 'U') IS NOT NULL
BEGIN
    DECLARE @input_count INT = (SELECT COUNT(*) FROM [poc_etl].[input_certificate_info]);
    PRINT '  poc_etl.input_certificate_info: ' + CAST(@input_count AS VARCHAR) + ' rows';
    
    -- Check active certificates
    DECLARE @active_count INT = (
        SELECT COUNT(DISTINCT CertificateId)
        FROM [poc_etl].[input_certificate_info]
        WHERE GroupId IS NOT NULL 
          AND LTRIM(RTRIM(GroupId)) <> ''
          AND CertStatus IN ('A', 'Active')
          AND RecStatus = 'A'
          AND Product IS NOT NULL
    );
    PRINT '  Active certificates (would create cert_split_configs): ' + CAST(@active_count AS VARCHAR);
END
ELSE
    PRINT '  poc_etl.input_certificate_info: DOES NOT EXIST';

PRINT '';
PRINT '============================================================';
PRINT 'DIAGNOSTIC COMPLETE';
PRINT '============================================================';

GO
