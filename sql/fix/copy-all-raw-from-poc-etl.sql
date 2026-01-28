-- =============================================================================
-- Copy ALL Raw Data from poc_etl to etl schema
-- Fixes root cause: Transform scripts expect data in etl schema
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'COPY ALL RAW DATA: poc_etl → etl';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- 1. raw_certificate_info
-- =============================================================================
DECLARE @cert_count BIGINT = (SELECT COUNT(*) FROM [poc_etl].[raw_certificate_info]);
PRINT '1. raw_certificate_info: ' + FORMAT(@cert_count, 'N0') + ' rows';

IF @cert_count > 0
BEGIN
    TRUNCATE TABLE [etl].[raw_certificate_info];
    INSERT INTO [etl].[raw_certificate_info]
    SELECT * FROM [poc_etl].[raw_certificate_info];
    PRINT '   ✅ Copied: ' + FORMAT(@@ROWCOUNT, 'N0') + ' rows';
END
ELSE
    PRINT '   ⏭️ Source empty, skipping';

-- =============================================================================
-- 2. raw_schedule_rates
-- =============================================================================
PRINT '';
DECLARE @sched_count BIGINT = (SELECT COUNT(*) FROM [poc_etl].[raw_schedule_rates]);
PRINT '2. raw_schedule_rates: ' + FORMAT(@sched_count, 'N0') + ' rows';

IF @sched_count > 0
BEGIN
    TRUNCATE TABLE [etl].[raw_schedule_rates];
    INSERT INTO [etl].[raw_schedule_rates]
    SELECT * FROM [poc_etl].[raw_schedule_rates];
    PRINT '   ✅ Copied: ' + FORMAT(@@ROWCOUNT, 'N0') + ' rows';
END
ELSE
    PRINT '   ⏭️ Source empty, skipping';

-- =============================================================================
-- 3. raw_perf_groups
-- =============================================================================
PRINT '';
DECLARE @perf_count BIGINT = (SELECT COUNT(*) FROM [poc_etl].[raw_perf_groups]);
PRINT '3. raw_perf_groups: ' + FORMAT(@perf_count, 'N0') + ' rows';

IF @perf_count > 0
BEGIN
    TRUNCATE TABLE [etl].[raw_perf_groups];
    INSERT INTO [etl].[raw_perf_groups]
    SELECT * FROM [poc_etl].[raw_perf_groups];
    PRINT '   ✅ Copied: ' + FORMAT(@@ROWCOUNT, 'N0') + ' rows';
END
ELSE
    PRINT '   ⏭️ Source empty, skipping';

-- =============================================================================
-- 4. raw_premiums
-- =============================================================================
PRINT '';
DECLARE @prem_count BIGINT = (SELECT COUNT(*) FROM [poc_etl].[raw_premiums]);
PRINT '4. raw_premiums: ' + FORMAT(@prem_count, 'N0') + ' rows';

IF @prem_count > 0
BEGIN
    TRUNCATE TABLE [etl].[raw_premiums];
    INSERT INTO [etl].[raw_premiums]
    SELECT * FROM [poc_etl].[raw_premiums];
    PRINT '   ✅ Copied: ' + FORMAT(@@ROWCOUNT, 'N0') + ' rows';
END
ELSE
    PRINT '   ⏭️ Source empty, skipping';

-- =============================================================================
-- 5. raw_individual_brokers
-- =============================================================================
PRINT '';
DECLARE @ind_broker_count BIGINT = (SELECT COUNT(*) FROM [poc_etl].[raw_individual_brokers]);
PRINT '5. raw_individual_brokers: ' + FORMAT(@ind_broker_count, 'N0') + ' rows';

IF @ind_broker_count > 0
BEGIN
    TRUNCATE TABLE [etl].[raw_individual_brokers];
    INSERT INTO [etl].[raw_individual_brokers]
    SELECT * FROM [poc_etl].[raw_individual_brokers];
    PRINT '   ✅ Copied: ' + FORMAT(@@ROWCOUNT, 'N0') + ' rows';
END
ELSE
    PRINT '   ⏭️ Source empty, skipping';

-- =============================================================================
-- 6. raw_org_brokers
-- =============================================================================
PRINT '';
DECLARE @org_broker_count BIGINT = (SELECT COUNT(*) FROM [poc_etl].[raw_org_brokers]);
PRINT '6. raw_org_brokers: ' + FORMAT(@org_broker_count, 'N0') + ' rows';

IF @org_broker_count > 0
BEGIN
    TRUNCATE TABLE [etl].[raw_org_brokers];
    INSERT INTO [etl].[raw_org_brokers]
    SELECT * FROM [poc_etl].[raw_org_brokers];
    PRINT '   ✅ Copied: ' + FORMAT(@@ROWCOUNT, 'N0') + ' rows';
END
ELSE
    PRINT '   ⏭️ Source empty, skipping';

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'raw_certificate_info' AS [table], COUNT(*) AS etl_count FROM [etl].[raw_certificate_info];
SELECT 'raw_schedule_rates' AS [table], COUNT(*) AS etl_count FROM [etl].[raw_schedule_rates];
SELECT 'raw_perf_groups' AS [table], COUNT(*) AS etl_count FROM [etl].[raw_perf_groups];
SELECT 'raw_premiums' AS [table], COUNT(*) AS etl_count FROM [etl].[raw_premiums];
SELECT 'raw_individual_brokers' AS [table], COUNT(*) AS etl_count FROM [etl].[raw_individual_brokers];
SELECT 'raw_org_brokers' AS [table], COUNT(*) AS etl_count FROM [etl].[raw_org_brokers];

PRINT '';
PRINT '============================================================';
PRINT 'RAW DATA COPY COMPLETED';
PRINT '============================================================';
PRINT '';
PRINT 'Now ready to run transforms with data present!';
PRINT '';

GO
