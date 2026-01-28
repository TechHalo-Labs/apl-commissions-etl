-- ═══════════════════════════════════════════════════════════════════════════════
-- CREATE RAW TABLES IN new_data SCHEMA
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Create raw_* tables/views in new_data schema so ETL ingest can read them
-- 
-- Strategy:
-- - raw_certificate_info: Use CertificateInfo table (1.7M rows - newest)
-- - raw_perf_groups: Reference poc_raw_data.raw_perf_groups (32K groups)
-- - raw_schedule_rates: Reference poc_raw_data.raw_schedule_rates
-- - Other raw tables: Reference from poc_raw_data
-- ═══════════════════════════════════════════════════════════════════════════════

SET NOCOUNT ON;

PRINT '═══════════════════════════════════════════════════════════';
PRINT 'CREATE RAW TABLES IN new_data SCHEMA';
PRINT '═══════════════════════════════════════════════════════════';
PRINT '';

-- ═══════════════════════════════════════════════════════════════════════════════
-- 1. Create raw_certificate_info as synonym/view to CertificateInfo
-- ═══════════════════════════════════════════════════════════════════════════════
PRINT '1. Creating new_data.raw_certificate_info...';

IF OBJECT_ID('new_data.raw_certificate_info', 'V') IS NOT NULL
    DROP VIEW new_data.raw_certificate_info;

CREATE VIEW new_data.raw_certificate_info AS
SELECT * FROM new_data.CertificateInfo;
GO

PRINT '   ✅ Created view: new_data.raw_certificate_info → new_data.CertificateInfo';
PRINT '';

-- ═══════════════════════════════════════════════════════════════════════════════
-- 2. Create raw_perf_groups as view to poc_raw_data
-- ═══════════════════════════════════════════════════════════════════════════════
PRINT '2. Creating new_data.raw_perf_groups...';

IF OBJECT_ID('new_data.raw_perf_groups', 'V') IS NOT NULL
    DROP VIEW new_data.raw_perf_groups;

CREATE VIEW new_data.raw_perf_groups AS
SELECT * FROM poc_raw_data.raw_perf_groups;
GO

PRINT '   ✅ Created view: new_data.raw_perf_groups → poc_raw_data.raw_perf_groups';
PRINT '';

-- ═══════════════════════════════════════════════════════════════════════════════
-- 3. Create raw_schedule_rates as view to poc_raw_data
-- ═══════════════════════════════════════════════════════════════════════════════
PRINT '3. Creating new_data.raw_schedule_rates...';

IF OBJECT_ID('new_data.raw_schedule_rates', 'V') IS NOT NULL
    DROP VIEW new_data.raw_schedule_rates;

CREATE VIEW new_data.raw_schedule_rates AS
SELECT * FROM poc_raw_data.raw_schedule_rates;
GO

PRINT '   ✅ Created view: new_data.raw_schedule_rates → poc_raw_data.raw_schedule_rates';
PRINT '';

-- ═══════════════════════════════════════════════════════════════════════════════
-- 4. Create raw_premiums as view to poc_raw_data
-- ═══════════════════════════════════════════════════════════════════════════════
PRINT '4. Creating new_data.raw_premiums...';

IF OBJECT_ID('new_data.raw_premiums', 'V') IS NOT NULL
    DROP VIEW new_data.raw_premiums;

CREATE VIEW new_data.raw_premiums AS
SELECT * FROM poc_raw_data.raw_premiums;
GO

PRINT '   ✅ Created view: new_data.raw_premiums → poc_raw_data.raw_premiums';
PRINT '';

-- ═══════════════════════════════════════════════════════════════════════════════
-- 5. Create raw_individual_brokers as view to poc_raw_data
-- ═══════════════════════════════════════════════════════════════════════════════
PRINT '5. Creating new_data.raw_individual_brokers...';

IF OBJECT_ID('new_data.raw_individual_brokers', 'V') IS NOT NULL
    DROP VIEW new_data.raw_individual_brokers;

CREATE VIEW new_data.raw_individual_brokers AS
SELECT * FROM poc_raw_data.raw_individual_brokers;
GO

PRINT '   ✅ Created view: new_data.raw_individual_brokers → poc_raw_data.raw_individual_brokers';
PRINT '';

-- ═══════════════════════════════════════════════════════════════════════════════
-- 6. Create raw_org_brokers as view to poc_raw_data
-- ═══════════════════════════════════════════════════════════════════════════════
PRINT '6. Creating new_data.raw_org_brokers...';

IF OBJECT_ID('new_data.raw_org_brokers', 'V') IS NOT NULL
    DROP VIEW new_data.raw_org_brokers;

CREATE VIEW new_data.raw_org_brokers AS
SELECT * FROM poc_raw_data.raw_org_brokers;
GO

PRINT '   ✅ Created view: new_data.raw_org_brokers → poc_raw_data.raw_org_brokers';
PRINT '';

-- ═══════════════════════════════════════════════════════════════════════════════
-- 7. Create raw_licenses (if needed)
-- ═══════════════════════════════════════════════════════════════════════════════
PRINT '7. Creating new_data.raw_licenses...';

IF OBJECT_ID('new_data.raw_licenses', 'V') IS NOT NULL
    DROP VIEW new_data.raw_licenses;

CREATE VIEW new_data.raw_licenses AS
SELECT * FROM poc_raw_data.raw_licenses;
GO

PRINT '   ✅ Created view: new_data.raw_licenses → poc_raw_data.raw_licenses';
PRINT '';

-- ═══════════════════════════════════════════════════════════════════════════════
-- 8. Create raw_eo_insurance (if needed)
-- ═══════════════════════════════════════════════════════════════════════════════
PRINT '8. Creating new_data.raw_eo_insurance...';

IF OBJECT_ID('new_data.raw_eo_insurance', 'V') IS NOT NULL
    DROP VIEW new_data.raw_eo_insurance;

CREATE VIEW new_data.raw_eo_insurance AS
SELECT * FROM poc_raw_data.raw_eo_insurance;
GO

PRINT '   ✅ Created view: new_data.raw_eo_insurance → poc_raw_data.raw_eo_insurance';
PRINT '';

-- ═══════════════════════════════════════════════════════════════════════════════
-- Verification
-- ═══════════════════════════════════════════════════════════════════════════════
PRINT '═══════════════════════════════════════════════════════════';
PRINT 'VERIFICATION - View Counts';
PRINT '═══════════════════════════════════════════════════════════';
PRINT '';

SELECT 'raw_certificate_info' AS ViewName, COUNT(*) AS RowCount 
FROM new_data.raw_certificate_info;

SELECT 'raw_perf_groups' AS ViewName, COUNT(*) AS RowCount 
FROM new_data.raw_perf_groups;

SELECT 'raw_schedule_rates' AS ViewName, COUNT(*) AS RowCount 
FROM new_data.raw_schedule_rates;

SELECT 'raw_premiums' AS ViewName, COUNT(*) AS RowCount 
FROM new_data.raw_premiums;

SELECT 'raw_individual_brokers' AS ViewName, COUNT(*) AS RowCount 
FROM new_data.raw_individual_brokers;

SELECT 'raw_org_brokers' AS ViewName, COUNT(*) AS RowCount 
FROM new_data.raw_org_brokers;

PRINT '';
PRINT '═══════════════════════════════════════════════════════════';
PRINT 'RAW TABLE/VIEW CREATION COMPLETE';
PRINT '═══════════════════════════════════════════════════════════';
