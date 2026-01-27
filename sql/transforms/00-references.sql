-- =============================================================================
-- Transform: Build Reference Tables (SQL Server)
-- =============================================================================
-- Identify "active" data based on premiums and commission details
-- Usage: sqlcmd -S server -d database -i transforms/00-references.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'BUILDING REFERENCE TABLES';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Create Reference Tables
-- =============================================================================
DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[ref_active_groups];
CREATE TABLE [$(ETL_SCHEMA)].[ref_active_groups] (
    GroupNumber NVARCHAR(100) NOT NULL,
    GroupName NVARCHAR(500),
    [Source] NVARCHAR(50),
    CONSTRAINT PK_ref_active_groups PRIMARY KEY (GroupNumber, [Source])
);

DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[ref_active_certificates];
CREATE TABLE [$(ETL_SCHEMA)].[ref_active_certificates] (
    CertificateId NVARCHAR(100) NOT NULL,
    GroupNumber NVARCHAR(100),
    [Source] NVARCHAR(50),
    CONSTRAINT PK_ref_active_certificates PRIMARY KEY (CertificateId, [Source])
);

DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[ref_active_brokers];
CREATE TABLE [$(ETL_SCHEMA)].[ref_active_brokers] (
    BrokerId NVARCHAR(50) NOT NULL,
    [Source] NVARCHAR(50),
    CONSTRAINT PK_ref_active_brokers PRIMARY KEY (BrokerId, [Source])
);

DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[ref_active_schedules];
CREATE TABLE [$(ETL_SCHEMA)].[ref_active_schedules] (
    ScheduleName NVARCHAR(200) NOT NULL,
    [Source] NVARCHAR(50),
    CONSTRAINT PK_ref_active_schedules PRIMARY KEY (ScheduleName, [Source])
);

PRINT 'Reference tables created.';

GO

-- =============================================================================
-- Active Groups - Groups with premium payments or commission activity
-- =============================================================================
PRINT '';
PRINT 'Building active groups...';

-- Groups from premiums
INSERT INTO [$(ETL_SCHEMA)].[ref_active_groups] (GroupNumber, GroupName, [Source])
SELECT DISTINCT
    GroupNumber,
    MIN(GroupName) AS GroupName,
    'premiums' AS [Source]
FROM [$(ETL_SCHEMA)].[raw_premiums]
WHERE GroupNumber IS NOT NULL AND GroupNumber <> ''
GROUP BY GroupNumber;

PRINT 'Groups from premiums: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Groups from certificate info linked to commissions
INSERT INTO [$(ETL_SCHEMA)].[ref_active_groups] (GroupNumber, GroupName, [Source])
SELECT DISTINCT
    ci.GroupId AS GroupNumber,
    CONCAT('Group ', ci.GroupId) AS GroupName,
    'commissions' AS [Source]
FROM [$(ETL_SCHEMA)].[input_certificate_info] ci
INNER JOIN [$(ETL_SCHEMA)].[input_commission_details] cd ON ci.CertificateId = cd.CertificateId
WHERE ci.GroupId IS NOT NULL AND ci.GroupId <> ''
  AND NOT EXISTS (
      SELECT 1 FROM [$(ETL_SCHEMA)].[ref_active_groups] r 
      WHERE r.GroupNumber = ci.GroupId
  );

PRINT 'Groups from commissions: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT COUNT(*) AS active_groups FROM [$(ETL_SCHEMA)].[ref_active_groups];

GO

-- =============================================================================
-- Active Certificates - Certificates with activity
-- =============================================================================
PRINT '';
PRINT 'Building active certificates...';

-- Certificates from premiums (Policy = CertificateId)
INSERT INTO [$(ETL_SCHEMA)].[ref_active_certificates] (CertificateId, GroupNumber, [Source])
SELECT DISTINCT
    Policy AS CertificateId,
    GroupNumber,
    'premiums' AS [Source]
FROM [$(ETL_SCHEMA)].[raw_premiums]
WHERE Policy IS NOT NULL AND Policy <> '';

PRINT 'Certificates from premiums: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Certificates from commission details
INSERT INTO [$(ETL_SCHEMA)].[ref_active_certificates] (CertificateId, GroupNumber, [Source])
SELECT DISTINCT
    CAST(cd.CertificateId AS NVARCHAR(100)) AS CertificateId,
    COALESCE(ci.GroupId, '') AS GroupNumber,
    'commissions' AS [Source]
FROM [$(ETL_SCHEMA)].[input_commission_details] cd
LEFT JOIN [$(ETL_SCHEMA)].[input_certificate_info] ci ON cd.CertificateId = ci.CertificateId
WHERE cd.CertificateId > 0
  AND NOT EXISTS (
      SELECT 1 FROM [$(ETL_SCHEMA)].[ref_active_certificates] r 
      WHERE r.CertificateId = CAST(cd.CertificateId AS NVARCHAR(100))
  );

PRINT 'Certificates from commissions: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT COUNT(*) AS active_certificates FROM [$(ETL_SCHEMA)].[ref_active_certificates];

GO

-- =============================================================================
-- Active Brokers - Brokers involved in active transactions
-- =============================================================================
PRINT '';
PRINT 'Building active brokers...';

-- Writing brokers from active certificates
INSERT INTO [$(ETL_SCHEMA)].[ref_active_brokers] (BrokerId, [Source])
SELECT DISTINCT
    ci.WritingBrokerID AS BrokerId,
    'certificate_writing' AS [Source]
FROM [$(ETL_SCHEMA)].[input_certificate_info] ci
WHERE CAST(ci.CertificateId AS NVARCHAR(100)) IN (SELECT CertificateId FROM [$(ETL_SCHEMA)].[ref_active_certificates])
  AND ci.WritingBrokerID IS NOT NULL AND ci.WritingBrokerID <> '';

PRINT 'Writing brokers: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Split brokers from active certificates
INSERT INTO [$(ETL_SCHEMA)].[ref_active_brokers] (BrokerId, [Source])
SELECT DISTINCT
    ci.SplitBrokerId AS BrokerId,
    'certificate_split' AS [Source]
FROM [$(ETL_SCHEMA)].[input_certificate_info] ci
WHERE CAST(ci.CertificateId AS NVARCHAR(100)) IN (SELECT CertificateId FROM [$(ETL_SCHEMA)].[ref_active_certificates])
  AND ci.SplitBrokerId IS NOT NULL AND ci.SplitBrokerId <> ''
  AND NOT EXISTS (
      SELECT 1 FROM [$(ETL_SCHEMA)].[ref_active_brokers] r 
      WHERE r.BrokerId = ci.SplitBrokerId
  );

PRINT 'Split brokers: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Paid brokers from active certificates
INSERT INTO [$(ETL_SCHEMA)].[ref_active_brokers] (BrokerId, [Source])
SELECT DISTINCT
    ci.PaidBrokerId AS BrokerId,
    'certificate_paid' AS [Source]
FROM [$(ETL_SCHEMA)].[input_certificate_info] ci
WHERE CAST(ci.CertificateId AS NVARCHAR(100)) IN (SELECT CertificateId FROM [$(ETL_SCHEMA)].[ref_active_certificates])
  AND ci.PaidBrokerId IS NOT NULL AND ci.PaidBrokerId <> ''
  AND NOT EXISTS (
      SELECT 1 FROM [$(ETL_SCHEMA)].[ref_active_brokers] r 
      WHERE r.BrokerId = ci.PaidBrokerId
  );

PRINT 'Paid brokers: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Split brokers from commission details
INSERT INTO [$(ETL_SCHEMA)].[ref_active_brokers] (BrokerId, [Source])
SELECT DISTINCT
    cd.SplitBrokerId AS BrokerId,
    'commission_split' AS [Source]
FROM [$(ETL_SCHEMA)].[input_commission_details] cd
WHERE cd.SplitBrokerId IS NOT NULL AND cd.SplitBrokerId <> ''
  AND NOT EXISTS (
      SELECT 1 FROM [$(ETL_SCHEMA)].[ref_active_brokers] r 
      WHERE r.BrokerId = cd.SplitBrokerId
  );

PRINT 'Commission split brokers: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Paid brokers from commission details
INSERT INTO [$(ETL_SCHEMA)].[ref_active_brokers] (BrokerId, [Source])
SELECT DISTINCT
    cd.PaidBrokerId AS BrokerId,
    'commission_paid' AS [Source]
FROM [$(ETL_SCHEMA)].[input_commission_details] cd
WHERE cd.PaidBrokerId IS NOT NULL AND cd.PaidBrokerId <> ''
  AND NOT EXISTS (
      SELECT 1 FROM [$(ETL_SCHEMA)].[ref_active_brokers] r 
      WHERE r.BrokerId = cd.PaidBrokerId
  );

PRINT 'Commission paid brokers: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT COUNT(*) AS active_brokers FROM [$(ETL_SCHEMA)].[ref_active_brokers];

GO

-- =============================================================================
-- Active Schedules - Schedules used in commission payments
-- =============================================================================
PRINT '';
PRINT 'Building active schedules...';

-- Schedules from commission details
INSERT INTO [$(ETL_SCHEMA)].[ref_active_schedules] (ScheduleName, [Source])
SELECT DISTINCT
    LTRIM(RTRIM(CommissionsSchedule)) AS ScheduleName,
    'commission_detail' AS [Source]
FROM [$(ETL_SCHEMA)].[input_commission_details]
WHERE CommissionsSchedule IS NOT NULL 
  AND CommissionsSchedule <> ''
  AND CommissionsSchedule <> 'NULL'
  AND LTRIM(RTRIM(CommissionsSchedule)) <> '';

PRINT 'Schedules from commission details: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Schedules from active certificates
INSERT INTO [$(ETL_SCHEMA)].[ref_active_schedules] (ScheduleName, [Source])
SELECT DISTINCT
    LTRIM(RTRIM(ci.CommissionsSchedule)) AS ScheduleName,
    'certificate_info' AS [Source]
FROM [$(ETL_SCHEMA)].[input_certificate_info] ci
WHERE CAST(ci.CertificateId AS NVARCHAR(100)) IN (SELECT CertificateId FROM [$(ETL_SCHEMA)].[ref_active_certificates])
  AND ci.CommissionsSchedule IS NOT NULL AND ci.CommissionsSchedule <> ''
  AND NOT EXISTS (
      SELECT 1 FROM [$(ETL_SCHEMA)].[ref_active_schedules] r 
      WHERE r.ScheduleName = LTRIM(RTRIM(ci.CommissionsSchedule))
  );

PRINT 'Schedules from certificate info: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT COUNT(*) AS active_schedules FROM [$(ETL_SCHEMA)].[ref_active_schedules];

GO

-- =============================================================================
-- Summary
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'REFERENCE TABLES SUMMARY';
PRINT '============================================================';

SELECT 
    'Reference tables built successfully' AS [status],
    (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[ref_active_groups]) AS groups,
    (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[ref_active_certificates]) AS certificates,
    (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[ref_active_brokers]) AS brokers,
    (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[ref_active_schedules]) AS schedules;

GO

