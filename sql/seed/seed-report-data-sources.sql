-- =============================================================================
-- Seed: Report Connections and Data Sources
-- Seeds the ReportConnections, ReportDataSources, and ReportDataSourceFields tables
-- for the Stimulsoft Designer data sources panel
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'SEED: Report Connections and Data Sources';
PRINT '============================================================';

-- =============================================================================
-- 1. CONNECTIONS
-- =============================================================================

-- SQL Server Connection (Primary)
IF NOT EXISTS (SELECT 1 FROM ReportConnections WHERE Name = 'APL SQL Server')
BEGIN
    SET IDENTITY_INSERT ReportConnections ON;
    INSERT INTO ReportConnections (Id, TenantId, Name, Alias, Description, Type, ConnectionString, ServiceRootUrl, AuthType, IsActive, IsDefault, CreationTime, IsDeleted)
    VALUES (
        1,
        NULL,
        'APL SQL Server',
        'sql-primary',
        'Primary SQL Server connection for APL Commissions database',
        1, -- SqlServer
        'Server=halo-sql.database.windows.net;Database=halo-sqldb;User Id=***REMOVED***;Password=***REMOVED***;TrustServerCertificate=True;Encrypt=True;',
        NULL,
        0, -- None
        1, -- IsActive
        1, -- IsDefault
        GETUTCDATE(),
        0
    );
    SET IDENTITY_INSERT ReportConnections OFF;
    PRINT 'Created SQL Server connection';
END
ELSE
    PRINT 'SQL Server connection already exists';

-- OData Connection
IF NOT EXISTS (SELECT 1 FROM ReportConnections WHERE Name = 'APL Reporting OData')
BEGIN
    SET IDENTITY_INSERT ReportConnections ON;
    INSERT INTO ReportConnections (Id, TenantId, Name, Alias, Description, Type, ConnectionString, ServiceRootUrl, AuthType, IsActive, IsDefault, CreationTime, IsDeleted)
    VALUES (
        2,
        NULL,
        'APL Reporting OData',
        'odata-reporting',
        'OData endpoint for pre-built reporting entities',
        0, -- OData
        NULL,
        'https://localhost:44301/reporting',
        1, -- Bearer
        1, -- IsActive
        0, -- IsDefault
        GETUTCDATE(),
        0
    );
    SET IDENTITY_INSERT ReportConnections OFF;
    PRINT 'Created OData connection';
END
ELSE
    PRINT 'OData connection already exists';

-- =============================================================================
-- 2. DATA SOURCES - SQL Queries
-- =============================================================================

-- Brokers Query
IF NOT EXISTS (SELECT 1 FROM ReportDataSources WHERE Name = 'Brokers')
BEGIN
    SET IDENTITY_INSERT ReportDataSources ON;
    INSERT INTO ReportDataSources (Id, TenantId, ConnectionId, Name, Alias, Description, EntitySetOrTable, SqlCommand, IsGlobal, Category, IsActive, CreationTime, IsDeleted)
    VALUES (
        1,
        NULL,
        1, -- SQL Server connection
        'Brokers',
        'brokers',
        'All brokers with basic information',
        'Brokers',
        'SELECT Id, Name, Email, Status, Type, NPN, ExternalBrokerId, Phone, CreationTime FROM Brokers WHERE IsDeleted = 0 ORDER BY Name',
        1, -- IsGlobal
        'Brokers',
        1, -- IsActive
        GETUTCDATE(),
        0
    );
    SET IDENTITY_INSERT ReportDataSources OFF;
    PRINT 'Created Brokers data source';
END

-- GL Journal Entries Query
IF NOT EXISTS (SELECT 1 FROM ReportDataSources WHERE Name = 'GL Journal Entries')
BEGIN
    SET IDENTITY_INSERT ReportDataSources ON;
    INSERT INTO ReportDataSources (Id, TenantId, ConnectionId, Name, Alias, Description, EntitySetOrTable, SqlCommand, IsGlobal, Category, IsActive, CreationTime, IsDeleted)
    VALUES (
        2,
        NULL,
        1, -- SQL Server connection
        'GL Journal Entries',
        'gl-entries',
        'Commission journal entries with broker and policy details',
        'GLJournalEntries',
        'SELECT TOP 10000 
            gl.Id, gl.BrokerId, b.Name AS BrokerName,
            gl.CommissionAmount, gl.PremiumAmount, gl.TransactionDate,
            gl.PolicyId, gl.CertificateId, gl.ProductCode,
            gl.CommissionRunId, gl.Status
        FROM GLJournalEntries gl
        LEFT JOIN Brokers b ON b.Id = gl.BrokerId
        WHERE gl.IsDeleted = 0
        ORDER BY gl.TransactionDate DESC',
        1, -- IsGlobal
        'Commissions',
        1, -- IsActive
        GETUTCDATE(),
        0
    );
    SET IDENTITY_INSERT ReportDataSources OFF;
    PRINT 'Created GL Journal Entries data source';
END

-- Policies Query
IF NOT EXISTS (SELECT 1 FROM ReportDataSources WHERE Name = 'Policies')
BEGIN
    SET IDENTITY_INSERT ReportDataSources ON;
    INSERT INTO ReportDataSources (Id, TenantId, ConnectionId, Name, Alias, Description, EntitySetOrTable, SqlCommand, IsGlobal, Category, IsActive, CreationTime, IsDeleted)
    VALUES (
        3,
        NULL,
        1, -- SQL Server connection
        'Policies',
        'policies',
        'All policies with group and product information',
        'Policies',
        'SELECT 
            p.Id, p.PolicyNumber, p.GroupId, g.GroupName,
            p.ProductCode, p.EffectiveDate, p.Premium, p.FaceValue,
            p.State, p.Status, p.CustomerId
        FROM Policies p
        LEFT JOIN [Group] g ON g.Id = p.GroupId
        WHERE p.IsDeleted = 0
        ORDER BY p.EffectiveDate DESC',
        1, -- IsGlobal
        'Policies',
        1, -- IsActive
        GETUTCDATE(),
        0
    );
    SET IDENTITY_INSERT ReportDataSources OFF;
    PRINT 'Created Policies data source';
END

-- Groups Query
IF NOT EXISTS (SELECT 1 FROM ReportDataSources WHERE Name = 'Groups')
BEGIN
    SET IDENTITY_INSERT ReportDataSources ON;
    INSERT INTO ReportDataSources (Id, TenantId, ConnectionId, Name, Alias, Description, EntitySetOrTable, SqlCommand, IsGlobal, Category, IsActive, CreationTime, IsDeleted)
    VALUES (
        4,
        NULL,
        1, -- SQL Server connection
        'Groups',
        'groups',
        'Employer groups with policy counts',
        '[Group]',
        'SELECT 
            g.Id, g.GroupNumber, g.GroupName, g.SitusState, g.GroupSize,
            g.IsPublicSector, g.EffectiveDate,
            COUNT(p.Id) AS PolicyCount
        FROM [Group] g
        LEFT JOIN Policies p ON p.GroupId = g.Id AND p.IsDeleted = 0
        WHERE g.IsDeleted = 0
        GROUP BY g.Id, g.GroupNumber, g.GroupName, g.SitusState, g.GroupSize, g.IsPublicSector, g.EffectiveDate
        ORDER BY g.GroupName',
        1, -- IsGlobal
        'Groups',
        1, -- IsActive
        GETUTCDATE(),
        0
    );
    SET IDENTITY_INSERT ReportDataSources OFF;
    PRINT 'Created Groups data source';
END

-- Commission Runs Query
IF NOT EXISTS (SELECT 1 FROM ReportDataSources WHERE Name = 'Commission Runs')
BEGIN
    SET IDENTITY_INSERT ReportDataSources ON;
    INSERT INTO ReportDataSources (Id, TenantId, ConnectionId, Name, Alias, Description, EntitySetOrTable, SqlCommand, IsGlobal, Category, IsActive, CreationTime, IsDeleted)
    VALUES (
        5,
        NULL,
        1, -- SQL Server connection
        'Commission Runs',
        'commission-runs',
        'Commission processing runs with status and totals',
        'CommissionRuns',
        'SELECT 
            Id, Name, Status, 
            TotalPremium, TotalCommission,
            ProcessedCount, ErrorCount,
            StartedAt, CompletedAt,
            CreationTime
        FROM CommissionRuns
        WHERE IsDeleted = 0
        ORDER BY CreationTime DESC',
        1, -- IsGlobal
        'Commissions',
        1, -- IsActive
        GETUTCDATE(),
        0
    );
    SET IDENTITY_INSERT ReportDataSources OFF;
    PRINT 'Created Commission Runs data source';
END

-- Proposals Query
IF NOT EXISTS (SELECT 1 FROM ReportDataSources WHERE Name = 'Proposals')
BEGIN
    SET IDENTITY_INSERT ReportDataSources ON;
    INSERT INTO ReportDataSources (Id, TenantId, ConnectionId, Name, Alias, Description, EntitySetOrTable, SqlCommand, IsGlobal, Category, IsActive, CreationTime, IsDeleted)
    VALUES (
        6,
        NULL,
        1, -- SQL Server connection
        'Proposals',
        'proposals',
        'Commission proposals with group and product details',
        'Proposals',
        'SELECT 
            p.Id, p.ProposalNumber, p.GroupId, g.GroupName,
            p.ProductCode, p.SitusState, p.Status,
            p.EffectiveDate, p.TerminationDate,
            p.SpecialCaseCode
        FROM Proposals p
        LEFT JOIN [Group] g ON g.Id = p.GroupId
        WHERE p.IsDeleted = 0
        ORDER BY p.CreationTime DESC',
        1, -- IsGlobal
        'Proposals',
        1, -- IsActive
        GETUTCDATE(),
        0
    );
    SET IDENTITY_INSERT ReportDataSources OFF;
    PRINT 'Created Proposals data source';
END

-- Hierarchies Query
IF NOT EXISTS (SELECT 1 FROM ReportDataSources WHERE Name = 'Hierarchies')
BEGIN
    SET IDENTITY_INSERT ReportDataSources ON;
    INSERT INTO ReportDataSources (Id, TenantId, ConnectionId, Name, Alias, Description, EntitySetOrTable, SqlCommand, IsGlobal, Category, IsActive, CreationTime, IsDeleted)
    VALUES (
        7,
        NULL,
        1, -- SQL Server connection
        'Hierarchies',
        'hierarchies',
        'Broker hierarchies with participant counts',
        'Hierarchies',
        'SELECT 
            h.Id, h.Name, h.ProposalId, h.Status,
            h.EffectiveDate, h.TerminationDate,
            (SELECT COUNT(*) FROM HierarchyVersions hv WHERE hv.HierarchyId = h.Id) AS VersionCount,
            h.CreationTime
        FROM Hierarchies h
        WHERE h.IsDeleted = 0
        ORDER BY h.CreationTime DESC',
        1, -- IsGlobal
        'Hierarchies',
        1, -- IsActive
        GETUTCDATE(),
        0
    );
    SET IDENTITY_INSERT ReportDataSources OFF;
    PRINT 'Created Hierarchies data source';
END

-- Schedules Query
IF NOT EXISTS (SELECT 1 FROM ReportDataSources WHERE Name = 'Schedules')
BEGIN
    SET IDENTITY_INSERT ReportDataSources ON;
    INSERT INTO ReportDataSources (Id, TenantId, ConnectionId, Name, Alias, Description, EntitySetOrTable, SqlCommand, IsGlobal, Category, IsActive, CreationTime, IsDeleted)
    VALUES (
        8,
        NULL,
        1, -- SQL Server connection
        'Schedules',
        'schedules',
        'Commission rate schedules',
        'Schedules',
        'SELECT 
            s.Id, s.Name, s.Code, s.Description, s.Status,
            s.EffectiveDate, s.TerminationDate,
            (SELECT COUNT(*) FROM ScheduleRates sr WHERE sr.ScheduleId = s.Id) AS RateCount,
            s.CreationTime
        FROM Schedules s
        WHERE s.IsDeleted = 0
        ORDER BY s.Name',
        1, -- IsGlobal
        'Schedules',
        1, -- IsActive
        GETUTCDATE(),
        0
    );
    SET IDENTITY_INSERT ReportDataSources OFF;
    PRINT 'Created Schedules data source';
END

-- =============================================================================
-- 3. DATA SOURCES - OData Entity Sets
-- =============================================================================

-- Production Tracking OData
IF NOT EXISTS (SELECT 1 FROM ReportDataSources WHERE Name = 'Production Tracking (OData)')
BEGIN
    SET IDENTITY_INSERT ReportDataSources ON;
    INSERT INTO ReportDataSources (Id, TenantId, ConnectionId, Name, Alias, Description, EntitySetOrTable, SqlCommand, IsGlobal, Category, IsActive, CreationTime, IsDeleted)
    VALUES (
        10,
        NULL,
        2, -- OData connection
        'Production Tracking (OData)',
        'production-tracking-odata',
        'Production tracking data via OData endpoint',
        'ProductionTracking',
        NULL,
        1, -- IsGlobal
        'Production',
        1, -- IsActive
        GETUTCDATE(),
        0
    );
    SET IDENTITY_INSERT ReportDataSources OFF;
    PRINT 'Created Production Tracking OData data source';
END

-- Brokers OData
IF NOT EXISTS (SELECT 1 FROM ReportDataSources WHERE Name = 'Brokers (OData)')
BEGIN
    SET IDENTITY_INSERT ReportDataSources ON;
    INSERT INTO ReportDataSources (Id, TenantId, ConnectionId, Name, Alias, Description, EntitySetOrTable, SqlCommand, IsGlobal, Category, IsActive, CreationTime, IsDeleted)
    VALUES (
        11,
        NULL,
        2, -- OData connection
        'Brokers (OData)',
        'brokers-odata',
        'Brokers data via OData endpoint with filtering support',
        'Brokers',
        NULL,
        1, -- IsGlobal
        'Brokers',
        1, -- IsActive
        GETUTCDATE(),
        0
    );
    SET IDENTITY_INSERT ReportDataSources OFF;
    PRINT 'Created Brokers OData data source';
END

PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'Connections' AS Entity, COUNT(*) AS Count FROM ReportConnections WHERE IsDeleted = 0;
SELECT 'Data Sources' AS Entity, COUNT(*) AS Count FROM ReportDataSources WHERE IsDeleted = 0;

PRINT '';
PRINT 'By Category:';
SELECT Category, COUNT(*) AS Count FROM ReportDataSources WHERE IsDeleted = 0 GROUP BY Category ORDER BY Category;

PRINT '';
PRINT '============================================================';
PRINT 'SEED COMPLETED';
PRINT '============================================================';
GO
