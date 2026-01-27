-- =====================================================
-- Export Brokers from etl staging to dbo
-- Only exports brokers that don't already exist
-- =====================================================

PRINT 'Exporting missing brokers to dbo.Brokers...';

SET IDENTITY_INSERT [$(PRODUCTION_SCHEMA)].[Brokers] ON;

INSERT INTO [$(PRODUCTION_SCHEMA)].[Brokers] (
    Id, ExternalPartyId, Name, FirstName, LastName, MiddleName, Suffix,
    [Type], [Status], Email, Phone, Npn, TaxId, Ssn,
    DateOfBirth, AppointmentDate, HireDate, DateContracted,
    BrokerClassification, HierarchyLevel, UplineId, UplineName, DownlineCount,
    AddressLine1, AddressLine2, City, [State], ZipCode, Country,
    PrimaryContactName, PrimaryContactRole, GroupId,
    CreationTime, IsDeleted
)
SELECT 
    sb.Id,
    sb.ExternalPartyId,
    sb.Name,
    sb.FirstName,
    sb.LastName,
    sb.MiddleName,
    sb.Suffix,
    CASE sb.[Type] WHEN 'Individual' THEN 0 WHEN 'Organization' THEN 1 ELSE 0 END AS [Type],
    -- BrokerStatus enum: Active=0, PendingReview=1, PendingOnboarding=2, Inactive=3, ComplianceIssues=4, Suspended=5, Terminated=6, TerminatedResiduals=7
    CASE sb.[Status] 
        WHEN 'Active' THEN 0 
        WHEN 'Terminated' THEN 6 
        WHEN 'TerminatedResiduals' THEN 7 
        ELSE 0 
    END AS [Status],
    sb.Email,
    sb.Phone,
    sb.Npn,
    sb.TaxId,
    sb.Ssn,
    sb.DateOfBirth,
    sb.AppointmentDate,
    sb.HireDate,
    sb.DateContracted,
    sb.BrokerClassification,
    sb.HierarchyLevel,
    sb.UplineId,
    sb.UplineName,
    sb.DownlineCount,
    sb.AddressLine1,
    sb.AddressLine2,
    sb.City,
    sb.[State],
    sb.ZipCode,
    sb.Country,
    sb.PrimaryContactName,
    sb.PrimaryContactRole,
    NULL AS GroupId,
    sb.CreationTime,
    sb.IsDeleted
FROM [$(ETL_SCHEMA)].[stg_brokers] sb
WHERE sb.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Brokers]);

SET IDENTITY_INSERT [$(PRODUCTION_SCHEMA)].[Brokers] OFF;

DECLARE @brokerCount INT;
SELECT @brokerCount = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[Brokers];
PRINT 'Total brokers in dbo: ' + CAST(@brokerCount AS VARCHAR);
GO

PRINT '=== Broker Export Complete ===';

