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
    -- If Name is NULL or empty, concatenate FirstName + LastName
    CASE 
        WHEN sb.Name IS NULL OR LTRIM(RTRIM(sb.Name)) = '' 
        THEN LTRIM(RTRIM(COALESCE(sb.FirstName, '') + ' ' + COALESCE(sb.LastName, '')))
        ELSE sb.Name 
    END AS Name,
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

DECLARE @insertedCount INT = @@ROWCOUNT;
PRINT 'Brokers inserted: ' + CAST(@insertedCount AS VARCHAR);
GO

-- Update ExternalPartyId for existing brokers (required for FK constraints)
PRINT 'Updating ExternalPartyId for existing brokers...';

UPDATE pb
SET pb.ExternalPartyId = sb.ExternalPartyId
FROM [$(PRODUCTION_SCHEMA)].[Brokers] pb
INNER JOIN [$(ETL_SCHEMA)].[stg_brokers] sb ON sb.Id = pb.Id
WHERE pb.ExternalPartyId IS NULL 
   OR pb.ExternalPartyId <> sb.ExternalPartyId;

DECLARE @updatedCount INT = @@ROWCOUNT;
PRINT 'Brokers ExternalPartyId updated: ' + CAST(@updatedCount AS VARCHAR);
GO

-- Update Name for existing brokers with NULL/empty Name (use FirstName + LastName)
PRINT 'Updating Name for existing brokers with NULL/empty Name...';

UPDATE pb
SET pb.Name = LTRIM(RTRIM(COALESCE(sb.FirstName, '') + ' ' + COALESCE(sb.LastName, '')))
FROM [$(PRODUCTION_SCHEMA)].[Brokers] pb
INNER JOIN [$(ETL_SCHEMA)].[stg_brokers] sb ON sb.Id = pb.Id
WHERE (pb.Name IS NULL OR LTRIM(RTRIM(pb.Name)) = '')
  AND (sb.FirstName IS NOT NULL OR sb.LastName IS NOT NULL);

DECLARE @nameUpdatedCount INT = @@ROWCOUNT;
PRINT 'Brokers Name updated: ' + CAST(@nameUpdatedCount AS VARCHAR);
GO

DECLARE @brokerCount INT;
SELECT @brokerCount = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[Brokers];
PRINT 'Total brokers in dbo: ' + CAST(@brokerCount AS VARCHAR);
GO

PRINT '=== Broker Export Complete ===';

