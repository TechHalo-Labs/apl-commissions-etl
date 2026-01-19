-- =====================================================
-- Export BrokerLicenses, BrokerAppointments, and BrokerEOInsurances
-- Only exports records that don't already exist
-- Creates appointments for each state where broker has a license
-- =====================================================

-- Create state name lookup table
IF OBJECT_ID('tempdb..#StateNames') IS NOT NULL DROP TABLE #StateNames;
CREATE TABLE #StateNames (StateCode NVARCHAR(2), StateName NVARCHAR(50));
INSERT INTO #StateNames VALUES
('AL','Alabama'),('AK','Alaska'),('AZ','Arizona'),('AR','Arkansas'),('CA','California'),
('CO','Colorado'),('CT','Connecticut'),('DE','Delaware'),('DC','District of Columbia'),('FL','Florida'),
('GA','Georgia'),('HI','Hawaii'),('ID','Idaho'),('IL','Illinois'),('IN','Indiana'),
('IA','Iowa'),('KS','Kansas'),('KY','Kentucky'),('LA','Louisiana'),('ME','Maine'),
('MD','Maryland'),('MA','Massachusetts'),('MI','Michigan'),('MN','Minnesota'),('MS','Mississippi'),
('MO','Missouri'),('MT','Montana'),('NE','Nebraska'),('NV','Nevada'),('NH','New Hampshire'),
('NJ','New Jersey'),('NM','New Mexico'),('NY','New York'),('NC','North Carolina'),('ND','North Dakota'),
('OH','Ohio'),('OK','Oklahoma'),('OR','Oregon'),('PA','Pennsylvania'),('RI','Rhode Island'),
('SC','South Carolina'),('SD','South Dakota'),('TN','Tennessee'),('TX','Texas'),('UT','Utah'),
('VT','Vermont'),('VA','Virginia'),('WA','Washington'),('WV','West Virginia'),('WI','Wisconsin'),
('WY','Wyoming'),('PR','Puerto Rico'),('VI','Virgin Islands'),('GU','Guam'),('AS','American Samoa'),
('MP','Northern Mariana Islands');

PRINT '=== Exporting BrokerLicenses ===';

-- BrokerLicenses has IDENTITY on Id
-- Use COALESCE for LicenseNumber to handle NULLs (use BrokerId-State as fallback)
-- GracePeriodDate set to 2099-01-01 (far future for compliance purposes)
INSERT INTO [dbo].[BrokerLicenses] (
    BrokerId, [State], LicenseNumber, [Type], [Status],
    EffectiveDate, ExpirationDate, GracePeriodDate, IsResidentLicense,
    CreationTime, IsDeleted
)
SELECT 
    sbl.BrokerId,
    sbl.[State],
    COALESCE(sbl.LicenseNumber, CONCAT('LIC-', sbl.BrokerId, '-', sbl.[State])) AS LicenseNumber,
    COALESCE(TRY_CAST(sbl.[Type] AS INT), 0) AS [Type],
    COALESCE(TRY_CAST(sbl.[Status] AS INT), 0) AS [Status],
    COALESCE(sbl.EffectiveDate, GETUTCDATE()) AS EffectiveDate,
    COALESCE(sbl.ExpirationDate, DATEADD(year, 2, GETUTCDATE())) AS ExpirationDate,
    '2099-01-01' AS GracePeriodDate,
    COALESCE(sbl.IsResidentLicense, 0) AS IsResidentLicense,
    COALESCE(sbl.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(sbl.IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_broker_licenses] sbl
WHERE sbl.BrokerId IN (SELECT Id FROM [dbo].[Brokers])
  AND sbl.[State] IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM [dbo].[BrokerLicenses] bl
    WHERE bl.BrokerId = sbl.BrokerId
      AND bl.LicenseNumber = COALESCE(sbl.LicenseNumber, CONCAT('LIC-', sbl.BrokerId, '-', sbl.[State]))
      AND bl.[State] = sbl.[State]
);

DECLARE @licCount INT;
SELECT @licCount = @@ROWCOUNT;
PRINT 'BrokerLicenses exported: ' + CAST(@licCount AS VARCHAR);
GO

PRINT '=== Creating BrokerAppointments from Licenses ===';

-- Create state name lookup table again (previous batch dropped it)
IF OBJECT_ID('tempdb..#StateNames') IS NOT NULL DROP TABLE #StateNames;
CREATE TABLE #StateNames (StateCode NVARCHAR(2), StateName NVARCHAR(50));
INSERT INTO #StateNames VALUES
('AL','Alabama'),('AK','Alaska'),('AZ','Arizona'),('AR','Arkansas'),('CA','California'),
('CO','Colorado'),('CT','Connecticut'),('DE','Delaware'),('DC','District of Columbia'),('FL','Florida'),
('GA','Georgia'),('HI','Hawaii'),('ID','Idaho'),('IL','Illinois'),('IN','Indiana'),
('IA','Iowa'),('KS','Kansas'),('KY','Kentucky'),('LA','Louisiana'),('ME','Maine'),
('MD','Maryland'),('MA','Massachusetts'),('MI','Michigan'),('MN','Minnesota'),('MS','Mississippi'),
('MO','Missouri'),('MT','Montana'),('NE','Nebraska'),('NV','Nevada'),('NH','New Hampshire'),
('NJ','New Jersey'),('NM','New Mexico'),('NY','New York'),('NC','North Carolina'),('ND','North Dakota'),
('OH','Ohio'),('OK','Oklahoma'),('OR','Oregon'),('PA','Pennsylvania'),('RI','Rhode Island'),
('SC','South Carolina'),('SD','South Dakota'),('TN','Tennessee'),('TX','Texas'),('UT','Utah'),
('VT','Vermont'),('VA','Virginia'),('WA','Washington'),('WV','West Virginia'),('WI','Wisconsin'),
('WY','Wyoming'),('PR','Puerto Rico'),('VI','Virgin Islands'),('GU','Guam'),('AS','American Samoa'),
('MP','Northern Mariana Islands');

-- Create appointments for each unique (BrokerId, State) from licenses
-- Uses the earliest effective date and latest expiration for each state
-- GracePeriodDate set to 2099-01-01 (far future for compliance purposes)
INSERT INTO [dbo].[BrokerAppointments] (
    BrokerId, StateCode, StateName, LicenseCode, LicenseCodeLabel,
    EffectiveDate, ExpirationDate, GracePeriodDate, OriginalEffectiveDate, TerminationDate,
    [Status], StatusReason, NiprStatus, IsCommissionEligible,
    CreationTime, IsDeleted
)
SELECT 
    lic.BrokerId,
    lic.[State] AS StateCode,
    COALESCE(sn.StateName, lic.[State]) AS StateName,
    0 AS LicenseCode,  -- Default license code
    'Life & Health' AS LicenseCodeLabel,
    MIN(lic.EffectiveDate) AS EffectiveDate,
    MAX(lic.ExpirationDate) AS ExpirationDate,
    '2099-01-01' AS GracePeriodDate,
    MIN(lic.EffectiveDate) AS OriginalEffectiveDate,
    CASE WHEN MAX(CASE WHEN lic.ExpirationDate >= GETUTCDATE() THEN 1 ELSE 0 END) = 0 
         THEN MAX(lic.ExpirationDate) 
         ELSE NULL 
    END AS TerminationDate,
    -- Status: 0 = Active if any license for this state is active, 1 = Inactive otherwise
    CASE WHEN MAX(CASE WHEN lic.[Status] = 0 AND lic.ExpirationDate >= GETUTCDATE() THEN 1 ELSE 0 END) = 1 
         THEN 0 ELSE 1 
    END AS [Status],
    NULL AS StatusReason,
    'Active' AS NiprStatus,
    -- Commission eligible if any active license exists for this state
    CASE WHEN MAX(CASE WHEN lic.[Status] = 0 AND lic.ExpirationDate >= GETUTCDATE() THEN 1 ELSE 0 END) = 1 
         THEN 1 ELSE 0 
    END AS IsCommissionEligible,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [dbo].[BrokerLicenses] lic
LEFT JOIN #StateNames sn ON sn.StateCode = lic.[State]
WHERE lic.IsDeleted = 0
  AND NOT EXISTS (
    SELECT 1 FROM [dbo].[BrokerAppointments] appt
    WHERE appt.BrokerId = lic.BrokerId
      AND appt.StateCode = lic.[State]
)
GROUP BY lic.BrokerId, lic.[State], sn.StateName;

DECLARE @apptCount INT;
SELECT @apptCount = @@ROWCOUNT;
PRINT 'BrokerAppointments created from licenses: ' + CAST(@apptCount AS VARCHAR);

DECLARE @totalAppts INT;
SELECT @totalAppts = COUNT(*) FROM [dbo].[BrokerAppointments];
PRINT 'Total appointments in dbo: ' + CAST(@totalAppts AS VARCHAR);

DROP TABLE #StateNames;
GO

PRINT 'Exporting missing BrokerEOInsurances...';

-- BrokerEOInsurances has IDENTITY on Id
-- Production schema: Id, BrokerId, PolicyNumber, Carrier, CoverageAmount, MinimumRequired,
--   DeductibleAmount, ClaimMaxAmount, AnnualMaxAmount, PolicyMaxAmount, LiabilityLimit,
--   EffectiveDate, ExpirationDate, GracePeriodDate, Status, CreationTime, IsDeleted

-- BrokerEOInsurances has a UNIQUE constraint on BrokerId (one EO per broker)
-- Only insert for brokers that don't already have an EO record
-- GracePeriodDate set to 2099-01-01 (far future for compliance purposes)
INSERT INTO [dbo].[BrokerEOInsurances] (
    BrokerId, PolicyNumber, Carrier, CoverageAmount, MinimumRequired,
    DeductibleAmount, ClaimMaxAmount, AnnualMaxAmount, PolicyMaxAmount, LiabilityLimit,
    EffectiveDate, ExpirationDate, GracePeriodDate, [Status], CreationTime, IsDeleted
)
SELECT 
    sbeo.BrokerId,
    COALESCE(sbeo.PolicyNumber, 'N/A') AS PolicyNumber,
    COALESCE(sbeo.Carrier, 'Unknown') AS Carrier,
    COALESCE(sbeo.CoverageAmount, 0) AS CoverageAmount,
    COALESCE(sbeo.MinimumRequired, 0) AS MinimumRequired,
    COALESCE(sbeo.DeductibleAmount, 0) AS DeductibleAmount,
    COALESCE(sbeo.ClaimMaxAmount, 0) AS ClaimMaxAmount,
    COALESCE(sbeo.AnnualMaxAmount, 0) AS AnnualMaxAmount,
    COALESCE(sbeo.PolicyMaxAmount, 0) AS PolicyMaxAmount,
    COALESCE(sbeo.LiabilityLimit, 0) AS LiabilityLimit,
    COALESCE(sbeo.EffectiveDate, GETUTCDATE()) AS EffectiveDate,
    COALESCE(sbeo.ExpirationDate, DATEADD(year, 1, GETUTCDATE())) AS ExpirationDate,
    '2099-01-01' AS GracePeriodDate,
    COALESCE(sbeo.[Status], 0) AS [Status],
    COALESCE(sbeo.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(sbeo.IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_broker_eo_insurances] sbeo
WHERE sbeo.BrokerId IN (SELECT Id FROM [dbo].[Brokers])
  AND sbeo.BrokerId NOT IN (SELECT BrokerId FROM [dbo].[BrokerEOInsurances]);

DECLARE @eoCount INT;
SELECT @eoCount = @@ROWCOUNT;
PRINT 'BrokerEOInsurances exported: ' + CAST(@eoCount AS VARCHAR);

DECLARE @totalEO INT;
SELECT @totalEO = COUNT(*) FROM [dbo].[BrokerEOInsurances];
PRINT 'Total EO insurances in dbo: ' + CAST(@totalEO AS VARCHAR);
GO

PRINT '=== License/EO Export Complete ===';
