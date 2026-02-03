-- ================================================================
-- EXPORT: Broker Licenses and Appointments
-- ================================================================
-- Exports from etl.stg_broker_licenses to dbo.BrokerLicenses
-- Also creates corresponding BrokerAppointments for each license state
-- Sets GracePeriodDate to 2099-01-01 for all licenses and appointments
-- ================================================================

SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;
GO

PRINT '================================================================'
PRINT 'EXPORT: Broker Licenses and Appointments'
PRINT '================================================================'
PRINT ''

-- ================================================================
-- Step 1: Clear existing license data
-- ================================================================
PRINT 'Step 1: Clearing existing BrokerLicenses...'

DELETE FROM dbo.BrokerLicenses;
PRINT '  Cleared BrokerLicenses: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows'
PRINT ''
GO

-- ================================================================
-- Step 2: Export Broker Licenses
-- ================================================================
PRINT 'Step 2: Exporting Broker Licenses...'

SET IDENTITY_INSERT dbo.BrokerLicenses ON;

INSERT INTO dbo.BrokerLicenses (
    Id, BrokerId, State, LicenseNumber, Type, Status,
    EffectiveDate, ExpirationDate, GracePeriodDate,
    LicenseCode, IsResidentLicense, ApplicableCounty,
    CreationTime, IsDeleted
)
SELECT 
    Id,
    BrokerId,
    COALESCE(State, 'XX') AS State,  -- Default state if NULL
    COALESCE(LicenseNumber, 'N/A') AS LicenseNumber,  -- Default if NULL
    COALESCE(Type, 0) AS Type,
    COALESCE(Status, 0) AS Status,
    COALESCE(EffectiveDate, GETUTCDATE()) AS EffectiveDate,
    COALESCE(ExpirationDate, '2099-01-01') AS ExpirationDate,
    '2099-01-01' AS GracePeriodDate,  -- Far-future grace period
    LicenseCode,
    COALESCE(IsResidentLicense, 0) AS IsResidentLicense,
    ApplicableCounty,
    COALESCE(CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(IsDeleted, 0) AS IsDeleted
FROM etl.stg_broker_licenses
WHERE BrokerId IS NOT NULL
  AND EXISTS (SELECT 1 FROM dbo.Brokers WHERE Id = stg_broker_licenses.BrokerId);

SET IDENTITY_INSERT dbo.BrokerLicenses OFF;

DECLARE @LicenseCount INT = @@ROWCOUNT;
PRINT '  Exported ' + CAST(@LicenseCount AS VARCHAR) + ' Broker Licenses'
PRINT ''
GO

-- ================================================================
-- Step 3: Create/Update Broker Appointments for each license state
-- ================================================================
PRINT 'Step 3: Creating Broker Appointments for license states...'

-- State name mapping
DECLARE @StateNames TABLE (StateCode NVARCHAR(2), StateName NVARCHAR(50));
INSERT INTO @StateNames VALUES
('AL','Alabama'),('AK','Alaska'),('AZ','Arizona'),('AR','Arkansas'),('CA','California'),
('CO','Colorado'),('CT','Connecticut'),('DE','Delaware'),('FL','Florida'),('GA','Georgia'),
('HI','Hawaii'),('ID','Idaho'),('IL','Illinois'),('IN','Indiana'),('IA','Iowa'),
('KS','Kansas'),('KY','Kentucky'),('LA','Louisiana'),('ME','Maine'),('MD','Maryland'),
('MA','Massachusetts'),('MI','Michigan'),('MN','Minnesota'),('MS','Mississippi'),('MO','Missouri'),
('MT','Montana'),('NE','Nebraska'),('NV','Nevada'),('NH','New Hampshire'),('NJ','New Jersey'),
('NM','New Mexico'),('NY','New York'),('NC','North Carolina'),('ND','North Dakota'),('OH','Ohio'),
('OK','Oklahoma'),('OR','Oregon'),('PA','Pennsylvania'),('RI','Rhode Island'),('SC','South Carolina'),
('SD','South Dakota'),('TN','Tennessee'),('TX','Texas'),('UT','Utah'),('VT','Vermont'),
('VA','Virginia'),('WA','Washington'),('WV','West Virginia'),('WI','Wisconsin'),('WY','Wyoming'),
('DC','District of Columbia'),('PR','Puerto Rico'),('VI','Virgin Islands'),('GU','Guam'),
('XX','Unknown');

-- Insert appointments for licenses where appointment doesn't exist
INSERT INTO dbo.BrokerAppointments (
    BrokerId, StateCode, StateName, LicenseCode, LicenseCodeLabel,
    EffectiveDate, ExpirationDate, GracePeriodDate, OriginalEffectiveDate,
    Status, NiprStatus, IsCommissionEligible,
    CreationTime, IsDeleted
)
SELECT DISTINCT
    bl.BrokerId,
    bl.State AS StateCode,
    COALESCE(sn.StateName, bl.State) AS StateName,
    bl.Type AS LicenseCode,
    CASE bl.Type 
        WHEN 0 THEN 'Life'
        WHEN 1 THEN 'Health'
        WHEN 2 THEN 'Variable'
        ELSE 'Other'
    END AS LicenseCodeLabel,
    bl.EffectiveDate,
    bl.ExpirationDate,
    '2099-01-01' AS GracePeriodDate,  -- Far-future grace period
    bl.EffectiveDate AS OriginalEffectiveDate,
    bl.Status,
    'Active' AS NiprStatus,
    1 AS IsCommissionEligible,  -- Eligible by default
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM dbo.BrokerLicenses bl
LEFT JOIN @StateNames sn ON sn.StateCode = bl.State
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BrokerAppointments ba 
    WHERE ba.BrokerId = bl.BrokerId AND ba.StateCode = bl.State
);

PRINT '  Created ' + CAST(@@ROWCOUNT AS VARCHAR) + ' new Broker Appointments'
PRINT ''
GO

-- ================================================================
-- Step 4: Update existing appointments to have 2099-01-01 grace period
-- ================================================================
PRINT 'Step 4: Updating all appointments to 2099-01-01 grace period...'

UPDATE dbo.BrokerAppointments
SET GracePeriodDate = '2099-01-01'
WHERE GracePeriodDate IS NULL OR GracePeriodDate <> '2099-01-01';

PRINT '  Updated ' + CAST(@@ROWCOUNT AS VARCHAR) + ' appointments with 2099-01-01 grace period'
PRINT ''
GO

-- ================================================================
-- Step 5: Verification
-- ================================================================
PRINT '================================================================'
PRINT 'VERIFICATION'
PRINT '================================================================'
PRINT ''

SELECT 'BrokerLicenses' AS Entity,
    (SELECT COUNT(*) FROM etl.stg_broker_licenses) AS Staging,
    (SELECT COUNT(*) FROM dbo.BrokerLicenses) AS Production
UNION ALL
SELECT 'BrokerAppointments',
    NULL,  -- No staging table for appointments
    (SELECT COUNT(*) FROM dbo.BrokerAppointments);

PRINT ''
PRINT 'Grace Period Check:'
SELECT 'Licenses with 2099 GracePeriod' AS Check_, 
    COUNT(*) AS Cnt 
FROM dbo.BrokerLicenses 
WHERE GracePeriodDate = '2099-01-01';

SELECT 'Appointments with 2099 GracePeriod' AS Check_, 
    COUNT(*) AS Cnt 
FROM dbo.BrokerAppointments 
WHERE GracePeriodDate = '2099-01-01';

PRINT ''
PRINT '================================================================'
PRINT 'EXPORT COMPLETE'
PRINT '================================================================'
GO
