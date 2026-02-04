-- =============================================================================
-- BROKER DATA SYNC: Staging (ETL) → Production (DBO)
-- =============================================================================
-- 
-- This script synchronizes broker data from staging to production with:
-- 1. Full backup capability for rollback
-- 2. Soft deletes for records not in staging
-- 3. Updates for existing records
-- 4. Inserts for new records
-- 5. Restoration of previously deleted records that reappear in staging
--
-- Tables synced:
-- - Brokers
-- - BrokerLicenses
-- - BrokerEOInsurances
-- - BrokerAppointments (created from licenses)
-- - EmployerGroups (broker references only)
--
-- Usage:
--   1. Run with @DryRun = 1 first to preview changes
--   2. Run with @DryRun = 0 to apply changes
--   3. To rollback: EXEC sp_broker_sync_rollback @BackupTimestamp = '<timestamp>'
--
-- =============================================================================

SET NOCOUNT ON;

DECLARE @DryRun BIT = 1;  -- SET TO 0 TO APPLY CHANGES
DECLARE @BackupTimestamp VARCHAR(20) = FORMAT(GETUTCDATE(), 'yyyyMMdd_HHmmss');
DECLARE @ETLSchema NVARCHAR(50) = 'etl';
DECLARE @ProdSchema NVARCHAR(50) = 'dbo';

PRINT '======================================================================';
PRINT 'BROKER DATA SYNC: ' + @ETLSchema + ' → ' + @ProdSchema;
PRINT 'Backup Timestamp: ' + @BackupTimestamp;
PRINT 'Dry Run: ' + CASE WHEN @DryRun = 1 THEN 'YES (preview only)' ELSE 'NO (applying changes)' END;
PRINT '======================================================================';
PRINT '';

-- =============================================================================
-- STEP 0: Create Backup Schema and Tables
-- =============================================================================

PRINT 'STEP 0: Creating backup tables...';

IF @DryRun = 0
BEGIN
    -- Create backup schema if not exists
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'backup')
    BEGIN
        EXEC('CREATE SCHEMA [backup]');
        PRINT '  Created [backup] schema';
    END

    -- Backup Brokers
    DECLARE @BrokerBackupTable NVARCHAR(200) = 'backup.Brokers_' + @BackupTimestamp;
    EXEC('SELECT * INTO ' + @BrokerBackupTable + ' FROM [dbo].[Brokers]');
    PRINT '  ✓ Backed up Brokers to ' + @BrokerBackupTable;

    -- Backup BrokerLicenses
    DECLARE @LicenseBackupTable NVARCHAR(200) = 'backup.BrokerLicenses_' + @BackupTimestamp;
    EXEC('SELECT * INTO ' + @LicenseBackupTable + ' FROM [dbo].[BrokerLicenses]');
    PRINT '  ✓ Backed up BrokerLicenses to ' + @LicenseBackupTable;

    -- Backup BrokerEOInsurances
    DECLARE @EOBackupTable NVARCHAR(200) = 'backup.BrokerEOInsurances_' + @BackupTimestamp;
    EXEC('SELECT * INTO ' + @EOBackupTable + ' FROM [dbo].[BrokerEOInsurances]');
    PRINT '  ✓ Backed up BrokerEOInsurances to ' + @EOBackupTable;

    -- Backup BrokerAppointments
    DECLARE @ApptBackupTable NVARCHAR(200) = 'backup.BrokerAppointments_' + @BackupTimestamp;
    EXEC('SELECT * INTO ' + @ApptBackupTable + ' FROM [dbo].[BrokerAppointments]');
    PRINT '  ✓ Backed up BrokerAppointments to ' + @ApptBackupTable;

    -- Backup EmployerGroups
    DECLARE @EGBackupTable NVARCHAR(200) = 'backup.EmployerGroups_' + @BackupTimestamp;
    EXEC('SELECT * INTO ' + @EGBackupTable + ' FROM [dbo].[EmployerGroups]');
    PRINT '  ✓ Backed up EmployerGroups to ' + @EGBackupTable;
END
ELSE
BEGIN
    PRINT '  [DRY RUN] Would create backup tables with timestamp: ' + @BackupTimestamp;
END

PRINT '';

-- =============================================================================
-- STEP 1: Analyze Differences
-- =============================================================================

PRINT 'STEP 1: Analyzing differences...';
PRINT '';

-- Brokers analysis
DECLARE @BrokersInStaging INT, @BrokersInProd INT, @BrokersToSoftDelete INT, 
        @BrokersToUpdate INT, @BrokersToInsert INT, @BrokersToRestore INT;

SELECT @BrokersInStaging = COUNT(*) FROM [etl].[stg_brokers] WHERE IsDeleted = 0;
SELECT @BrokersInProd = COUNT(*) FROM [dbo].[Brokers] WHERE IsDeleted = 0;

-- To soft delete: in prod (active) but not in staging
SELECT @BrokersToSoftDelete = COUNT(*)
FROM [dbo].[Brokers] pb
WHERE pb.IsDeleted = 0
  AND NOT EXISTS (
    SELECT 1 FROM [etl].[stg_brokers] sb 
    WHERE sb.Id = pb.Id AND sb.IsDeleted = 0
  );

-- To update: in both, but data differs
SELECT @BrokersToUpdate = COUNT(*)
FROM [dbo].[Brokers] pb
INNER JOIN [etl].[stg_brokers] sb ON sb.Id = pb.Id
WHERE sb.IsDeleted = 0
  AND (
    ISNULL(pb.ExternalPartyId, '') <> ISNULL(sb.ExternalPartyId, '')
    OR ISNULL(pb.Name, '') <> ISNULL(CASE WHEN sb.Name IS NULL OR LTRIM(RTRIM(sb.Name)) = '' 
         THEN LTRIM(RTRIM(COALESCE(sb.FirstName, '') + ' ' + COALESCE(sb.LastName, '')))
         ELSE sb.Name END, '')
    OR ISNULL(pb.FirstName, '') <> ISNULL(sb.FirstName, '')
    OR ISNULL(pb.LastName, '') <> ISNULL(sb.LastName, '')
    OR ISNULL(pb.Email, '') <> ISNULL(sb.Email, '')
    OR ISNULL(pb.Phone, '') <> ISNULL(sb.Phone, '')
    OR ISNULL(pb.Npn, '') <> ISNULL(sb.Npn, '')
  );

-- To insert: in staging but not in prod
SELECT @BrokersToInsert = COUNT(*)
FROM [etl].[stg_brokers] sb
WHERE sb.IsDeleted = 0
  AND NOT EXISTS (
    SELECT 1 FROM [dbo].[Brokers] pb WHERE pb.Id = sb.Id
  );

-- To restore: in staging (active) but soft-deleted in prod
SELECT @BrokersToRestore = COUNT(*)
FROM [dbo].[Brokers] pb
INNER JOIN [etl].[stg_brokers] sb ON sb.Id = pb.Id
WHERE pb.IsDeleted = 1
  AND sb.IsDeleted = 0;

PRINT '  BROKERS:';
PRINT '    Staging (active):     ' + CAST(@BrokersInStaging AS VARCHAR);
PRINT '    Production (active):  ' + CAST(@BrokersInProd AS VARCHAR);
PRINT '    To soft delete:       ' + CAST(@BrokersToSoftDelete AS VARCHAR);
PRINT '    To update:            ' + CAST(@BrokersToUpdate AS VARCHAR);
PRINT '    To insert (new):      ' + CAST(@BrokersToInsert AS VARCHAR);
PRINT '    To restore:           ' + CAST(@BrokersToRestore AS VARCHAR);
PRINT '';

-- Licenses analysis
DECLARE @LicensesInStaging INT, @LicensesInProd INT, @LicensesToSoftDelete INT,
        @LicensesToUpdate INT, @LicensesToInsert INT;

SELECT @LicensesInStaging = COUNT(*) FROM [etl].[stg_broker_licenses] WHERE IsDeleted = 0;
SELECT @LicensesInProd = COUNT(*) FROM [dbo].[BrokerLicenses] WHERE IsDeleted = 0;

SELECT @LicensesToSoftDelete = COUNT(*)
FROM [dbo].[BrokerLicenses] pl
WHERE pl.IsDeleted = 0
  AND NOT EXISTS (
    SELECT 1 FROM [etl].[stg_broker_licenses] sl 
    WHERE sl.Id = pl.Id AND sl.IsDeleted = 0
  );

SELECT @LicensesToInsert = COUNT(*)
FROM [etl].[stg_broker_licenses] sl
WHERE sl.IsDeleted = 0
  AND NOT EXISTS (
    SELECT 1 FROM [dbo].[BrokerLicenses] pl WHERE pl.Id = sl.Id
  );

PRINT '  LICENSES:';
PRINT '    Staging (active):     ' + CAST(@LicensesInStaging AS VARCHAR);
PRINT '    Production (active):  ' + CAST(@LicensesInProd AS VARCHAR);
PRINT '    To soft delete:       ' + CAST(@LicensesToSoftDelete AS VARCHAR);
PRINT '    To insert (new):      ' + CAST(@LicensesToInsert AS VARCHAR);
PRINT '';

-- E&O analysis
DECLARE @EOInStaging INT, @EOInProd INT, @EOToSoftDelete INT, @EOToInsert INT;

SELECT @EOInStaging = COUNT(*) FROM [etl].[stg_broker_eo_insurances] WHERE IsDeleted = 0;
SELECT @EOInProd = COUNT(*) FROM [dbo].[BrokerEOInsurances] WHERE IsDeleted = 0;

SELECT @EOToSoftDelete = COUNT(*)
FROM [dbo].[BrokerEOInsurances] pe
WHERE pe.IsDeleted = 0
  AND NOT EXISTS (
    SELECT 1 FROM [etl].[stg_broker_eo_insurances] se 
    WHERE se.Id = pe.Id AND se.IsDeleted = 0
  );

SELECT @EOToInsert = COUNT(*)
FROM [etl].[stg_broker_eo_insurances] se
WHERE se.IsDeleted = 0
  AND NOT EXISTS (
    SELECT 1 FROM [dbo].[BrokerEOInsurances] pe WHERE pe.Id = se.Id
  );

PRINT '  E&O INSURANCES:';
PRINT '    Staging (active):     ' + CAST(@EOInStaging AS VARCHAR);
PRINT '    Production (active):  ' + CAST(@EOInProd AS VARCHAR);
PRINT '    To soft delete:       ' + CAST(@EOToSoftDelete AS VARCHAR);
PRINT '    To insert (new):      ' + CAST(@EOToInsert AS VARCHAR);
PRINT '';

IF @DryRun = 1
BEGIN
    PRINT '======================================================================';
    PRINT 'DRY RUN COMPLETE - No changes made';
    PRINT 'Set @DryRun = 0 and re-run to apply changes';
    PRINT '======================================================================';
    RETURN;
END

-- =============================================================================
-- STEP 2: Sync Brokers
-- =============================================================================

PRINT '';
PRINT 'STEP 2: Syncing Brokers...';

BEGIN TRY
    BEGIN TRANSACTION;

    -- 2a. Soft delete brokers not in staging
    UPDATE pb
    SET pb.IsDeleted = 1,
        pb.DeletionTime = GETUTCDATE(),
        pb.LastModificationTime = GETUTCDATE()
    FROM [dbo].[Brokers] pb
    WHERE pb.IsDeleted = 0
      AND NOT EXISTS (
        SELECT 1 FROM [etl].[stg_brokers] sb 
        WHERE sb.Id = pb.Id AND sb.IsDeleted = 0
      );
    PRINT '  ✓ Soft deleted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' brokers';

    -- 2b. Restore previously deleted brokers that reappear in staging
    UPDATE pb
    SET pb.IsDeleted = 0,
        pb.DeletionTime = NULL,
        pb.DeleterUserId = NULL,
        pb.LastModificationTime = GETUTCDATE()
    FROM [dbo].[Brokers] pb
    INNER JOIN [etl].[stg_brokers] sb ON sb.Id = pb.Id
    WHERE pb.IsDeleted = 1
      AND sb.IsDeleted = 0;
    PRINT '  ✓ Restored ' + CAST(@@ROWCOUNT AS VARCHAR) + ' brokers';

    -- 2c. Update existing brokers
    UPDATE pb
    SET 
        pb.ExternalPartyId = sb.ExternalPartyId,
        pb.Name = CASE 
            WHEN sb.Name IS NULL OR LTRIM(RTRIM(sb.Name)) = '' 
            THEN LTRIM(RTRIM(COALESCE(sb.FirstName, '') + ' ' + COALESCE(sb.LastName, '')))
            ELSE sb.Name 
        END,
        pb.FirstName = sb.FirstName,
        pb.LastName = sb.LastName,
        pb.MiddleName = sb.MiddleName,
        pb.Suffix = sb.Suffix,
        pb.Email = sb.Email,
        pb.Phone = sb.Phone,
        pb.Npn = sb.Npn,
        pb.TaxId = sb.TaxId,
        pb.Ssn = sb.Ssn,
        pb.DateOfBirth = sb.DateOfBirth,
        pb.AppointmentDate = sb.AppointmentDate,
        pb.HireDate = sb.HireDate,
        pb.DateContracted = sb.DateContracted,
        pb.BrokerClassification = sb.BrokerClassification,
        pb.HierarchyLevel = sb.HierarchyLevel,
        pb.UplineId = sb.UplineId,
        pb.UplineName = sb.UplineName,
        pb.DownlineCount = ISNULL(sb.DownlineCount, 0),
        pb.AddressLine1 = sb.AddressLine1,
        pb.AddressLine2 = sb.AddressLine2,
        pb.City = sb.City,
        pb.State = sb.State,
        pb.ZipCode = sb.ZipCode,
        pb.Country = sb.Country,
        pb.PrimaryContactName = sb.PrimaryContactName,
        pb.PrimaryContactRole = sb.PrimaryContactRole,
        pb.LastModificationTime = GETUTCDATE()
    FROM [dbo].[Brokers] pb
    INNER JOIN [etl].[stg_brokers] sb ON sb.Id = pb.Id
    WHERE sb.IsDeleted = 0;
    PRINT '  ✓ Updated ' + CAST(@@ROWCOUNT AS VARCHAR) + ' brokers';

    -- 2d. Insert new brokers
    SET IDENTITY_INSERT [dbo].[Brokers] ON;

    INSERT INTO [dbo].[Brokers] (
        Id, ExternalPartyId, Name, FirstName, LastName, MiddleName, Suffix,
        Type, Status, Email, Phone, Npn, TaxId, Ssn,
        DateOfBirth, AppointmentDate, HireDate, DateContracted,
        BrokerClassification, HierarchyLevel, UplineId, UplineName, DownlineCount,
        AddressLine1, AddressLine2, City, State, ZipCode, Country,
        PrimaryContactName, PrimaryContactRole,
        CreationTime, IsDeleted, EarnedCommissionLast3Months, IsAssigneeOnly
    )
    SELECT 
        sb.Id,
        sb.ExternalPartyId,
        CASE 
            WHEN sb.Name IS NULL OR LTRIM(RTRIM(sb.Name)) = '' 
            THEN LTRIM(RTRIM(COALESCE(sb.FirstName, '') + ' ' + COALESCE(sb.LastName, '')))
            ELSE sb.Name 
        END,
        sb.FirstName,
        sb.LastName,
        sb.MiddleName,
        sb.Suffix,
        CASE 
            WHEN sb.Type = 'Individual' THEN 1 
            WHEN sb.Type = 'Organization' THEN 2 
            ELSE 1 
        END,  -- Type (int)
        CASE 
            WHEN sb.Status = 'Active' THEN 1
            WHEN sb.Status = 'Inactive' THEN 2
            WHEN sb.Status = 'Terminated' THEN 3
            ELSE 1
        END,  -- Status (int)
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
        ISNULL(sb.DownlineCount, 0),
        sb.AddressLine1,
        sb.AddressLine2,
        sb.City,
        sb.State,
        sb.ZipCode,
        sb.Country,
        sb.PrimaryContactName,
        sb.PrimaryContactRole,
        GETUTCDATE(),  -- CreationTime
        0,  -- IsDeleted
        0,  -- EarnedCommissionLast3Months
        0   -- IsAssigneeOnly
    FROM [etl].[stg_brokers] sb
    WHERE sb.IsDeleted = 0
      AND NOT EXISTS (
        SELECT 1 FROM [dbo].[Brokers] pb WHERE pb.Id = sb.Id
      );

    SET IDENTITY_INSERT [dbo].[Brokers] OFF;
    PRINT '  ✓ Inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' new brokers';

    COMMIT TRANSACTION;
    PRINT '  ✓ Broker sync complete';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    PRINT '  ❌ Error syncing brokers: ' + ERROR_MESSAGE();
    THROW;
END CATCH

-- =============================================================================
-- STEP 3: Sync BrokerLicenses
-- =============================================================================

PRINT '';
PRINT 'STEP 3: Syncing BrokerLicenses...';

BEGIN TRY
    BEGIN TRANSACTION;

    -- 3a. Soft delete licenses not in staging
    UPDATE pl
    SET pl.IsDeleted = 1,
        pl.DeletionTime = GETUTCDATE(),
        pl.LastModificationTime = GETUTCDATE()
    FROM [dbo].[BrokerLicenses] pl
    WHERE pl.IsDeleted = 0
      AND NOT EXISTS (
        SELECT 1 FROM [etl].[stg_broker_licenses] sl 
        WHERE sl.Id = pl.Id AND sl.IsDeleted = 0
      );
    PRINT '  ✓ Soft deleted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' licenses';

    -- 3b. Restore previously deleted licenses
    UPDATE pl
    SET pl.IsDeleted = 0,
        pl.DeletionTime = NULL,
        pl.DeleterUserId = NULL,
        pl.LastModificationTime = GETUTCDATE()
    FROM [dbo].[BrokerLicenses] pl
    INNER JOIN [etl].[stg_broker_licenses] sl ON sl.Id = pl.Id
    WHERE pl.IsDeleted = 1
      AND sl.IsDeleted = 0;
    PRINT '  ✓ Restored ' + CAST(@@ROWCOUNT AS VARCHAR) + ' licenses';

    -- 3c. Update existing licenses
    UPDATE pl
    SET 
        pl.BrokerId = sl.BrokerId,
        pl.State = sl.State,
        pl.LicenseNumber = sl.LicenseNumber,
        pl.Type = sl.Type,
        pl.Status = sl.Status,
        pl.EffectiveDate = sl.EffectiveDate,
        pl.ExpirationDate = sl.ExpirationDate,
        pl.LicenseCode = sl.LicenseCode,
        pl.IsResidentLicense = sl.IsResidentLicense,
        pl.ApplicableCounty = sl.ApplicableCounty,
        pl.LastModificationTime = GETUTCDATE()
    FROM [dbo].[BrokerLicenses] pl
    INNER JOIN [etl].[stg_broker_licenses] sl ON sl.Id = pl.Id
    WHERE sl.IsDeleted = 0;
    PRINT '  ✓ Updated ' + CAST(@@ROWCOUNT AS VARCHAR) + ' licenses';

    -- 3d. Insert new licenses
    SET IDENTITY_INSERT [dbo].[BrokerLicenses] ON;

    INSERT INTO [dbo].[BrokerLicenses] (
        Id, BrokerId, State, LicenseNumber, Type, Status,
        EffectiveDate, ExpirationDate, LicenseCode,
        IsResidentLicense, ApplicableCounty,
        CreationTime, IsDeleted
    )
    SELECT 
        sl.Id,
        sl.BrokerId,
        sl.State,
        sl.LicenseNumber,
        sl.Type,
        sl.Status,
        sl.EffectiveDate,
        sl.ExpirationDate,
        sl.LicenseCode,
        sl.IsResidentLicense,
        sl.ApplicableCounty,
        GETUTCDATE(),
        0
    FROM [etl].[stg_broker_licenses] sl
    WHERE sl.IsDeleted = 0
      AND NOT EXISTS (
        SELECT 1 FROM [dbo].[BrokerLicenses] pl WHERE pl.Id = sl.Id
      );

    SET IDENTITY_INSERT [dbo].[BrokerLicenses] OFF;
    PRINT '  ✓ Inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' new licenses';

    COMMIT TRANSACTION;
    PRINT '  ✓ License sync complete';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    PRINT '  ❌ Error syncing licenses: ' + ERROR_MESSAGE();
    THROW;
END CATCH

-- =============================================================================
-- STEP 4: Sync BrokerEOInsurances
-- =============================================================================

PRINT '';
PRINT 'STEP 4: Syncing BrokerEOInsurances...';

BEGIN TRY
    BEGIN TRANSACTION;

    -- 4a. Soft delete E&O not in staging
    UPDATE pe
    SET pe.IsDeleted = 1,
        pe.DeletionTime = GETUTCDATE(),
        pe.LastModificationTime = GETUTCDATE()
    FROM [dbo].[BrokerEOInsurances] pe
    WHERE pe.IsDeleted = 0
      AND NOT EXISTS (
        SELECT 1 FROM [etl].[stg_broker_eo_insurances] se 
        WHERE se.Id = pe.Id AND se.IsDeleted = 0
      );
    PRINT '  ✓ Soft deleted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' E&O records';

    -- 4b. Restore previously deleted E&O
    UPDATE pe
    SET pe.IsDeleted = 0,
        pe.DeletionTime = NULL,
        pe.DeleterUserId = NULL,
        pe.LastModificationTime = GETUTCDATE()
    FROM [dbo].[BrokerEOInsurances] pe
    INNER JOIN [etl].[stg_broker_eo_insurances] se ON se.Id = pe.Id
    WHERE pe.IsDeleted = 1
      AND se.IsDeleted = 0;
    PRINT '  ✓ Restored ' + CAST(@@ROWCOUNT AS VARCHAR) + ' E&O records';

    -- 4c. Update existing E&O
    UPDATE pe
    SET 
        pe.BrokerId = se.BrokerId,
        pe.PolicyNumber = se.PolicyNumber,
        pe.Carrier = se.Carrier,
        pe.CoverageAmount = se.CoverageAmount,
        pe.MinimumRequired = se.MinimumRequired,
        pe.DeductibleAmount = se.DeductibleAmount,
        pe.EffectiveDate = se.EffectiveDate,
        pe.ExpirationDate = se.ExpirationDate,
        pe.RenewalDate = se.RenewalDate,
        pe.Status = se.Status,
        pe.LastModificationTime = GETUTCDATE()
    FROM [dbo].[BrokerEOInsurances] pe
    INNER JOIN [etl].[stg_broker_eo_insurances] se ON se.Id = pe.Id
    WHERE se.IsDeleted = 0;
    PRINT '  ✓ Updated ' + CAST(@@ROWCOUNT AS VARCHAR) + ' E&O records';

    -- 4d. Insert new E&O
    SET IDENTITY_INSERT [dbo].[BrokerEOInsurances] ON;

    INSERT INTO [dbo].[BrokerEOInsurances] (
        Id, BrokerId, PolicyNumber, Carrier,
        CoverageAmount, MinimumRequired, DeductibleAmount,
        EffectiveDate, ExpirationDate, RenewalDate, Status,
        CreationTime, IsDeleted
    )
    SELECT 
        se.Id,
        se.BrokerId,
        se.PolicyNumber,
        se.Carrier,
        se.CoverageAmount,
        se.MinimumRequired,
        se.DeductibleAmount,
        se.EffectiveDate,
        se.ExpirationDate,
        se.RenewalDate,
        se.Status,
        GETUTCDATE(),
        0
    FROM [etl].[stg_broker_eo_insurances] se
    WHERE se.IsDeleted = 0
      AND NOT EXISTS (
        SELECT 1 FROM [dbo].[BrokerEOInsurances] pe WHERE pe.Id = se.Id
      );

    SET IDENTITY_INSERT [dbo].[BrokerEOInsurances] OFF;
    PRINT '  ✓ Inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' new E&O records';

    COMMIT TRANSACTION;
    PRINT '  ✓ E&O sync complete';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    PRINT '  ❌ Error syncing E&O: ' + ERROR_MESSAGE();
    THROW;
END CATCH

-- =============================================================================
-- STEP 5: Create/Update BrokerAppointments from Licenses
-- =============================================================================

PRINT '';
PRINT 'STEP 5: Creating BrokerAppointments from Licenses...';

BEGIN TRY
    BEGIN TRANSACTION;

    -- Create appointments for each active license that doesn't have one
    INSERT INTO [dbo].[BrokerAppointments] (
        BrokerId, StateCode, StateName, LicenseCode, LicenseCodeLabel,
        EffectiveDate, ExpirationDate, GracePeriodDate,
        OriginalEffectiveDate, Status, IsCommissionEligible,
        CreationTime, IsDeleted
    )
    SELECT DISTINCT
        l.BrokerId,
        l.State AS StateCode,
        CASE l.State
            WHEN 'AL' THEN 'Alabama' WHEN 'AK' THEN 'Alaska' WHEN 'AZ' THEN 'Arizona'
            WHEN 'AR' THEN 'Arkansas' WHEN 'CA' THEN 'California' WHEN 'CO' THEN 'Colorado'
            WHEN 'CT' THEN 'Connecticut' WHEN 'DE' THEN 'Delaware' WHEN 'FL' THEN 'Florida'
            WHEN 'GA' THEN 'Georgia' WHEN 'HI' THEN 'Hawaii' WHEN 'ID' THEN 'Idaho'
            WHEN 'IL' THEN 'Illinois' WHEN 'IN' THEN 'Indiana' WHEN 'IA' THEN 'Iowa'
            WHEN 'KS' THEN 'Kansas' WHEN 'KY' THEN 'Kentucky' WHEN 'LA' THEN 'Louisiana'
            WHEN 'ME' THEN 'Maine' WHEN 'MD' THEN 'Maryland' WHEN 'MA' THEN 'Massachusetts'
            WHEN 'MI' THEN 'Michigan' WHEN 'MN' THEN 'Minnesota' WHEN 'MS' THEN 'Mississippi'
            WHEN 'MO' THEN 'Missouri' WHEN 'MT' THEN 'Montana' WHEN 'NE' THEN 'Nebraska'
            WHEN 'NV' THEN 'Nevada' WHEN 'NH' THEN 'New Hampshire' WHEN 'NJ' THEN 'New Jersey'
            WHEN 'NM' THEN 'New Mexico' WHEN 'NY' THEN 'New York' WHEN 'NC' THEN 'North Carolina'
            WHEN 'ND' THEN 'North Dakota' WHEN 'OH' THEN 'Ohio' WHEN 'OK' THEN 'Oklahoma'
            WHEN 'OR' THEN 'Oregon' WHEN 'PA' THEN 'Pennsylvania' WHEN 'RI' THEN 'Rhode Island'
            WHEN 'SC' THEN 'South Carolina' WHEN 'SD' THEN 'South Dakota' WHEN 'TN' THEN 'Tennessee'
            WHEN 'TX' THEN 'Texas' WHEN 'UT' THEN 'Utah' WHEN 'VT' THEN 'Vermont'
            WHEN 'VA' THEN 'Virginia' WHEN 'WA' THEN 'Washington' WHEN 'WV' THEN 'West Virginia'
            WHEN 'WI' THEN 'Wisconsin' WHEN 'WY' THEN 'Wyoming' WHEN 'DC' THEN 'District of Columbia'
            ELSE l.State
        END AS StateName,
        l.Type AS LicenseCode,
        'License' AS LicenseCodeLabel,
        l.EffectiveDate,
        l.ExpirationDate,
        DATEADD(DAY, 30, l.ExpirationDate) AS GracePeriodDate,  -- 30 day grace period
        l.EffectiveDate AS OriginalEffectiveDate,
        1 AS Status,  -- Active
        1 AS IsCommissionEligible,
        GETUTCDATE(),
        0
    FROM [dbo].[BrokerLicenses] l
    WHERE l.IsDeleted = 0
      AND l.Status = 1  -- Active licenses only
      AND NOT EXISTS (
        SELECT 1 FROM [dbo].[BrokerAppointments] a
        WHERE a.BrokerId = l.BrokerId
          AND a.StateCode = l.State
          AND a.IsDeleted = 0
      );

    PRINT '  ✓ Created ' + CAST(@@ROWCOUNT AS VARCHAR) + ' new appointments from licenses';

    -- Update existing appointments with latest license data
    UPDATE a
    SET 
        a.ExpirationDate = l.ExpirationDate,
        a.GracePeriodDate = DATEADD(DAY, 30, l.ExpirationDate),
        a.Status = CASE WHEN l.Status = 1 THEN 1 ELSE 2 END,
        a.LastModificationTime = GETUTCDATE()
    FROM [dbo].[BrokerAppointments] a
    INNER JOIN (
        SELECT BrokerId, State, MAX(ExpirationDate) AS ExpirationDate, MAX(Status) AS Status
        FROM [dbo].[BrokerLicenses]
        WHERE IsDeleted = 0
        GROUP BY BrokerId, State
    ) l ON l.BrokerId = a.BrokerId AND l.State = a.StateCode
    WHERE a.IsDeleted = 0;

    PRINT '  ✓ Updated ' + CAST(@@ROWCOUNT AS VARCHAR) + ' existing appointments';

    COMMIT TRANSACTION;
    PRINT '  ✓ Appointments sync complete';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    PRINT '  ❌ Error syncing appointments: ' + ERROR_MESSAGE();
    THROW;
END CATCH

-- =============================================================================
-- STEP 6: Update EmployerGroups Broker References
-- =============================================================================

PRINT '';
PRINT 'STEP 6: Updating EmployerGroups broker references...';

BEGIN TRY
    BEGIN TRANSACTION;

    -- Update BrokerOfRecordId to ensure it references valid brokers
    UPDATE eg
    SET eg.LastModificationTime = GETUTCDATE()
    FROM [dbo].[EmployerGroups] eg
    WHERE eg.BrokerOfRecordId IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM [dbo].[Brokers] b 
        WHERE b.Id = eg.BrokerOfRecordId AND b.IsDeleted = 0
      );
    PRINT '  ✓ Validated ' + CAST(@@ROWCOUNT AS VARCHAR) + ' EmployerGroups broker references';

    -- Clear invalid broker references
    UPDATE eg
    SET eg.BrokerOfRecordId = NULL,
        eg.LastModificationTime = GETUTCDATE()
    FROM [dbo].[EmployerGroups] eg
    WHERE eg.BrokerOfRecordId IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM [dbo].[Brokers] b 
        WHERE b.Id = eg.BrokerOfRecordId AND b.IsDeleted = 0
      );
    PRINT '  ✓ Cleared ' + CAST(@@ROWCOUNT AS VARCHAR) + ' invalid broker references';

    COMMIT TRANSACTION;
    PRINT '  ✓ EmployerGroups sync complete';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    PRINT '  ❌ Error syncing EmployerGroups: ' + ERROR_MESSAGE();
    THROW;
END CATCH

-- =============================================================================
-- SUMMARY
-- =============================================================================

PRINT '';
PRINT '======================================================================';
PRINT 'SYNC COMPLETE';
PRINT '======================================================================';
PRINT '';
PRINT 'Backup tables created with timestamp: ' + @BackupTimestamp;
PRINT '';
PRINT 'To ROLLBACK, use:';
PRINT '  -- Restore Brokers';
PRINT '  TRUNCATE TABLE [dbo].[Brokers];';
PRINT '  INSERT INTO [dbo].[Brokers] SELECT * FROM [backup].[Brokers_' + @BackupTimestamp + '];';
PRINT '';
PRINT '  -- Similar pattern for other tables...';
PRINT '';

GO
