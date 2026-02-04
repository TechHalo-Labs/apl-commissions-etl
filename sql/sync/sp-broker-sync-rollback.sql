-- =============================================================================
-- ROLLBACK PROCEDURE: Restore broker data from backup
-- =============================================================================
--
-- Usage: EXEC sp_broker_sync_rollback @BackupTimestamp = '20260201_123456'
--
-- =============================================================================

CREATE OR ALTER PROCEDURE sp_broker_sync_rollback
    @BackupTimestamp VARCHAR(20),
    @DryRun BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    PRINT '======================================================================';
    PRINT 'BROKER DATA ROLLBACK';
    PRINT 'Backup Timestamp: ' + @BackupTimestamp;
    PRINT 'Dry Run: ' + CASE WHEN @DryRun = 1 THEN 'YES' ELSE 'NO' END;
    PRINT '======================================================================';
    PRINT '';
    
    -- Validate backup tables exist
    DECLARE @BrokerBackup NVARCHAR(200) = 'backup.Brokers_' + @BackupTimestamp;
    DECLARE @LicenseBackup NVARCHAR(200) = 'backup.BrokerLicenses_' + @BackupTimestamp;
    DECLARE @EOBackup NVARCHAR(200) = 'backup.BrokerEOInsurances_' + @BackupTimestamp;
    DECLARE @ApptBackup NVARCHAR(200) = 'backup.BrokerAppointments_' + @BackupTimestamp;
    DECLARE @EGBackup NVARCHAR(200) = 'backup.EmployerGroups_' + @BackupTimestamp;
    
    IF NOT EXISTS (SELECT 1 FROM sys.tables t 
                   JOIN sys.schemas s ON t.schema_id = s.schema_id 
                   WHERE s.name = 'backup' AND t.name = 'Brokers_' + @BackupTimestamp)
    BEGIN
        RAISERROR('Backup table %s does not exist', 16, 1, @BrokerBackup);
        RETURN;
    END
    
    PRINT 'Backup tables found:';
    PRINT '  - ' + @BrokerBackup;
    PRINT '  - ' + @LicenseBackup;
    PRINT '  - ' + @EOBackup;
    PRINT '  - ' + @ApptBackup;
    PRINT '  - ' + @EGBackup;
    PRINT '';
    
    IF @DryRun = 1
    BEGIN
        PRINT '[DRY RUN] Would restore all tables from backup';
        PRINT 'Set @DryRun = 0 to execute rollback';
        RETURN;
    END
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Restore in reverse FK order (children first)
        
        -- 1. Clear and restore BrokerAppointments
        PRINT 'Restoring BrokerAppointments...';
        DELETE FROM [dbo].[BrokerAppointments];
        EXEC('INSERT INTO [dbo].[BrokerAppointments] SELECT * FROM ' + @ApptBackup);
        PRINT '  ✓ Restored ' + CAST(@@ROWCOUNT AS VARCHAR) + ' appointments';
        
        -- 2. Clear and restore BrokerEOInsurances
        PRINT 'Restoring BrokerEOInsurances...';
        DELETE FROM [dbo].[BrokerEOInsurances];
        EXEC('INSERT INTO [dbo].[BrokerEOInsurances] SELECT * FROM ' + @EOBackup);
        PRINT '  ✓ Restored ' + CAST(@@ROWCOUNT AS VARCHAR) + ' E&O records';
        
        -- 3. Clear and restore BrokerLicenses
        PRINT 'Restoring BrokerLicenses...';
        DELETE FROM [dbo].[BrokerLicenses];
        EXEC('SET IDENTITY_INSERT [dbo].[BrokerLicenses] ON; INSERT INTO [dbo].[BrokerLicenses] SELECT * FROM ' + @LicenseBackup + '; SET IDENTITY_INSERT [dbo].[BrokerLicenses] OFF');
        PRINT '  ✓ Restored licenses';
        
        -- 4. Clear and restore Brokers
        PRINT 'Restoring Brokers...';
        DELETE FROM [dbo].[Brokers];
        EXEC('SET IDENTITY_INSERT [dbo].[Brokers] ON; INSERT INTO [dbo].[Brokers] SELECT * FROM ' + @BrokerBackup + '; SET IDENTITY_INSERT [dbo].[Brokers] OFF');
        PRINT '  ✓ Restored brokers';
        
        -- 5. Clear and restore EmployerGroups
        PRINT 'Restoring EmployerGroups...';
        DELETE FROM [dbo].[EmployerGroups];
        EXEC('INSERT INTO [dbo].[EmployerGroups] SELECT * FROM ' + @EGBackup);
        PRINT '  ✓ Restored ' + CAST(@@ROWCOUNT AS VARCHAR) + ' employer groups';
        
        COMMIT TRANSACTION;
        
        PRINT '';
        PRINT '======================================================================';
        PRINT 'ROLLBACK COMPLETE';
        PRINT '======================================================================';
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT '❌ Rollback failed: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END
GO
