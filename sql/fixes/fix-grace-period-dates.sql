-- =============================================
-- FIX GRACE PERIOD DATE ISSUES - ALL TABLES
-- Bug #36: Correct far-future expiration dates
-- 
-- This script fixes all 3 tables:
-- 1. BrokerLicenses
-- 2. BrokerAppointments
-- 3. BrokerEOInsurances
-- 
-- BEFORE RUNNING:
-- 1. Review STEP 0 ANALYZE queries to see affected counts
-- 2. This script will create backups and fix all tables
-- 3. Run in TRANSACTION (will rollback on error)
-- =============================================

-- =============================================
-- STEP 0: ANALYZE - Check All Tables
-- =============================================
-- Run these queries FIRST to see what will be fixed

SELECT 
    'BrokerLicenses' as TableName,
    COUNT(*) as AffectedCount,
    MIN(ExpirationDate) as MinBadDate,
    MAX(ExpirationDate) as MaxBadDate
FROM [dbo].[BrokerLicenses]
WHERE ExpirationDate > '2050-01-01';

SELECT 
    'BrokerAppointments' as TableName,
    COUNT(*) as AffectedCount,
    MIN(ExpirationDate) as MinBadDate,
    MAX(ExpirationDate) as MaxBadDate
FROM [dbo].[BrokerAppointments]
WHERE ExpirationDate IS NOT NULL 
  AND ExpirationDate > '2050-01-01';

SELECT 
    'BrokerEOInsurances' as TableName,
    COUNT(*) as AffectedCount,
    MIN(ExpirationDate) as MinBadDate,
    MAX(ExpirationDate) as MaxBadDate
FROM [dbo].[BrokerEOInsurances]
WHERE ExpirationDate > '2050-01-01';

-- =============================================
-- STEP 1: CREATE BACKUPS
-- =============================================

-- Backup BrokerLicenses
IF OBJECT_ID('dbo.BrokerLicenses_Backup_GracePeriodFix', 'U') IS NOT NULL
    DROP TABLE [dbo].[BrokerLicenses_Backup_GracePeriodFix];

SELECT * 
INTO [dbo].[BrokerLicenses_Backup_GracePeriodFix]
FROM [dbo].[BrokerLicenses]
WHERE ExpirationDate > '2050-01-01';

-- Backup BrokerAppointments
IF OBJECT_ID('dbo.BrokerAppointments_Backup_GracePeriodFix', 'U') IS NOT NULL
    DROP TABLE [dbo].[BrokerAppointments_Backup_GracePeriodFix];

SELECT * 
INTO [dbo].[BrokerAppointments_Backup_GracePeriodFix]
FROM [dbo].[BrokerAppointments]
WHERE ExpirationDate IS NOT NULL
  AND ExpirationDate > '2050-01-01';

-- Backup BrokerEOInsurances
IF OBJECT_ID('dbo.BrokerEOInsurances_Backup_GracePeriodFix', 'U') IS NOT NULL
    DROP TABLE [dbo].[BrokerEOInsurances_Backup_GracePeriodFix];

SELECT * 
INTO [dbo].[BrokerEOInsurances_Backup_GracePeriodFix]
FROM [dbo].[BrokerEOInsurances]
WHERE ExpirationDate > '2050-01-01';

-- Verify backups
SELECT 'BrokerLicenses' as BackupTable, COUNT(*) as BackupCount FROM [dbo].[BrokerLicenses_Backup_GracePeriodFix]
UNION ALL
SELECT 'BrokerAppointments', COUNT(*) FROM [dbo].[BrokerAppointments_Backup_GracePeriodFix]
UNION ALL
SELECT 'BrokerEOInsurances', COUNT(*) FROM [dbo].[BrokerEOInsurances_Backup_GracePeriodFix];

-- =============================================
-- STEP 2: FIX ALL TABLES IN TRANSACTION
-- =============================================

BEGIN TRY
    BEGIN TRANSACTION;
    
    DECLARE @TotalRowsUpdated INT = 0;
    DECLARE @RowsUpdated INT = 0;
    
    -- =============================================
    -- FIX BrokerLicenses
    -- =============================================
    
    -- Fix 1: Use GracePeriodDate where available
    UPDATE [dbo].[BrokerLicenses]
    SET ExpirationDate = GracePeriodDate,
        LastModificationTime = GETUTCDATE(),
        LastModifierUserId = 1
    WHERE ExpirationDate > '2050-01-01'
      AND GracePeriodDate IS NOT NULL
      AND GracePeriodDate < '2050-01-01';
    
    SET @RowsUpdated = @@ROWCOUNT;
    SET @TotalRowsUpdated = @TotalRowsUpdated + @RowsUpdated;
    PRINT 'BrokerLicenses: Rows updated with GracePeriodDate: ' + CAST(@RowsUpdated AS VARCHAR);
    
    -- Fix 2: For remaining bad dates, set reasonable default (30 days from effective)
    UPDATE [dbo].[BrokerLicenses]
    SET ExpirationDate = DATEADD(day, 30, EffectiveDate),
        LastModificationTime = GETUTCDATE(),
        LastModifierUserId = 1
    WHERE ExpirationDate > '2050-01-01'
      AND (GracePeriodDate IS NULL OR GracePeriodDate > '2050-01-01');
    
    SET @RowsUpdated = @@ROWCOUNT;
    SET @TotalRowsUpdated = @TotalRowsUpdated + @RowsUpdated;
    PRINT 'BrokerLicenses: Rows updated with default (30 days): ' + CAST(@RowsUpdated AS VARCHAR);
    
    -- =============================================
    -- FIX BrokerAppointments
    -- =============================================
    
    -- Fix 1: Use GracePeriodDate where available
    UPDATE [dbo].[BrokerAppointments]
    SET ExpirationDate = GracePeriodDate,
        LastModificationTime = GETUTCDATE(),
        LastModifierUserId = 1
    WHERE ExpirationDate IS NOT NULL
      AND ExpirationDate > '2050-01-01'
      AND GracePeriodDate IS NOT NULL
      AND GracePeriodDate < '2050-01-01';
    
    SET @RowsUpdated = @@ROWCOUNT;
    SET @TotalRowsUpdated = @TotalRowsUpdated + @RowsUpdated;
    PRINT 'BrokerAppointments: Rows updated with GracePeriodDate: ' + CAST(@RowsUpdated AS VARCHAR);
    
    -- Fix 2: For remaining bad dates, set reasonable default (30 days from effective)
    UPDATE [dbo].[BrokerAppointments]
    SET ExpirationDate = DATEADD(day, 30, EffectiveDate),
        LastModificationTime = GETUTCDATE(),
        LastModifierUserId = 1
    WHERE ExpirationDate IS NOT NULL
      AND ExpirationDate > '2050-01-01'
      AND (GracePeriodDate IS NULL OR GracePeriodDate > '2050-01-01');
    
    SET @RowsUpdated = @@ROWCOUNT;
    SET @TotalRowsUpdated = @TotalRowsUpdated + @RowsUpdated;
    PRINT 'BrokerAppointments: Rows updated with default (30 days): ' + CAST(@RowsUpdated AS VARCHAR);
    
    -- =============================================
    -- FIX BrokerEOInsurances
    -- =============================================
    
    -- Fix 1: Use GracePeriodDate where available
    UPDATE [dbo].[BrokerEOInsurances]
    SET ExpirationDate = GracePeriodDate,
        LastModificationTime = GETUTCDATE(),
        LastModifierUserId = 1
    WHERE ExpirationDate > '2050-01-01'
      AND GracePeriodDate IS NOT NULL
      AND GracePeriodDate < '2050-01-01';
    
    SET @RowsUpdated = @@ROWCOUNT;
    SET @TotalRowsUpdated = @TotalRowsUpdated + @RowsUpdated;
    PRINT 'BrokerEOInsurances: Rows updated with GracePeriodDate: ' + CAST(@RowsUpdated AS VARCHAR);
    
    -- Fix 2: For remaining bad dates, set reasonable default (30 days from effective)
    UPDATE [dbo].[BrokerEOInsurances]
    SET ExpirationDate = DATEADD(day, 30, EffectiveDate),
        LastModificationTime = GETUTCDATE(),
        LastModifierUserId = 1
    WHERE ExpirationDate > '2050-01-01'
      AND (GracePeriodDate IS NULL OR GracePeriodDate > '2050-01-01');
    
    SET @RowsUpdated = @@ROWCOUNT;
    SET @TotalRowsUpdated = @TotalRowsUpdated + @RowsUpdated;
    PRINT 'BrokerEOInsurances: Rows updated with default (30 days): ' + CAST(@RowsUpdated AS VARCHAR);
    
    PRINT 'Total rows updated across all tables: ' + CAST(@TotalRowsUpdated AS VARCHAR);
    
    -- =============================================
    -- STEP 3: VERIFY (Review before commit)
    -- =============================================
    
    -- Check remaining bad dates (should be 0 for all tables)
    SELECT 'BrokerLicenses' as TableName, COUNT(*) as RemainingBadDates
    FROM [dbo].[BrokerLicenses]
    WHERE ExpirationDate > '2050-01-01'
    UNION ALL
    SELECT 'BrokerAppointments', COUNT(*)
    FROM [dbo].[BrokerAppointments]
    WHERE ExpirationDate IS NOT NULL
      AND ExpirationDate > '2050-01-01'
    UNION ALL
    SELECT 'BrokerEOInsurances', COUNT(*)
    FROM [dbo].[BrokerEOInsurances]
    WHERE ExpirationDate > '2050-01-01';
    
    -- If everything looks correct:
    COMMIT TRANSACTION;
    PRINT 'Transaction committed successfully.';
    
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    
    PRINT 'ERROR: ' + ERROR_MESSAGE();
    PRINT 'Transaction rolled back. Backup tables preserved.';
    THROW;
END CATCH

-- =============================================
-- STEP 4: FINAL SUMMARY
-- =============================================

SELECT 
    'BrokerLicenses' as TableName,
    (SELECT COUNT(*) FROM [dbo].[BrokerLicenses_Backup_GracePeriodFix]) as RecordsFixed,
    (SELECT COUNT(*) FROM [dbo].[BrokerLicenses] WHERE ExpirationDate > '2050-01-01') as RemainingBadDates
UNION ALL
SELECT 
    'BrokerAppointments',
    (SELECT COUNT(*) FROM [dbo].[BrokerAppointments_Backup_GracePeriodFix]),
    (SELECT COUNT(*) FROM [dbo].[BrokerAppointments] WHERE ExpirationDate IS NOT NULL AND ExpirationDate > '2050-01-01')
UNION ALL
SELECT 
    'BrokerEOInsurances',
    (SELECT COUNT(*) FROM [dbo].[BrokerEOInsurances_Backup_GracePeriodFix]),
    (SELECT COUNT(*) FROM [dbo].[BrokerEOInsurances] WHERE ExpirationDate > '2050-01-01');
