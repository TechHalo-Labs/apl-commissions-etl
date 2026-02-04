/**
 * Broker Data Sync: Staging (ETL) → Production (DBO)
 * 
 * This script synchronizes broker-related data from staging to production:
 * - Brokers (soft delete, update, insert, restore)
 * - BrokerLicenses
 * - BrokerEOInsurances
 * - BrokerAppointments (created from licenses)
 * - EmployerGroups (broker references)
 * 
 * Features:
 * - Full backup before any changes
 * - Dry-run mode for preview
 * - Rollback capability
 * 
 * Usage:
 *   npx tsx scripts/sync-broker-data.ts                    # Dry run (preview)
 *   npx tsx scripts/sync-broker-data.ts --apply            # Apply changes
 *   npx tsx scripts/sync-broker-data.ts --rollback <timestamp>  # Rollback
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';

// =============================================================================
// Configuration
// =============================================================================

interface SyncConfig {
  server: string;
  database: string;
  user: string;
  password: string;
  options: {
    encrypt: boolean;
    trustServerCertificate: boolean;
  };
}

function parseConnectionString(connStr: string): Record<string, string> {
  return connStr.split(';').reduce((acc, part) => {
    const [key, value] = part.split('=');
    if (key && value) acc[key.toLowerCase().trim()] = value.trim();
    return acc;
  }, {} as Record<string, string>);
}

function getConfig(): SyncConfig {
  const connStr = process.env.SQLSERVER;
  if (!connStr) {
    throw new Error('SQLSERVER environment variable not set');
  }
  
  const parts = parseConnectionString(connStr);
  
  return {
    server: parts['server'] || parts['data source'] || '',
    database: parts['database'] || parts['initial catalog'] || '',
    user: parts['user id'] || parts['uid'] || '',
    password: parts['password'] || parts['pwd'] || '',
    options: {
      encrypt: true,
      trustServerCertificate: true
    }
  };
}

// =============================================================================
// Main Sync Logic
// =============================================================================

interface SyncStats {
  brokers: {
    softDeleted: number;
    restored: number;
    updated: number;
    inserted: number;
  };
  licenses: {
    softDeleted: number;
    restored: number;
    updated: number;
    inserted: number;
  };
  eo: {
    softDeleted: number;
    restored: number;
    updated: number;
    inserted: number;
  };
  appointments: {
    created: number;
    updated: number;
  };
}

async function analyzeChanges(pool: sql.ConnectionPool): Promise<{
  brokers: { staging: number; prod: number; toDelete: number; toUpdate: number; toInsert: number; toRestore: number };
  licenses: { staging: number; prod: number; toDelete: number; toInsert: number };
  eo: { staging: number; prod: number; toDelete: number; toInsert: number };
}> {
  // Brokers
  const brokersStaging = (await pool.request().query(`SELECT COUNT(*) AS cnt FROM [etl].[stg_brokers] WHERE IsDeleted = 0`)).recordset[0].cnt;
  const brokersProd = (await pool.request().query(`SELECT COUNT(*) AS cnt FROM [dbo].[Brokers] WHERE IsDeleted = 0`)).recordset[0].cnt;
  
  const brokersToDelete = (await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM [dbo].[Brokers] pb
    WHERE pb.IsDeleted = 0 AND NOT EXISTS (
      SELECT 1 FROM [etl].[stg_brokers] sb WHERE sb.Id = pb.Id AND sb.IsDeleted = 0
    )
  `)).recordset[0].cnt;
  
  const brokersToInsert = (await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM [etl].[stg_brokers] sb
    WHERE sb.IsDeleted = 0 AND NOT EXISTS (
      SELECT 1 FROM [dbo].[Brokers] pb WHERE pb.Id = sb.Id
    )
  `)).recordset[0].cnt;
  
  const brokersToRestore = (await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM [dbo].[Brokers] pb
    INNER JOIN [etl].[stg_brokers] sb ON sb.Id = pb.Id
    WHERE pb.IsDeleted = 1 AND sb.IsDeleted = 0
  `)).recordset[0].cnt;
  
  const brokersToUpdate = (await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM [dbo].[Brokers] pb
    INNER JOIN [etl].[stg_brokers] sb ON sb.Id = pb.Id
    WHERE sb.IsDeleted = 0
  `)).recordset[0].cnt;

  // Licenses
  const licensesStaging = (await pool.request().query(`SELECT COUNT(*) AS cnt FROM [etl].[stg_broker_licenses] WHERE IsDeleted = 0`)).recordset[0].cnt;
  const licensesProd = (await pool.request().query(`SELECT COUNT(*) AS cnt FROM [dbo].[BrokerLicenses] WHERE IsDeleted = 0`)).recordset[0].cnt;
  
  const licensesToDelete = (await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM [dbo].[BrokerLicenses] pl
    WHERE pl.IsDeleted = 0 AND NOT EXISTS (
      SELECT 1 FROM [etl].[stg_broker_licenses] sl WHERE sl.Id = pl.Id AND sl.IsDeleted = 0
    )
  `)).recordset[0].cnt;
  
  const licensesToInsert = (await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM [etl].[stg_broker_licenses] sl
    WHERE sl.IsDeleted = 0 AND NOT EXISTS (
      SELECT 1 FROM [dbo].[BrokerLicenses] pl WHERE pl.Id = sl.Id
    )
  `)).recordset[0].cnt;

  // E&O
  const eoStaging = (await pool.request().query(`SELECT COUNT(*) AS cnt FROM [etl].[stg_broker_eo_insurances] WHERE IsDeleted = 0`)).recordset[0].cnt;
  const eoProd = (await pool.request().query(`SELECT COUNT(*) AS cnt FROM [dbo].[BrokerEOInsurances] WHERE IsDeleted = 0`)).recordset[0].cnt;
  
  const eoToDelete = (await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM [dbo].[BrokerEOInsurances] pe
    WHERE pe.IsDeleted = 0 AND NOT EXISTS (
      SELECT 1 FROM [etl].[stg_broker_eo_insurances] se WHERE se.Id = pe.Id AND se.IsDeleted = 0
    )
  `)).recordset[0].cnt;
  
  const eoToInsert = (await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM [etl].[stg_broker_eo_insurances] se
    WHERE se.IsDeleted = 0 AND NOT EXISTS (
      SELECT 1 FROM [dbo].[BrokerEOInsurances] pe WHERE pe.Id = se.Id
    )
  `)).recordset[0].cnt;

  return {
    brokers: {
      staging: brokersStaging,
      prod: brokersProd,
      toDelete: brokersToDelete,
      toUpdate: brokersToUpdate,
      toInsert: brokersToInsert,
      toRestore: brokersToRestore
    },
    licenses: {
      staging: licensesStaging,
      prod: licensesProd,
      toDelete: licensesToDelete,
      toInsert: licensesToInsert
    },
    eo: {
      staging: eoStaging,
      prod: eoProd,
      toDelete: eoToDelete,
      toInsert: eoToInsert
    }
  };
}

async function createBackups(pool: sql.ConnectionPool, timestamp: string): Promise<void> {
  console.log('Creating backup tables...');
  
  // Create backup schema if needed
  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'backup')
      EXEC('CREATE SCHEMA [backup]')
  `);
  
  const tables = [
    { src: 'dbo.Brokers', dest: `Brokers_${timestamp}` },
    { src: 'dbo.BrokerLicenses', dest: `BrokerLicenses_${timestamp}` },
    { src: 'dbo.BrokerEOInsurances', dest: `BrokerEOInsurances_${timestamp}` },
    { src: 'dbo.BrokerAppointments', dest: `BrokerAppointments_${timestamp}` },
    { src: 'dbo.EmployerGroups', dest: `EmployerGroups_${timestamp}` },
  ];
  
  for (const t of tables) {
    // Drop if exists, then create
    await pool.request().query(`
      IF OBJECT_ID('backup.${t.dest}', 'U') IS NOT NULL
        DROP TABLE [backup].[${t.dest}]
    `);
    await pool.request().query(`SELECT * INTO [backup].[${t.dest}] FROM [${t.src.replace('.', '].[')}]`);
    console.log(`  ✓ backup.${t.dest}`);
  }
}

async function syncBrokers(pool: sql.ConnectionPool): Promise<{ softDeleted: number; restored: number; updated: number; inserted: number }> {
  console.log('\nSyncing Brokers...');
  const stats = { softDeleted: 0, restored: 0, updated: 0, inserted: 0 };
  
  // Soft delete
  let result = await pool.request().query(`
    UPDATE pb
    SET pb.IsDeleted = 1, pb.DeletionTime = GETUTCDATE(), pb.LastModificationTime = GETUTCDATE()
    FROM [dbo].[Brokers] pb
    WHERE pb.IsDeleted = 0
      AND NOT EXISTS (SELECT 1 FROM [etl].[stg_brokers] sb WHERE sb.Id = pb.Id AND sb.IsDeleted = 0)
  `);
  stats.softDeleted = result.rowsAffected[0];
  console.log(`  ✓ Soft deleted: ${stats.softDeleted}`);
  
  // Restore
  result = await pool.request().query(`
    UPDATE pb
    SET pb.IsDeleted = 0, pb.DeletionTime = NULL, pb.DeleterUserId = NULL, pb.LastModificationTime = GETUTCDATE()
    FROM [dbo].[Brokers] pb
    INNER JOIN [etl].[stg_brokers] sb ON sb.Id = pb.Id
    WHERE pb.IsDeleted = 1 AND sb.IsDeleted = 0
  `);
  stats.restored = result.rowsAffected[0];
  console.log(`  ✓ Restored: ${stats.restored}`);
  
  // Update existing
  result = await pool.request().query(`
    UPDATE pb
    SET 
      pb.ExternalPartyId = sb.ExternalPartyId,
      pb.Name = CASE WHEN sb.Name IS NULL OR LTRIM(RTRIM(sb.Name)) = '' 
                THEN LTRIM(RTRIM(COALESCE(sb.FirstName, '') + ' ' + COALESCE(sb.LastName, '')))
                ELSE sb.Name END,
      pb.FirstName = sb.FirstName, pb.LastName = sb.LastName, pb.MiddleName = sb.MiddleName,
      pb.Suffix = sb.Suffix, pb.Email = sb.Email, pb.Phone = sb.Phone, pb.Npn = sb.Npn,
      pb.TaxId = sb.TaxId, pb.Ssn = sb.Ssn, pb.DateOfBirth = sb.DateOfBirth,
      pb.AppointmentDate = sb.AppointmentDate, pb.HireDate = sb.HireDate,
      pb.DateContracted = sb.DateContracted, pb.BrokerClassification = sb.BrokerClassification,
      pb.HierarchyLevel = sb.HierarchyLevel, pb.UplineId = sb.UplineId, pb.UplineName = sb.UplineName,
      pb.DownlineCount = ISNULL(sb.DownlineCount, 0),
      pb.AddressLine1 = sb.AddressLine1, pb.AddressLine2 = sb.AddressLine2,
      pb.City = sb.City, pb.State = sb.State, pb.ZipCode = sb.ZipCode, pb.Country = sb.Country,
      pb.PrimaryContactName = sb.PrimaryContactName, pb.PrimaryContactRole = sb.PrimaryContactRole,
      pb.LastModificationTime = GETUTCDATE()
    FROM [dbo].[Brokers] pb
    INNER JOIN [etl].[stg_brokers] sb ON sb.Id = pb.Id
    WHERE sb.IsDeleted = 0
  `);
  stats.updated = result.rowsAffected[0];
  console.log(`  ✓ Updated: ${stats.updated}`);
  
  // Insert new (combine SET IDENTITY_INSERT with INSERT in single batch)
  result = await pool.request().query(`
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
      sb.Id, sb.ExternalPartyId,
      CASE WHEN sb.Name IS NULL OR LTRIM(RTRIM(sb.Name)) = '' 
           THEN LTRIM(RTRIM(COALESCE(sb.FirstName, '') + ' ' + COALESCE(sb.LastName, '')))
           ELSE sb.Name END,
      sb.FirstName, sb.LastName, sb.MiddleName, sb.Suffix,
      CASE WHEN sb.Type = 'Individual' THEN 1 WHEN sb.Type = 'Organization' THEN 2 ELSE 1 END,
      CASE WHEN sb.Status = 'Active' THEN 1 WHEN sb.Status = 'Inactive' THEN 2 ELSE 1 END,
      sb.Email, sb.Phone, sb.Npn, sb.TaxId, sb.Ssn,
      sb.DateOfBirth, sb.AppointmentDate, sb.HireDate, sb.DateContracted,
      sb.BrokerClassification, sb.HierarchyLevel, sb.UplineId, sb.UplineName, ISNULL(sb.DownlineCount, 0),
      sb.AddressLine1, sb.AddressLine2, sb.City, sb.State, sb.ZipCode, sb.Country,
      sb.PrimaryContactName, sb.PrimaryContactRole,
      GETUTCDATE(), 0, 0, 0
    FROM [etl].[stg_brokers] sb
    WHERE sb.IsDeleted = 0
      AND NOT EXISTS (SELECT 1 FROM [dbo].[Brokers] pb WHERE pb.Id = sb.Id);
    SET IDENTITY_INSERT [dbo].[Brokers] OFF;
  `);
  stats.inserted = result.rowsAffected[0] || 0;
  console.log(`  ✓ Inserted: ${stats.inserted}`);
  
  return stats;
}

async function syncLicenses(pool: sql.ConnectionPool): Promise<{ softDeleted: number; restored: number; updated: number; inserted: number }> {
  console.log('\nSyncing BrokerLicenses...');
  const stats = { softDeleted: 0, restored: 0, updated: 0, inserted: 0 };
  
  // Soft delete
  let result = await pool.request().query(`
    UPDATE pl SET pl.IsDeleted = 1, pl.DeletionTime = GETUTCDATE(), pl.LastModificationTime = GETUTCDATE()
    FROM [dbo].[BrokerLicenses] pl
    WHERE pl.IsDeleted = 0 AND NOT EXISTS (
      SELECT 1 FROM [etl].[stg_broker_licenses] sl WHERE sl.Id = pl.Id AND sl.IsDeleted = 0
    )
  `);
  stats.softDeleted = result.rowsAffected[0];
  console.log(`  ✓ Soft deleted: ${stats.softDeleted}`);
  
  // Restore
  result = await pool.request().query(`
    UPDATE pl SET pl.IsDeleted = 0, pl.DeletionTime = NULL, pl.LastModificationTime = GETUTCDATE()
    FROM [dbo].[BrokerLicenses] pl
    INNER JOIN [etl].[stg_broker_licenses] sl ON sl.Id = pl.Id
    WHERE pl.IsDeleted = 1 AND sl.IsDeleted = 0
  `);
  stats.restored = result.rowsAffected[0];
  console.log(`  ✓ Restored: ${stats.restored}`);
  
  // Update (handle NULL values for required columns)
  result = await pool.request().query(`
    UPDATE pl SET 
      pl.BrokerId = sl.BrokerId, 
      pl.State = sl.State, 
      pl.LicenseNumber = ISNULL(sl.LicenseNumber, 'PENDING'),
      pl.Type = sl.Type, 
      pl.Status = sl.Status,
      pl.EffectiveDate = ISNULL(sl.EffectiveDate, pl.EffectiveDate),
      pl.ExpirationDate = ISNULL(sl.ExpirationDate, pl.ExpirationDate),
      pl.LicenseCode = sl.LicenseCode, 
      pl.IsResidentLicense = ISNULL(sl.IsResidentLicense, 0),
      pl.ApplicableCounty = sl.ApplicableCounty, 
      pl.LastModificationTime = GETUTCDATE()
    FROM [dbo].[BrokerLicenses] pl
    INNER JOIN [etl].[stg_broker_licenses] sl ON sl.Id = pl.Id
    WHERE sl.IsDeleted = 0
  `);
  stats.updated = result.rowsAffected[0];
  console.log(`  ✓ Updated: ${stats.updated}`);
  
  // Insert (combine SET IDENTITY_INSERT with INSERT in single batch)
  // Handle NULL values for required columns
  result = await pool.request().query(`
    SET IDENTITY_INSERT [dbo].[BrokerLicenses] ON;
    INSERT INTO [dbo].[BrokerLicenses] (
      Id, BrokerId, State, LicenseNumber, Type, Status,
      EffectiveDate, ExpirationDate, LicenseCode, IsResidentLicense, ApplicableCounty,
      CreationTime, IsDeleted
    )
    SELECT sl.Id, sl.BrokerId, sl.State, 
           ISNULL(sl.LicenseNumber, 'PENDING'),  -- Default for NULL
           sl.Type, sl.Status,
           ISNULL(sl.EffectiveDate, GETUTCDATE()),
           ISNULL(sl.ExpirationDate, DATEADD(YEAR, 1, GETUTCDATE())),
           sl.LicenseCode, 
           ISNULL(sl.IsResidentLicense, 0),
           sl.ApplicableCounty,
           GETUTCDATE(), 0
    FROM [etl].[stg_broker_licenses] sl
    WHERE sl.IsDeleted = 0 
      AND sl.BrokerId IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM [dbo].[BrokerLicenses] pl WHERE pl.Id = sl.Id);
    SET IDENTITY_INSERT [dbo].[BrokerLicenses] OFF;
  `);
  stats.inserted = result.rowsAffected[0] || 0;
  console.log(`  ✓ Inserted: ${stats.inserted}`);
  
  return stats;
}

async function syncEO(pool: sql.ConnectionPool): Promise<{ softDeleted: number; restored: number; updated: number; inserted: number }> {
  console.log('\nSyncing BrokerEOInsurances...');
  console.log('  (Using BrokerId + PolicyNumber as match key due to unique index)');
  const stats = { softDeleted: 0, restored: 0, updated: 0, inserted: 0 };
  
  // Soft delete - match on (BrokerId, PolicyNumber)
  let result = await pool.request().query(`
    UPDATE pe SET pe.IsDeleted = 1, pe.DeletionTime = GETUTCDATE(), pe.LastModificationTime = GETUTCDATE()
    FROM [dbo].[BrokerEOInsurances] pe
    WHERE pe.IsDeleted = 0 AND NOT EXISTS (
      SELECT 1 FROM [etl].[stg_broker_eo_insurances] se 
      WHERE se.BrokerId = pe.BrokerId 
        AND ISNULL(se.PolicyNumber, 'PENDING') = pe.PolicyNumber
        AND se.IsDeleted = 0
    )
  `);
  stats.softDeleted = result.rowsAffected[0];
  console.log(`  ✓ Soft deleted: ${stats.softDeleted}`);
  
  // Restore and Update existing - match on (BrokerId, PolicyNumber)
  result = await pool.request().query(`
    UPDATE pe SET 
      pe.IsDeleted = 0,
      pe.DeletionTime = NULL,
      pe.Carrier = ISNULL(se.Carrier, 'Unknown'),
      pe.CoverageAmount = ISNULL(se.CoverageAmount, 0), 
      pe.MinimumRequired = ISNULL(se.MinimumRequired, 0),
      pe.DeductibleAmount = ISNULL(se.DeductibleAmount, 0),
      pe.ClaimMaxAmount = ISNULL(se.ClaimMaxAmount, 0),
      pe.AnnualMaxAmount = ISNULL(se.AnnualMaxAmount, 0),
      pe.PolicyMaxAmount = ISNULL(se.PolicyMaxAmount, 0),
      pe.LiabilityLimit = ISNULL(se.LiabilityLimit, 0),
      pe.EffectiveDate = ISNULL(se.EffectiveDate, pe.EffectiveDate), 
      pe.ExpirationDate = ISNULL(se.ExpirationDate, pe.ExpirationDate),
      pe.RenewalDate = se.RenewalDate, 
      pe.Status = ISNULL(se.Status, 1),
      pe.LastModificationTime = GETUTCDATE()
    FROM [dbo].[BrokerEOInsurances] pe
    INNER JOIN [etl].[stg_broker_eo_insurances] se 
      ON se.BrokerId = pe.BrokerId 
      AND ISNULL(se.PolicyNumber, 'PENDING') = pe.PolicyNumber
    WHERE se.IsDeleted = 0
  `);
  stats.updated = result.rowsAffected[0];
  stats.restored = stats.updated; // Combined restore + update
  console.log(`  ✓ Updated/Restored: ${stats.updated}`);
  
  // Insert only records that don't exist by (BrokerId, PolicyNumber)
  result = await pool.request().query(`
    INSERT INTO [dbo].[BrokerEOInsurances] (
      BrokerId, PolicyNumber, Carrier, 
      CoverageAmount, MinimumRequired, DeductibleAmount,
      ClaimMaxAmount, AnnualMaxAmount, PolicyMaxAmount, LiabilityLimit,
      EffectiveDate, ExpirationDate, RenewalDate, Status, CreationTime, IsDeleted
    )
    SELECT se.BrokerId, 
           ISNULL(se.PolicyNumber, 'PENDING'),
           ISNULL(se.Carrier, 'Unknown'),
           ISNULL(se.CoverageAmount, 0),
           ISNULL(se.MinimumRequired, 0),
           ISNULL(se.DeductibleAmount, 0),
           ISNULL(se.ClaimMaxAmount, 0),
           ISNULL(se.AnnualMaxAmount, 0),
           ISNULL(se.PolicyMaxAmount, 0),
           ISNULL(se.LiabilityLimit, 0),
           ISNULL(se.EffectiveDate, GETUTCDATE()),
           ISNULL(se.ExpirationDate, DATEADD(YEAR, 1, GETUTCDATE())),
           se.RenewalDate, 
           ISNULL(se.Status, 1),
           GETUTCDATE(), 0
    FROM [etl].[stg_broker_eo_insurances] se
    WHERE se.IsDeleted = 0 
      AND se.BrokerId IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM [dbo].[BrokerEOInsurances] pe 
        WHERE pe.BrokerId = se.BrokerId 
          AND pe.PolicyNumber = ISNULL(se.PolicyNumber, 'PENDING')
      )
  `);
  stats.inserted = result.rowsAffected[0] || 0;
  console.log(`  ✓ Inserted: ${stats.inserted}`);
  
  return stats;
}

async function createAppointments(pool: sql.ConnectionPool): Promise<{ created: number; updated: number }> {
  console.log('\nCreating BrokerAppointments from Licenses...');
  const stats = { created: 0, updated: 0 };
  
  // Create appointments for licenses without existing appointments
  let result = await pool.request().query(`
    INSERT INTO [dbo].[BrokerAppointments] (
      BrokerId, StateCode, StateName, LicenseCode, LicenseCodeLabel,
      EffectiveDate, ExpirationDate, GracePeriodDate, OriginalEffectiveDate,
      Status, IsCommissionEligible, CreationTime, IsDeleted
    )
    SELECT DISTINCT
      l.BrokerId, l.State,
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
        ELSE l.State END,
      l.Type, 'License',
      l.EffectiveDate, l.ExpirationDate, DATEADD(DAY, 30, l.ExpirationDate), l.EffectiveDate,
      1, 1, GETUTCDATE(), 0
    FROM [dbo].[BrokerLicenses] l
    WHERE l.IsDeleted = 0 AND l.Status = 1
      AND NOT EXISTS (
        SELECT 1 FROM [dbo].[BrokerAppointments] a
        WHERE a.BrokerId = l.BrokerId AND a.StateCode = l.State AND a.IsDeleted = 0
      )
  `);
  stats.created = result.rowsAffected[0];
  console.log(`  ✓ Created: ${stats.created}`);
  
  // Update existing appointments
  result = await pool.request().query(`
    UPDATE a SET 
      a.ExpirationDate = l.ExpirationDate,
      a.GracePeriodDate = DATEADD(DAY, 30, l.ExpirationDate),
      a.Status = CASE WHEN l.Status = 1 THEN 1 ELSE 2 END,
      a.LastModificationTime = GETUTCDATE()
    FROM [dbo].[BrokerAppointments] a
    INNER JOIN (
      SELECT BrokerId, State, MAX(ExpirationDate) AS ExpirationDate, MAX(Status) AS Status
      FROM [dbo].[BrokerLicenses] WHERE IsDeleted = 0 GROUP BY BrokerId, State
    ) l ON l.BrokerId = a.BrokerId AND l.State = a.StateCode
    WHERE a.IsDeleted = 0
  `);
  stats.updated = result.rowsAffected[0];
  console.log(`  ✓ Updated: ${stats.updated}`);
  
  return stats;
}

async function rollback(pool: sql.ConnectionPool, timestamp: string): Promise<void> {
  console.log(`\nRolling back to backup: ${timestamp}`);
  
  // Verify backup exists
  const check = await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'backup' AND t.name = 'Brokers_${timestamp}'
  `);
  
  if (check.recordset[0].cnt === 0) {
    throw new Error(`Backup tables for ${timestamp} not found`);
  }
  
  // Execute rollback in order
  console.log('  Restoring BrokerAppointments...');
  await pool.request().query(`DELETE FROM [dbo].[BrokerAppointments]`);
  await pool.request().query(`INSERT INTO [dbo].[BrokerAppointments] SELECT * FROM [backup].[BrokerAppointments_${timestamp}]`);
  
  console.log('  Restoring BrokerEOInsurances...');
  await pool.request().query(`DELETE FROM [dbo].[BrokerEOInsurances]`);
  await pool.request().query(`INSERT INTO [dbo].[BrokerEOInsurances] SELECT * FROM [backup].[BrokerEOInsurances_${timestamp}]`);
  
  console.log('  Restoring BrokerLicenses...');
  await pool.request().query(`DELETE FROM [dbo].[BrokerLicenses]`);
  await pool.request().query(`
    SET IDENTITY_INSERT [dbo].[BrokerLicenses] ON;
    INSERT INTO [dbo].[BrokerLicenses] SELECT * FROM [backup].[BrokerLicenses_${timestamp}];
    SET IDENTITY_INSERT [dbo].[BrokerLicenses] OFF;
  `);
  
  console.log('  Restoring Brokers...');
  await pool.request().query(`DELETE FROM [dbo].[Brokers]`);
  await pool.request().query(`
    SET IDENTITY_INSERT [dbo].[Brokers] ON;
    INSERT INTO [dbo].[Brokers] SELECT * FROM [backup].[Brokers_${timestamp}];
    SET IDENTITY_INSERT [dbo].[Brokers] OFF;
  `);
  
  console.log('  Restoring EmployerGroups...');
  await pool.request().query(`DELETE FROM [dbo].[EmployerGroups]`);
  await pool.request().query(`INSERT INTO [dbo].[EmployerGroups] SELECT * FROM [backup].[EmployerGroups_${timestamp}]`);
  
  console.log('  ✓ Rollback complete');
}

// =============================================================================
// Main Entry Point
// =============================================================================

async function main() {
  const args = process.argv.slice(2);
  const applyChanges = args.includes('--apply');
  const rollbackIndex = args.indexOf('--rollback');
  const rollbackTimestamp = rollbackIndex >= 0 ? args[rollbackIndex + 1] : null;
  
  console.log('======================================================================');
  console.log('BROKER DATA SYNC: Staging → Production');
  console.log('======================================================================');
  
  const config = getConfig();
  const pool = await sql.connect(config);
  
  try {
    if (rollbackTimestamp) {
      await rollback(pool, rollbackTimestamp);
      return;
    }
    
    // Analyze changes
    console.log('\nAnalyzing differences...\n');
    const analysis = await analyzeChanges(pool);
    
    console.log('BROKERS:');
    console.log(`  Staging (active):    ${analysis.brokers.staging}`);
    console.log(`  Production (active): ${analysis.brokers.prod}`);
    console.log(`  To soft delete:      ${analysis.brokers.toDelete}`);
    console.log(`  To update:           ${analysis.brokers.toUpdate}`);
    console.log(`  To insert:           ${analysis.brokers.toInsert}`);
    console.log(`  To restore:          ${analysis.brokers.toRestore}`);
    
    console.log('\nLICENSES:');
    console.log(`  Staging (active):    ${analysis.licenses.staging}`);
    console.log(`  Production (active): ${analysis.licenses.prod}`);
    console.log(`  To soft delete:      ${analysis.licenses.toDelete}`);
    console.log(`  To insert:           ${analysis.licenses.toInsert}`);
    
    console.log('\nE&O INSURANCES:');
    console.log(`  Staging (active):    ${analysis.eo.staging}`);
    console.log(`  Production (active): ${analysis.eo.prod}`);
    console.log(`  To soft delete:      ${analysis.eo.toDelete}`);
    console.log(`  To insert:           ${analysis.eo.toInsert}`);
    
    if (!applyChanges) {
      console.log('\n======================================================================');
      console.log('DRY RUN - No changes made');
      console.log('Run with --apply to execute sync');
      console.log('======================================================================');
      return;
    }
    
    // Create backups
    const timestamp = new Date().toISOString().replace(/[-:T]/g, '').slice(0, 15);
    await createBackups(pool, timestamp);
    
    // Execute sync
    await syncBrokers(pool);
    await syncLicenses(pool);
    await syncEO(pool);
    await createAppointments(pool);
    
    console.log('\n======================================================================');
    console.log('SYNC COMPLETE');
    console.log(`Backup timestamp: ${timestamp}`);
    console.log(`To rollback: npx tsx scripts/sync-broker-data.ts --rollback ${timestamp}`);
    console.log('======================================================================');
    
  } finally {
    await pool.close();
  }
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
