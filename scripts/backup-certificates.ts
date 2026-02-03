/**
 * Backup Certificates Data to Backup Schema
 * =========================================
 * 
 * Backs up poc_etl.raw_certificate_info to backup.certificates-20260201
 * 
 * Usage:
 *   npx tsx scripts/backup-certificates.ts
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

const SOURCE_SCHEMA = 'poc_etl';
const SOURCE_TABLE = 'raw_certificate_info';
const BACKUP_SCHEMA = 'backup';
const BACKUP_TABLE = 'certificates-20260201';

async function main() {
  console.log('\n' + '═'.repeat(70));
  console.log('  Backup Certificates Data');
  console.log('═'.repeat(70));
  console.log(`Source: [${SOURCE_SCHEMA}].[${SOURCE_TABLE}]`);
  console.log(`Target: [${BACKUP_SCHEMA}].[${BACKUP_TABLE}]`);
  console.log('');

  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));

  try {
    // Step 1: Create backup schema if it doesn't exist
    console.log('Step 1: Creating backup schema...');
    await pool.request().query(`
      IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '${BACKUP_SCHEMA}')
      BEGIN
        EXEC('CREATE SCHEMA [${BACKUP_SCHEMA}]');
        PRINT 'Schema [${BACKUP_SCHEMA}] created';
      END
      ELSE
        PRINT 'Schema [${BACKUP_SCHEMA}] already exists';
    `);
    console.log(`✅ Schema [${BACKUP_SCHEMA}] ready`);

    // Step 2: Check source table exists and get row count
    console.log('\nStep 2: Checking source data...');
    const sourceCheck = await pool.request().query(`
      SELECT COUNT(*) as cnt FROM [${SOURCE_SCHEMA}].[${SOURCE_TABLE}]
    `);
    const sourceCount = sourceCheck.recordset[0].cnt;
    
    if (sourceCount === 0) {
      console.log('⚠️  Source table is empty. Nothing to backup.');
      return;
    }
    
    console.log(`✅ Found ${sourceCount.toLocaleString()} rows in source table`);

    // Step 3: Drop backup table if it exists
    console.log('\nStep 3: Preparing backup table...');
    await pool.request().query(`
      IF OBJECT_ID('[${BACKUP_SCHEMA}].[${BACKUP_TABLE}]', 'U') IS NOT NULL
        DROP TABLE [${BACKUP_SCHEMA}].[${BACKUP_TABLE}];
    `);
    console.log(`✅ Backup table prepared`);

    // Step 4: Copy table structure and data using SELECT INTO
    console.log('\nStep 4: Copying data (this may take a while)...');
    const startTime = Date.now();
    
    await pool.request().query(`
      SELECT * INTO [${BACKUP_SCHEMA}].[${BACKUP_TABLE}]
      FROM [${SOURCE_SCHEMA}].[${SOURCE_TABLE}];
    `);
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`✅ Data copied in ${duration}s`);

    // Step 5: Verify backup
    console.log('\nStep 5: Verifying backup...');
    const backupCheck = await pool.request().query(`
      SELECT COUNT(*) as cnt FROM [${BACKUP_SCHEMA}].[${BACKUP_TABLE}]
    `);
    const backupCount = backupCheck.recordset[0].cnt;

    if (sourceCount === backupCount) {
      console.log(`✅ Backup verified: ${backupCount.toLocaleString()} rows (match)`);
    } else {
      console.log(`⚠️  MISMATCH: Source=${sourceCount.toLocaleString()}, Backup=${backupCount.toLocaleString()}`);
    }

    // Step 6: Get table info
    console.log('\nStep 6: Table information...');
    const tableInfo = await pool.request().query(`
      SELECT 
        COLUMN_NAME,
        DATA_TYPE,
        CHARACTER_MAXIMUM_LENGTH
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = '${BACKUP_SCHEMA}' 
        AND TABLE_NAME = '${BACKUP_TABLE}'
      ORDER BY ORDINAL_POSITION
    `);
    
    console.log(`✅ Backup table has ${tableInfo.recordset.length} columns`);

    // Summary
    console.log('\n' + '═'.repeat(70));
    console.log('  BACKUP SUMMARY');
    console.log('═'.repeat(70));
    console.log(`  Source: [${SOURCE_SCHEMA}].[${SOURCE_TABLE}]`);
    console.log(`  Backup: [${BACKUP_SCHEMA}].[${BACKUP_TABLE}]`);
    console.log(`  Rows: ${backupCount.toLocaleString()}`);
    console.log(`  Duration: ${duration}s`);
    console.log('═'.repeat(70) + '\n');

    console.log('✅ Backup completed successfully!\n');

  } catch (error) {
    console.error('\n❌ Fatal error:', error);
    process.exit(1);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
