/**
 * Copy All Data from poc_etl Schema to old_poc Schema
 * ====================================================
 * 
 * Creates the old_poc schema if it doesn't exist and copies all tables
 * (structure + data) from poc_etl to old_poc.
 * 
 * Usage:
 *   npx tsx scripts/copy-poc-etl-to-old-poc.ts
 * 
 * Options:
 *   --dry-run    Preview what would be copied without executing
 *   --skip-data  Copy table structures only (no data)
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

const SOURCE_SCHEMA = 'poc_etl';
const TARGET_SCHEMA = 'old_poc';

interface TableInfo {
  tableName: string;
  rowCount: number;
  hasIdentity: boolean;
}

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const skipData = args.includes('--skip-data');

  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));

  try {
    console.log('\n' + '═'.repeat(70));
    console.log('  Copy Schema: [poc_etl] → [old_poc]');
    console.log('═'.repeat(70));
    
    if (dryRun) {
      console.log('\n🔍 DRY RUN MODE - No changes will be made\n');
    }
    if (skipData) {
      console.log('\n📋 STRUCTURE ONLY MODE - No data will be copied\n');
    }

    // Step 1: Create target schema if it doesn't exist
    console.log('\nStep 1: Creating target schema...');
    if (!dryRun) {
      await pool.request().query(`
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '${TARGET_SCHEMA}')
        BEGIN
          EXEC('CREATE SCHEMA [${TARGET_SCHEMA}]');
          PRINT 'Schema [${TARGET_SCHEMA}] created';
        END
        ELSE
          PRINT 'Schema [${TARGET_SCHEMA}] already exists';
      `);
      console.log(`✅ Schema [${TARGET_SCHEMA}] ready`);
    } else {
      console.log(`  Would create schema [${TARGET_SCHEMA}]`);
    }

    // Step 2: Discover all tables in source schema
    console.log('\nStep 2: Discovering tables in source schema...');
    const tablesResult = await pool.request().query(`
      SELECT TABLE_NAME
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = '${SOURCE_SCHEMA}'
        AND TABLE_TYPE = 'BASE TABLE'
      ORDER BY TABLE_NAME
    `);

    const tables: TableInfo[] = [];
    
    for (const row of tablesResult.recordset) {
      const tableName = row.TABLE_NAME;
      
      // Check row count
      const countResult = await pool.request().query(`
        SELECT COUNT(*) as cnt FROM [${SOURCE_SCHEMA}].[${tableName}]
      `);
      const rowCount = countResult.recordset[0].cnt;

      // Check if table has identity column
      const identityResult = await pool.request().query(`
        SELECT COUNT(*) as cnt
        FROM sys.identity_columns ic
        INNER JOIN sys.tables t ON ic.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '${SOURCE_SCHEMA}' AND OBJECT_NAME(ic.object_id) = '${tableName}'
      `);
      const hasIdentity = identityResult.recordset[0].cnt > 0;

      tables.push({ tableName, rowCount, hasIdentity });
    }

    console.log(`✅ Found ${tables.length} tables in [${SOURCE_SCHEMA}]`);
    console.log(`   Total rows: ${tables.reduce((sum, t) => sum + t.rowCount, 0).toLocaleString()}`);

    if (tables.length === 0) {
      console.log('\n⚠️  No tables found in source schema. Exiting.');
      return;
    }

    // Step 3: Copy each table
    console.log('\nStep 3: Copying tables...\n');
    
    let successCount = 0;
    let failedCount = 0;
    let totalRowsCopied = 0;
    const startTime = Date.now();

    for (const table of tables) {
      const { tableName, rowCount, hasIdentity } = table;
      
      try {
        if (dryRun) {
          console.log(`  [DRY RUN] Would copy [${tableName}]: ${rowCount.toLocaleString()} rows${hasIdentity ? ' (has identity)' : ''}`);
          continue;
        }

        // Drop target table if it exists
        await pool.request().query(`
          IF OBJECT_ID('[${TARGET_SCHEMA}].[${tableName}]', 'U') IS NOT NULL
            DROP TABLE [${TARGET_SCHEMA}].[${tableName}];
        `);

        if (skipData) {
          // Copy structure only
          await pool.request().query(`
            SELECT * INTO [${TARGET_SCHEMA}].[${tableName}]
            FROM [${SOURCE_SCHEMA}].[${tableName}]
            WHERE 1 = 0
          `);
          console.log(`  ✅ [${tableName}]: Structure copied (${rowCount.toLocaleString()} rows in source, not copied)`);
        } else {
          // Copy structure and data
          if (hasIdentity) {
            // For tables with identity columns, use SELECT INTO then enable identity insert
            await pool.request().query(`
              SELECT * INTO [${TARGET_SCHEMA}].[${tableName}]
              FROM [${SOURCE_SCHEMA}].[${tableName}]
            `);
            
            // Verify the copy worked
            const verifyResult = await pool.request().query(`
              SELECT COUNT(*) as cnt FROM [${TARGET_SCHEMA}].[${tableName}]
            `);
            const copiedCount = verifyResult.recordset[0].cnt;
            
            console.log(`  ✅ [${tableName}]: ${copiedCount.toLocaleString()} rows copied${hasIdentity ? ' (identity preserved)' : ''}`);
            totalRowsCopied += copiedCount;
          } else {
            // For tables without identity, use SELECT INTO directly
            await pool.request().query(`
              SELECT * INTO [${TARGET_SCHEMA}].[${tableName}]
              FROM [${SOURCE_SCHEMA}].[${tableName}]
            `);
            
            // Verify the copy worked
            const verifyResult = await pool.request().query(`
              SELECT COUNT(*) as cnt FROM [${TARGET_SCHEMA}].[${tableName}]
            `);
            const copiedCount = verifyResult.recordset[0].cnt;
            
            console.log(`  ✅ [${tableName}]: ${copiedCount.toLocaleString()} rows copied`);
            totalRowsCopied += copiedCount;
          }
        }
        
        successCount++;
      } catch (error: any) {
        console.error(`  ❌ [${tableName}]: Failed - ${error.message}`);
        failedCount++;
      }
    }

    // Step 4: Summary
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    
    console.log('\n' + '═'.repeat(70));
    console.log('  COPY SUMMARY');
    console.log('═'.repeat(70));
    console.log(`  Tables processed: ${tables.length}`);
    console.log(`  ✅ Successful: ${successCount}`);
    console.log(`  ❌ Failed: ${failedCount}`);
    if (!skipData) {
      console.log(`  📊 Total rows copied: ${totalRowsCopied.toLocaleString()}`);
    }
    console.log(`  ⏱️  Duration: ${duration}s`);
    console.log('═'.repeat(70) + '\n');

    // Step 5: Verification
    if (!dryRun && !skipData) {
      console.log('Step 4: Verification...\n');
      
      let verifiedCount = 0;
      let mismatchCount = 0;

      for (const table of tables) {
        try {
          const sourceCountResult = await pool.request().query(`
            SELECT COUNT(*) as cnt FROM [${SOURCE_SCHEMA}].[${table.tableName}]
          `);
          const targetCountResult = await pool.request().query(`
            SELECT COUNT(*) as cnt FROM [${TARGET_SCHEMA}].[${table.tableName}]
          `);

          const sourceCount = sourceCountResult.recordset[0].cnt;
          const targetCount = targetCountResult.recordset[0].cnt;

          if (sourceCount === targetCount) {
            console.log(`  ✅ [${table.tableName}]: ${sourceCount.toLocaleString()} rows (match)`);
            verifiedCount++;
          } else {
            console.log(`  ⚠️  [${table.tableName}]: Source=${sourceCount.toLocaleString()}, Target=${targetCount.toLocaleString()} (MISMATCH)`);
            mismatchCount++;
          }
        } catch (error: any) {
          console.error(`  ❌ [${table.tableName}]: Verification failed - ${error.message}`);
        }
      }

      console.log(`\n  Verification: ${verifiedCount} matched, ${mismatchCount} mismatched`);
    }

    console.log('\n✅ Copy operation completed!\n');

  } catch (error) {
    console.error('\n❌ Fatal error:', error);
    process.exit(1);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
