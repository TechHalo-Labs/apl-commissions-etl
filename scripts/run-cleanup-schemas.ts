/**
 * Run Schema Cleanup Script
 * =========================
 * 
 * Executes the cleanup-schemas.sql script to remove unused schemas.
 * 
 * Usage:
 *   npx tsx scripts/run-cleanup-schemas.ts           # Dry run (default)
 *   npx tsx scripts/run-cleanup-schemas.ts --execute # Actually execute cleanup
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const args = process.argv.slice(2);
  const executeMode = args.includes('--execute');

  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));

  try {
    console.log('\n' + '═'.repeat(70));
    console.log('  Schema Cleanup Script');
    console.log('═'.repeat(70));
    
    if (executeMode) {
      console.log('\n⚠️  EXECUTE MODE - Changes WILL be made!\n');
    } else {
      console.log('\n🔍 DRY RUN MODE - No changes will be made');
      console.log('   Use --execute flag to actually perform cleanup\n');
    }

    // Read the SQL script
    const scriptPath = path.join(__dirname, '..', 'sql', 'utils', 'cleanup-schemas.sql');
    let scriptContent = fs.readFileSync(scriptPath, 'utf-8');

    // Modify @DryRun based on command line flag
    if (executeMode) {
      scriptContent = scriptContent.replace(
        'DECLARE @DryRun BIT = 1;',
        'DECLARE @DryRun BIT = 0;'
      );
    }

    // Split by GO and execute each batch
    const batches = scriptContent
      .split(/^\s*GO\s*$/gm)
      .map(batch => batch.trim())
      .filter(batch => batch.length > 0);

    console.log(`Executing ${batches.length} SQL batch(es)...\n`);
    console.log('─'.repeat(70));

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      
      try {
        const request = pool.request();
        
        // Capture PRINT output
        request.on('info', (info) => {
          if (info.message) {
            console.log(info.message);
          }
        });

        await request.query(batch);
      } catch (error: any) {
        // Some "errors" are just info messages, check if it's a real error
        if (error.message && !error.message.includes('info')) {
          console.error(`\n❌ Error in batch ${i + 1}: ${error.message}`);
        }
      }
    }

    console.log('─'.repeat(70));
    console.log('\n✅ Script execution completed!\n');

    if (!executeMode) {
      console.log('To actually execute the cleanup, run:');
      console.log('  npx tsx scripts/run-cleanup-schemas.ts --execute\n');
    }

  } catch (error) {
    console.error('\n❌ Fatal error:', error);
    process.exit(1);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
