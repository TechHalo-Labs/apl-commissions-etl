/**
 * Create raw_* views in new_data schema
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './scripts/lib/config-loader';
import * as fs from 'fs';
import * as path from 'path';

async function createViews() {
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  
  console.log('\n═══════════════════════════════════════════════════════════');
  console.log('CREATE RAW VIEWS IN new_data SCHEMA');
  console.log('═══════════════════════════════════════════════════════════\n');

  try {
    const pool = await sql.connect(sqlConfig);
    
    const scriptPath = path.join(__dirname, 'sql/fix/create-new-data-raw-tables.sql');
    const sqlScript = fs.readFileSync(scriptPath, 'utf-8');
    
    // Split by GO statements
    const batches = sqlScript.split(/\bGO\b/i).filter(b => b.trim());
    
    console.log(`Executing ${batches.length} SQL batches...`);
    
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i].trim();
      if (batch) {
        try {
          const result = await pool.request().query(batch);
          if (result.recordset && result.recordset.length > 0) {
            console.table(result.recordset);
          }
        } catch (err: any) {
          if (!err.message.includes('Cannot use empty object')) {
            console.log(`Batch ${i + 1}: ${err.message}`);
          }
        }
      }
    }

    console.log('\n✅ All views created successfully\n');

    await pool.close();
  } catch (error: any) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

createViews();
