/**
 * Populate input tables from raw data
 * This must be run AFTER raw data is ingested
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';

const config: sql.config = {
  server: 'halo-sql.database.windows.net',
  database: 'halo-sqldb',
  user: '***REMOVED***',
  password: '***REMOVED***',
  options: { encrypt: true, trustServerCertificate: true },
  requestTimeout: 600000, // 10 minutes for large data
};

const SQL_DIR = path.join(__dirname, '../sql');

function log(message: string): void {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

async function executeSqlFile(pool: sql.ConnectionPool, filePath: string): Promise<void> {
  const fullPath = path.join(SQL_DIR, filePath);
  const sqlContent = fs.readFileSync(fullPath, 'utf-8');
  
  const batches = sqlContent
    .split(/^\s*GO\s*$/gim)
    .filter(batch => batch.trim().length > 0);
  
  log(`Executing ${filePath} (${batches.length} batches)...`);
  
  for (let i = 0; i < batches.length; i++) {
    try {
      await pool.request().batch(batches[i]);
      if ((i + 1) % 2 === 0) {
        log(`  Batch ${i + 1}/${batches.length} complete`);
      }
    } catch (err: any) {
      log(`❌ Error in batch ${i + 1}: ${err.message}`);
      // Continue with other batches
    }
  }
  
  log(`✅ ${filePath} completed`);
}

async function main() {
  log('');
  log('============================================================');
  log('Populating Input Tables from Raw Data');
  log('============================================================');
  
  const pool = await sql.connect(config);
  log('✅ Connected to SQL Server');
  
  try {
    // Run 02-input-tables.sql which creates prep tables and populates from raw
    await executeSqlFile(pool, '02-input-tables.sql');
    
    // Check results
    log('');
    log('============================================================');
    log('Results');
    log('============================================================');
    
    const tables = [
      'prep_certificate_info',
      'prep_commission_details',
      'input_certificate_info',
      'input_commission_details',
    ];
    
    for (const table of tables) {
      const result = await pool.request().query(`SELECT COUNT(*) as cnt FROM [etl].[${table}]`);
      const count = result.recordset[0].cnt;
      const status = count > 0 ? '✅' : '⚠️';
      log(`${status} ${table}: ${count.toLocaleString()} rows`);
    }
    
  } finally {
    await pool.close();
    log('');
    log('Connection closed');
  }
}

main().catch(err => {
  console.error('Failed:', err);
  process.exit(1);
});

