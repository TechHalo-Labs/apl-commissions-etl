/**
 * Load missing raw tables (perf_groups, premiums, commissions_detail)
 */
import sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import { parse } from 'csv-parse';

const config: sql.config = { 
  server: 'halo-sql.database.windows.net', 
  database: 'halo-sqldb', 
  user: '***REMOVED***', 
  password: '***REMOVED***', 
  options: { encrypt: true, trustServerCertificate: true },
  requestTimeout: 600000
};

const csvDataPath = '/Users/kennpalm/Downloads/source/APL/apl-commissions-frontend/docs/data-map/rawdata';

function log(msg: string) { 
  console.log(`[${new Date().toISOString()}] ${msg}`); 
}

async function loadCsv(pool: sql.ConnectionPool, tableName: string, csvFile: string, truncate = true): Promise<number> {
  const filePath = path.join(csvDataPath, csvFile);
  if (!fs.existsSync(filePath)) {
    log(`File not found: ${filePath}`);
    return 0;
  }
  
  const fileSize = (fs.statSync(filePath).size / 1024 / 1024).toFixed(2);
  log(`Loading ${csvFile} (${fileSize} MB) into ${tableName}...`);
  
  // Read all records
  const records: any[] = [];
  await new Promise<void>((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(parse({ columns: true, skip_empty_lines: true, relax_column_count: true }))
      .on('data', (record) => records.push(record))
      .on('end', resolve)
      .on('error', reject);
  });
  
  if (records.length === 0) return 0;
  
  const columns = Object.keys(records[0]);
  // Clean column names: remove BOM, special chars, prefix with Col_ if starts with number
  const cleanCols = columns.map((c, i) => {
    let clean = c.replace(/^\uFEFF/, '').replace(/[^a-zA-Z0-9_]/g, '_').trim();
    if (!clean || /^[0-9]/.test(clean)) clean = `Col_${i}`;
    return clean;
  });
  
  // Drop and recreate table if truncate is true
  if (truncate) {
    const dropSql = `IF OBJECT_ID('[etl].[${tableName}]', 'U') IS NOT NULL DROP TABLE [etl].[${tableName}]`;
    await pool.request().query(dropSql);
  }
  
  // Create table if needed
  const createTableSql = `
    IF OBJECT_ID('[etl].[${tableName}]', 'U') IS NULL
    CREATE TABLE [etl].[${tableName}] (${cleanCols.map(c => `[${c}] NVARCHAR(MAX)`).join(', ')});
  `;
  await pool.request().query(createTableSql);
  
  // Insert in batches
  const batchSize = 1000;
  let inserted = 0;
  
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    const values = batch.map(record => {
      const vals = columns.map(col => {
        const val = record[col];
        if (val === null || val === undefined || val === '') return 'NULL';
        return `N'${String(val).replace(/'/g, "''")}'`;
      });
      return `(${vals.join(', ')})`;
    }).join(',\n');
    
    const insertSql = `INSERT INTO [etl].[${tableName}] (${cleanCols.map(c => `[${c}]`).join(', ')}) VALUES ${values}`;
    await pool.request().batch(insertSql);
    inserted += batch.length;
    
    if (inserted % 50000 === 0) log(`  ... ${inserted.toLocaleString()} rows`);
  }
  
  log(`✅ ${inserted.toLocaleString()} rows loaded into ${tableName}`);
  return inserted;
}

async function run() {
  const pool = await sql.connect(config);
  log('Connected to SQL Server');
  
  // Load perf-group
  await loadCsv(pool, 'raw_perf_groups', 'perf-group.csv');
  
  // Load premiums
  await loadCsv(pool, 'raw_premiums', 'premiums.csv');
  
  // Load commissions detail files
  const files = fs.readdirSync(csvDataPath)
    .filter(f => f.startsWith('CommissionsDetail') && f.endsWith('.csv') && !f.includes('-old'))
    .sort();
  
  log(`Found ${files.length} CommissionsDetail files: ${files.join(', ')}`);
  
  if (files.length > 0) {
    // Load first file (creates and truncates table)
    await loadCsv(pool, 'raw_commissions_detail', files[0], true);
    
    // Append remaining files
    for (let i = 1; i < files.length; i++) {
      await loadCsv(pool, 'raw_commissions_detail', files[i], false);
    }
  }
  
  // Final counts
  log('\n=== FINAL COUNTS ===');
  for (const tbl of ['raw_perf_groups', 'raw_premiums', 'raw_commissions_detail']) {
    const result = await pool.request().query(`SELECT COUNT(*) as cnt FROM [etl].[${tbl}]`);
    log(`${tbl}: ${result.recordset[0].cnt.toLocaleString()}`);
  }
  
  await pool.close();
  log('Done!');
}

run().catch(e => { console.error(e); process.exit(1); });

