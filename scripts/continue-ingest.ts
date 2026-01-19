/**
 * Continue CSV Ingestion - Only ingest tables that are empty or incomplete
 * Handles large files with better connection management
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import { parse } from 'csv-parse';

const config: sql.config = {
  server: process.env.SQLSERVER_HOST || 'halo-sql.database.windows.net',
  database: process.env.SQLSERVER_DATABASE || 'halo-sqldb',
  user: process.env.SQLSERVER_USER || '***REMOVED***',
  password: process.env.SQLSERVER_PASSWORD || '***REMOVED***',
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
  requestTimeout: 120000, // 2 minutes per request
  connectionTimeout: 30000,
  pool: {
    max: 5,
    min: 0,
    idleTimeoutMillis: 60000,
  },
};

const CSV_DATA_PATH = process.env.CSV_DATA_PATH || 
  '/Users/kennpalm/Downloads/source/APL/apl-commissions-frontend/docs/data-map/rawdata';

// CSV file mapping - tableName: { file(s), expectedMinRows }
const csvFiles: Record<string, { files: string[], minRows: number }> = {
  'raw_premiums': { 
    files: ['premiums.csv'], 
    minRows: 138000 
  },
  'raw_commissions_detail': { 
    files: [
      'CommissionsDetail_20251101_20251115.csv',
      'CommissionsDetail_20251116_20251130.csv', 
      'CommissionsDetail_20251201_20251215.csv',
      'CommissionsDetail_20251216_20251231.csv',
    ], 
    minRows: 100000 
  },
  'raw_certificate_info': { 
    files: ['CertificateInfo.csv'], 
    minRows: 90000 
  },
  'raw_individual_brokers': { 
    files: ['IndividualRosterExtract_20260107.csv'], 
    minRows: 1000 
  },
  'raw_org_brokers': { 
    files: ['OrganizationRosterExtract_20260107.csv'], 
    minRows: 100 
  },
  'raw_licenses': { 
    files: ['BrokerLicenseExtract_20260107.csv'], 
    minRows: 1000 
  },
  'raw_eo_insurance': { 
    files: ['BrokerEO_20260107.csv'], 
    minRows: 500 
  },
  'raw_schedule_rates': { 
    files: ['perf.csv'], 
    minRows: 100000 
  },
  'raw_perf_groups': { 
    files: ['perf-group.csv'], 
    minRows: 1000 
  },
};

function log(message: string, level: 'info' | 'error' | 'success' = 'info'): void {
  const timestamp = new Date().toISOString();
  const prefix = { info: '  ', error: '❌', success: '✅' }[level];
  console.log(`[${timestamp}] ${prefix} ${message}`);
}

async function getRowCount(pool: sql.ConnectionPool, tableName: string): Promise<number> {
  const result = await pool.request().query(`SELECT COUNT(*) as cnt FROM [etl].[${tableName}]`);
  return result.recordset[0].cnt;
}

async function loadCsvFile(filePath: string): Promise<Record<string, string>[]> {
  return new Promise((resolve, reject) => {
    const records: Record<string, string>[] = [];
    fs.createReadStream(filePath)
      .pipe(parse({ columns: true, skip_empty_lines: true, relax_column_count: true }))
      .on('data', (record) => records.push(record))
      .on('end', () => resolve(records))
      .on('error', reject);
  });
}

async function insertBatch(
  pool: sql.ConnectionPool, 
  tableName: string, 
  records: Record<string, string>[],
  columns: string[]
): Promise<void> {
  const values = records.map(record => {
    const vals = columns.map(col => {
      const val = record[col];
      if (val === null || val === undefined || val === '') {
        return 'NULL';
      }
      return `'${String(val).replace(/'/g, "''")}'`;
    });
    return `(${vals.join(', ')})`;
  }).join(',\n');

  const insertSql = `
    INSERT INTO [etl].[${tableName}] (${columns.map(c => `[${c}]`).join(', ')})
    VALUES ${values}
  `;
  
  await pool.request().batch(insertSql);
}

async function ingestTable(
  pool: sql.ConnectionPool,
  tableName: string,
  fileConfig: { files: string[], minRows: number }
): Promise<void> {
  const currentCount = await getRowCount(pool, tableName);
  
  if (currentCount >= fileConfig.minRows) {
    log(`${tableName}: Already has ${currentCount.toLocaleString()} rows (min: ${fileConfig.minRows.toLocaleString()}), skipping`);
    return;
  }
  
  log(`${tableName}: Has ${currentCount.toLocaleString()} rows, needs ${fileConfig.minRows.toLocaleString()}, loading...`);
  
  // Truncate if incomplete
  if (currentCount > 0) {
    log(`  Truncating ${tableName} (had incomplete data)...`);
    await pool.request().query(`TRUNCATE TABLE [etl].[${tableName}]`);
  }
  
  let totalInserted = 0;
  
  for (const fileName of fileConfig.files) {
    const filePath = path.join(CSV_DATA_PATH, fileName);
    
    if (!fs.existsSync(filePath)) {
      log(`  File not found: ${fileName}, skipping`, 'error');
      continue;
    }
    
    log(`  Loading ${fileName}...`);
    const records = await loadCsvFile(filePath);
    
    if (records.length === 0) {
      log(`  ${fileName} is empty, skipping`);
      continue;
    }
    
    const columns = Object.keys(records[0]);
    const batchSize = 500; // Smaller batches for stability
    
    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      
      try {
        await insertBatch(pool, tableName, batch, columns);
        totalInserted += batch.length;
        
        if ((i + batchSize) % 10000 === 0 || i + batchSize >= records.length) {
          log(`    ${totalInserted.toLocaleString()} rows inserted...`);
        }
      } catch (err: any) {
        log(`  Error at row ${i}: ${err.message}`, 'error');
        throw err;
      }
    }
  }
  
  log(`${tableName}: Complete with ${totalInserted.toLocaleString()} rows`, 'success');
}

async function main() {
  log('');
  log('============================================================');
  log('SQL Server ETL - Continue Ingestion');
  log('============================================================');
  log(`CSV Path: ${CSV_DATA_PATH}`);
  log('');
  
  const pool = await sql.connect(config);
  log('Connected to SQL Server', 'success');
  
  try {
    for (const [tableName, fileConfig] of Object.entries(csvFiles)) {
      await ingestTable(pool, tableName, fileConfig);
    }
    
    log('');
    log('============================================================');
    log('Ingestion Complete - Final Counts');
    log('============================================================');
    
    for (const tableName of Object.keys(csvFiles)) {
      const count = await getRowCount(pool, tableName);
      log(`${tableName}: ${count.toLocaleString()} rows`);
    }
    
  } finally {
    await pool.close();
    log('Connection closed');
  }
}

main().catch(err => {
  console.error('Pipeline failed:', err);
  process.exit(1);
});

