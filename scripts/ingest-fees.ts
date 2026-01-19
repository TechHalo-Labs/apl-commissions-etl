/**
 * Ingest Fees CSV into SQL Server
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import { parse } from 'csv-parse';

const config: sql.config = {
  server: 'halo-sql.database.windows.net',
  database: 'halo-sqldb',
  user: '***REMOVED***',
  password: '***REMOVED***',
  options: { encrypt: true, trustServerCertificate: true },
  requestTimeout: 120000
};

const CSV_PATH = '/Users/kennpalm/Downloads/source/APL/apl-commissions-frontend/docs/data-map/rawdata/Fees_20260107.csv';

async function loadCsv(filePath: string): Promise<Record<string, string>[]> {
  return new Promise((resolve, reject) => {
    const records: Record<string, string>[] = [];
    fs.createReadStream(filePath)
      .pipe(parse({ columns: true, skip_empty_lines: true, relax_column_count: true }))
      .on('data', (record) => records.push(record))
      .on('end', () => resolve(records))
      .on('error', reject);
  });
}

async function main() {
  const pool = await sql.connect(config);
  console.log('Connected to SQL Server');
  
  console.log('\nLoading Fees_20260107.csv...');
  const records = await loadCsv(CSV_PATH);
  console.log(`Loaded ${records.length} records from CSV`);
  
  if (records.length === 0) {
    console.log('No records to insert');
    await pool.close();
    return;
  }
  
  const columns = Object.keys(records[0]);
  console.log('Columns:', columns.join(', '));
  
  const batchSize = 500;
  let inserted = 0;
  
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    
    const values = batch.map(record => {
      const vals = columns.map(col => {
        const val = record[col];
        if (val === null || val === undefined || val === '') return 'NULL';
        return `'${String(val).replace(/'/g, "''")}'`;
      });
      return `(${vals.join(', ')})`;
    }).join(',\n');
    
    const insertSql = `
      INSERT INTO [etl].[raw_fees] (${columns.map(c => `[${c}]`).join(', ')})
      VALUES ${values}
    `;
    
    await pool.request().batch(insertSql);
    inserted += batch.length;
    
    if (i % 1000 === 0 && i > 0) {
      console.log(`  ${inserted.toLocaleString()} rows inserted...`);
    }
  }
  
  console.log(`✅ Complete: ${inserted.toLocaleString()} rows inserted`);
  
  // Verify
  const count = await pool.request().query('SELECT COUNT(*) as cnt FROM [etl].[raw_fees]');
  console.log(`\nVerified: ${count.recordset[0].cnt.toLocaleString()} rows in etl.raw_fees`);
  
  await pool.close();
}

main().catch(console.error);

