/**
 * CSV Loader for SQL Server ETL
 * Dynamically reads CSV columns and creates/loads tables in the etl schema
 * 
 * Usage:
 *   npx tsx scripts/load-csv.ts              # Load all rows
 *   npx tsx scripts/load-csv.ts --limit 100  # Test with 100 rows per file
 */

import sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import { parse } from 'csv-parse';

// Parse CLI args
const args = process.argv.slice(2);
const limitIndex = args.indexOf('--limit');
const ROW_LIMIT = limitIndex !== -1 ? parseInt(args[limitIndex + 1], 10) : 0;

const config: sql.config = {
  server: process.env.SQLSERVER_HOST || 'halo-sql.database.windows.net',
  database: process.env.SQLSERVER_DATABASE || 'halo-sqldb',
  user: process.env.SQLSERVER_USER || '***REMOVED***',
  password: process.env.SQLSERVER_PASSWORD || '***REMOVED***',
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
  requestTimeout: 600000, // 10 minutes for bulk operations
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
};

const csvDataPath = '/Users/kennpalm/Downloads/source/APL/apl-commissions-frontend/docs/data-map/rawdata';

function log(message: string) {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

interface CsvMapping {
  csvFile: string | string[]; // Single file or pattern for multiple files
  tableName: string;
}

const csvMappings: CsvMapping[] = [
  // Updated broker/org files from client (Jan 2026)
  { csvFile: 'IndividualRosterExtract_20260107.csv', tableName: 'raw_individual_brokers' },
  { csvFile: 'OrganizationRosterExtract_20260107.csv', tableName: 'raw_org_brokers' },
  { csvFile: 'BrokerLicenseExtract_20260107.csv', tableName: 'raw_licenses' },
  { csvFile: 'BrokerEO_20260107.csv', tableName: 'raw_eo_insurance' },
  { csvFile: 'Fees_20260107.csv', tableName: 'raw_fees' },
  // Legacy broker files (fallback for brokers not in current extract)
  { csvFile: 'individual-roster-old.csv', tableName: 'raw_individual_brokers_legacy' },
  { csvFile: 'org-old.csv', tableName: 'raw_org_brokers_legacy' },
  // Main data files
  { csvFile: 'CertificateInfo.csv', tableName: 'raw_certificate_info' },
  { csvFile: 'perf.csv', tableName: 'raw_schedule_rates' },
  { csvFile: 'perf-group.csv', tableName: 'raw_perf_groups' },
  { csvFile: 'premiums.csv', tableName: 'raw_premiums' },
  { csvFile: 'CommissionsDetail_*.csv', tableName: 'raw_commissions_detail' },
];

function findMatchingFiles(pattern: string): string[] {
  const files = fs.readdirSync(csvDataPath);
  
  if (pattern.includes('*')) {
    const prefix = pattern.split('*')[0];
    const suffix = pattern.split('*')[1] || '';
    return files
      .filter(f => f.startsWith(prefix) && f.endsWith(suffix) && !f.includes('-old'))
      .sort();
  }
  
  return [pattern];
}

async function getCsvColumns(csvFile: string): Promise<string[]> {
  const filePath = path.join(csvDataPath, csvFile);
  
  return new Promise((resolve, reject) => {
    const parser = fs.createReadStream(filePath).pipe(
      parse({ columns: false, to: 1 })
    );
    
    parser.on('data', (row: string[]) => {
      // Clean BOM from first column if present
      const columns = row.map((col, i) => {
        let cleaned = col.replace(/^\uFEFF/, '').trim();
        // Sanitize column name for SQL Server
        cleaned = cleaned.replace(/[^a-zA-Z0-9_]/g, '_');
        if (/^[0-9]/.test(cleaned)) {
          cleaned = 'Col_' + cleaned;
        }
        return cleaned || `Column${i}`;
      });
      resolve(columns);
    });
    
    parser.on('error', reject);
    parser.on('end', () => resolve([]));
  });
}

async function createTable(pool: sql.ConnectionPool, tableName: string, columns: string[]): Promise<void> {
  // Drop existing table
  await pool.request().query(`DROP TABLE IF EXISTS [etl].[${tableName}]`);
  
  // Create new table with all NVARCHAR(MAX) columns
  const columnDefs = columns.map(col => `[${col}] NVARCHAR(MAX) NULL`).join(',\n  ');
  const createSql = `CREATE TABLE [etl].[${tableName}] (\n  ${columnDefs}\n)`;
  
  await pool.request().query(createSql);
  log(`   Created table [etl].[${tableName}] with ${columns.length} columns`);
}

async function loadCsvToTable(
  pool: sql.ConnectionPool,
  csvFile: string,
  tableName: string,
  columns: string[]
): Promise<number> {
  const BULK_BATCH_SIZE = 5000; // Rows per bulk insert (much faster than parameterized)
  const filePath = path.join(csvDataPath, csvFile);
  
  if (!fs.existsSync(filePath)) {
    log(`⚠️  File not found: ${csvFile}`);
    return 0;
  }

  const fileSize = fs.statSync(filePath).size;
  const limitMsg = ROW_LIMIT > 0 ? ` (LIMIT: ${ROW_LIMIT} rows)` : '';
  log(`   Loading ${csvFile} (${(fileSize / 1024 / 1024).toFixed(2)} MB)${limitMsg}...`);

  let totalRows = 0;
  let batch: Record<string, string>[] = [];

  const parser = fs
    .createReadStream(filePath)
    .pipe(parse({ 
      columns: true, 
      skip_empty_lines: true, 
      relax_column_count: true,
      bom: true  // Handle BOM
    }));

  // Get keys from first row (for column mapping)
  let csvKeys: string[] | null = null;

  for await (const record of parser) {
    // Check row limit
    if (ROW_LIMIT > 0 && totalRows >= ROW_LIMIT) {
      break;
    }
    
    if (!csvKeys) {
      csvKeys = Object.keys(record);
    }
    
    batch.push(record);

    if (batch.length >= BULK_BATCH_SIZE) {
      await bulkInsert(pool, tableName, columns, batch, csvKeys);
      totalRows += batch.length;
      if (totalRows % 50000 === 0) {
        log(`   ... ${totalRows.toLocaleString()} rows loaded`);
      }
      batch = [];
    }
  }

  // Insert remaining rows
  if (batch.length > 0 && csvKeys) {
    await bulkInsert(pool, tableName, columns, batch, csvKeys);
    totalRows += batch.length;
  }

  log(`   ✅ ${totalRows.toLocaleString()} rows from ${csvFile}`);
  return totalRows;
}

async function bulkInsert(
  pool: sql.ConnectionPool,
  tableName: string,
  columns: string[],
  rows: Record<string, string>[],
  csvKeys: string[]
): Promise<void> {
  if (rows.length === 0) return;

  // Create a Table object for bulk insert
  const table = new sql.Table(`[etl].[${tableName}]`);
  table.create = false; // Table already exists
  
  // Define columns (all NVARCHAR(MAX))
  for (const col of columns) {
    table.columns.add(col, sql.NVarChar(sql.MAX), { nullable: true });
  }
  
  // Build column mapping: CSV key -> table column index
  const columnMap: Map<number, string> = new Map();
  for (let j = 0; j < columns.length; j++) {
    const csvKey = csvKeys.find(k => 
      k.replace(/^\uFEFF/, '').replace(/[^a-zA-Z0-9_]/g, '_') === columns[j] ||
      k.replace(/^\uFEFF/, '') === columns[j]
    );
    if (csvKey) {
      columnMap.set(j, csvKey);
    }
  }
  
  // Add rows to table
  for (const row of rows) {
    const values: (string | null)[] = [];
    for (let j = 0; j < columns.length; j++) {
      const csvKey = columnMap.get(j);
      let value = csvKey ? row[csvKey] : null;
      value = value === '' ? null : value;
      values.push(value);
    }
    table.rows.add(...values);
  }

  try {
    const request = pool.request();
    await request.bulk(table);
  } catch (err: any) {
    log(`❌ Error bulk inserting into ${tableName}: ${err.message}`);
    log(`   Columns: ${columns.join(', ')}`);
    log(`   CSV Keys: ${csvKeys.join(', ')}`);
    throw err;
  }
}

async function main() {
  log('');
  log('============================================================');
  log('SQL Server CSV Loader (Dynamic Schema + Bulk Insert)');
  log('============================================================');
  log(`Server: ${config.server}`);
  log(`Database: ${config.database}`);
  log(`CSV Path: ${csvDataPath}`);
  if (ROW_LIMIT > 0) {
    log(`⚠️  ROW LIMIT: ${ROW_LIMIT} rows per file (test mode)`);
  }
  log('');

  log('Connecting to SQL Server...');
  const pool = await sql.connect(config);
  log('✅ Connected');
  log('');

  const summary: { table: string; files: number; rows: number }[] = [];

  for (const mapping of csvMappings) {
    // Find all matching files
    const files = Array.isArray(mapping.csvFile) 
      ? mapping.csvFile.flatMap(p => findMatchingFiles(p))
      : findMatchingFiles(mapping.csvFile);
    
    if (files.length === 0) {
      log(`⚠️  No files found for pattern: ${mapping.csvFile}`);
      continue;
    }

    log(`\n📁 ${mapping.tableName} (${files.length} file(s))`);
    
    // Get columns from first file
    const firstFile = files[0];
    const columns = await getCsvColumns(firstFile);
    
    if (columns.length === 0) {
      log(`⚠️  No columns found in ${firstFile}`);
      continue;
    }
    
    // Create table
    await createTable(pool, mapping.tableName, columns);
    
    // Load all files
    let totalRows = 0;
    for (const file of files) {
      totalRows += await loadCsvToTable(pool, file, mapping.tableName, columns);
    }
    
    summary.push({ table: mapping.tableName, files: files.length, rows: totalRows });
  }

  log('');
  log('============================================================');
  log('LOAD SUMMARY');
  log('============================================================');
  console.table(summary);

  // Verify counts
  log('');
  log('Verifying counts in database...');
  const tables = summary.map(s => s.table);
  const countQueries = tables.map(t => `SELECT '${t}' as tbl, COUNT(*) as cnt FROM [etl].[${t}]`);
  const result = await pool.request().query(countQueries.join(' UNION ALL ') + ' ORDER BY tbl');
  console.table(result.recordset);

  await pool.close();
  log('');
  log('✅ CSV LOAD COMPLETED');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
