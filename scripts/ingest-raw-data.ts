/**
 * Improved Raw Data Ingest Script
 * ================================
 * 
 * Features:
 * - Extract from ZIP file (user-specified or auto-detect most recent in ~/Downloads)
 * - Find CSV files by prefix matching
 * - Validate column headers against expected schema
 * - Auto-select schema name (raw_data1, raw_data2, etc.)
 * - Preview mode (10 records per table)
 * - Dry-run mode
 * 
 * Usage:
 *   npx tsx scripts/ingest-raw-data.ts [options]
 * 
 * Options:
 *   --zip <path>           Specify ZIP file path
 *   --schema <name>        Specify schema name (default: auto-detect)
 *   --preview              Preview mode (10 records per table)
 *   --dry-run              Dry run (show what would be done)
 *   --skip-validation      Skip column header validation
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import { parse } from 'csv-parse';
const yauzl = require('yauzl');

// =============================================================================
// Configuration
// =============================================================================

/**
 * Parse a SQL Server connection string into mssql config
 */
function parseConnectionString(connStr: string): Partial<sql.config> {
  const parts: Record<string, string> = {};
  connStr.split(';').forEach(part => {
    const [key, ...valueParts] = part.split('=');
    if (key && valueParts.length > 0) {
      parts[key.trim().toLowerCase()] = valueParts.join('=').trim();
    }
  });
  
  return {
    server: parts['server'] || parts['data source'],
    database: parts['database'] || parts['initial catalog'],
    user: parts['user id'] || parts['uid'] || parts['user'],
    password: parts['password'] || parts['pwd'],
    options: {
      encrypt: parts['encrypt']?.toLowerCase() !== 'false',
      trustServerCertificate: parts['trustservercertificate']?.toLowerCase() === 'true',
    }
  };
}

/**
 * Get SQL Server configuration from environment
 */
function getSqlConfig(): sql.config {
  const connStr = process.env.SQLSERVER;
  
  if (connStr) {
    const parsed = parseConnectionString(connStr);
    if (!parsed.server || !parsed.database || !parsed.user || !parsed.password) {
      console.error('❌ Invalid $SQLSERVER connection string.');
      process.exit(1);
    }
    return {
      server: parsed.server,
      database: parsed.database,
      user: parsed.user,
      password: parsed.password,
      options: {
        encrypt: parsed.options?.encrypt ?? true,
        trustServerCertificate: parsed.options?.trustServerCertificate ?? true,
      },
      requestTimeout: 600000,
      connectionTimeout: 30000,
    };
  }
  
  const server = process.env.SQLSERVER_HOST;
  const database = process.env.SQLSERVER_DATABASE;
  const user = process.env.SQLSERVER_USER;
  const password = process.env.SQLSERVER_PASSWORD;
  
  if (!server || !database || !user || !password) {
    console.error('❌ SQL Server connection not configured!');
    process.exit(1);
  }
  
  return {
    server,
    database,
    user,
    password,
    options: {
      encrypt: true,
      trustServerCertificate: true,
    },
    requestTimeout: 600000,
    connectionTimeout: 30000,
  };
}

// Config will be created lazily when needed (not in dry-run mode)
let config: sql.config | null = null;
function getConfig(): sql.config {
  if (!config) {
    config = getSqlConfig();
  }
  return config;
}

// =============================================================================
// File Mappings
// =============================================================================

interface FileMapping {
  prefix: string;           // File prefix to match (e.g., "brokers", "CertificateInfo")
  tableName: string;        // Target table name (without schema)
  expectedColumns: string[]; // Expected column names (for validation)
}

const FILE_MAPPINGS: FileMapping[] = [
  {
    prefix: 'brokers',
    tableName: 'raw_brokers',
    expectedColumns: ['BrokerId', 'Name', 'Status', 'Type'] // Add more as needed
  },
  {
    prefix: 'CertificateInfo',
    tableName: 'raw_certificate_info',
    expectedColumns: ['Company', 'ProductMasterCategory', 'ProductCategory', 'GroupId', 'Product', 'PlanCode', 'CertificateId', 'CertEffectiveDate', 'CertIssuedState', 'CertStatus', 'CertPremium', 'CertSplitSeq', 'CertSplitPercent', 'CustomerId', 'RecStatus']
  },
  {
    prefix: 'perf',
    tableName: 'raw_schedule_rates',
    expectedColumns: ['ScheduleName', 'ProductCode', 'State', 'Level', 'Year2', 'Year16'] // Add more as needed
  },
  {
    prefix: 'premiums',
    tableName: 'raw_premiums',
    expectedColumns: ['Company', 'GroupNumber', 'Policy', 'OldPolicy', 'LastName', 'FirstName', 'Product', 'MasterCategory', 'Category', 'PayMode', 'StateIssued', 'Division', 'CertificateEffectiveDate', 'DatePost', 'DatePaidTo', 'Amount', 'TransactionType', 'InvoiceNumber', 'CommissionType', 'GroupName', 'SplitPercentage', 'SplitCommissionHierarchy', 'SplitSalesHierarchy', 'LionRecNo']
  },
  {
    prefix: 'CommissionsDetail',
    tableName: 'raw_commissions_detail',
    expectedColumns: ['Company', 'CertificateId', 'CertEffectiveDate', 'SplitBrokerId', 'PmtPostedDate', 'PaidToDate', 'PaidAmount', 'TransActionType', 'InvoiceNumber', 'CertInForceMonths', 'CommissionRate', 'RealCommissionRate', 'PaidBrokerId', 'CreaditCardType', 'TransactionId']
  },
  {
    prefix: 'licenses',
    tableName: 'raw_broker_licenses',
    expectedColumns: ['BrokerId', 'State', 'LicenseNumber', 'Type', 'Status', 'EffectiveDate', 'ExpirationDate']
  },
  {
    prefix: 'EO',
    tableName: 'raw_broker_eo',
    expectedColumns: ['BrokerId', 'PolicyNumber', 'Carrier', 'CoverageAmount', 'EffectiveDate', 'ExpirationDate']
  }
];

// =============================================================================
// Utility Functions
// =============================================================================

function log(msg: string, level: 'info' | 'success' | 'warn' | 'error' = 'info'): void {
  const timestamp = new Date().toISOString();
  const prefix = {
    info: '   ',
    success: ' ✅',
    warn: ' ⚠️',
    error: ' ❌'
  }[level];
  console.log(`[${timestamp}]${prefix} ${msg}`);
}

/**
 * Find most recent ZIP file in Downloads folder
 */
function findMostRecentZip(): string | null {
  const downloadsPath = path.join(process.env.HOME || '', 'Downloads');
  if (!fs.existsSync(downloadsPath)) {
    return null;
  }
  
  const files = fs.readdirSync(downloadsPath)
    .filter(f => f.toLowerCase().endsWith('.zip'))
    .map(f => ({
      name: f,
      path: path.join(downloadsPath, f),
      mtime: fs.statSync(path.join(downloadsPath, f)).mtime
    }))
    .sort((a, b) => b.mtime.getTime() - a.mtime.getTime());
  
  return files.length > 0 ? files[0].path : null;
}

/**
 * Extract ZIP file to temporary directory
 */
async function extractZip(zipPath: string, extractDir: string): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const extractedFiles: string[] = [];
    
    yauzl.open(zipPath, { lazyEntries: true }, (err, zipfile) => {
      if (err) return reject(err);
      
      zipfile.readEntry();
      
      zipfile.on('entry', (entry) => {
        if (/\/$/.test(entry.fileName)) {
          // Directory entry
          zipfile.readEntry();
        } else {
          // File entry
          zipfile.openReadStream(entry, (err, readStream) => {
            if (err) return reject(err);
            
            const filePath = path.join(extractDir, entry.fileName);
            const dir = path.dirname(filePath);
            if (!fs.existsSync(dir)) {
              fs.mkdirSync(dir, { recursive: true });
            }
            
            const writeStream = fs.createWriteStream(filePath);
            readStream.pipe(writeStream);
            
            writeStream.on('close', () => {
              extractedFiles.push(filePath);
              zipfile.readEntry();
            });
          });
        }
      });
      
      zipfile.on('end', () => {
        resolve(extractedFiles);
      });
      
      zipfile.on('error', reject);
    });
  });
}

/**
 * Find CSV files matching prefixes
 */
function findMatchingFiles(extractedFiles: string[], prefix: string): string[] {
  return extractedFiles
    .filter(f => {
      const fileName = path.basename(f).toLowerCase();
      const prefixLower = prefix.toLowerCase();
      return fileName.startsWith(prefixLower) && fileName.endsWith('.csv');
    })
    .sort();
}

/**
 * Read CSV headers
 */
async function readCsvHeaders(filePath: string): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const parser = fs.createReadStream(filePath).pipe(
      parse({ columns: false, to: 1 })
    );
    
    parser.on('data', (row: string[]) => {
      const columns = row.map((col, i) => {
        let cleaned = col.replace(/^\uFEFF/, '').trim();
        // Sanitize for SQL Server
        cleaned = cleaned.replace(/[^a-zA-Z0-9_]/g, '_');
        if (/^[0-9]/.test(cleaned)) {
          cleaned = 'Col_' + cleaned;
        }
        return cleaned || `Column${i}`;
      });
      resolve(columns);
    });
    
    parser.on('error', reject);
  });
}

/**
 * Validate column headers
 */
function validateHeaders(actual: string[], expected: string[]): { valid: boolean; missing: string[]; extra: string[] } {
  const actualSet = new Set(actual.map(c => c.toLowerCase()));
  const expectedSet = new Set(expected.map(c => c.toLowerCase()));
  
  const missing = expected.filter(c => !actualSet.has(c.toLowerCase()));
  const extra = actual.filter(c => !expectedSet.has(c.toLowerCase()));
  
  return {
    valid: missing.length === 0,
    missing,
    extra
  };
}

/**
 * Find next available schema name
 */
async function findNextSchema(pool: sql.ConnectionPool): Promise<string> {
  const result = await pool.request().query(`
    SELECT SCHEMA_NAME 
    FROM INFORMATION_SCHEMA.SCHEMATA 
    WHERE SCHEMA_NAME LIKE 'raw_data%'
    ORDER BY SCHEMA_NAME DESC
  `);
  
  if (result.recordset.length === 0) {
    return 'raw_data1';
  }
  
  const lastSchema = result.recordset[0].SCHEMA_NAME;
  const match = lastSchema.match(/raw_data(\d+)/);
  if (match) {
    const num = parseInt(match[1], 10);
    return `raw_data${num + 1}`;
  }
  
  return 'raw_data1';
}

/**
 * Create schema and tables
 */
async function createSchemaAndTables(pool: sql.ConnectionPool, schemaName: string, dryRun: boolean): Promise<void> {
  if (dryRun) {
    log(`Would create schema [${schemaName}] and tables`, 'info');
    return;
  }
  
  // Create schema
  await pool.request().query(`CREATE SCHEMA [${schemaName}]`);
  log(`Created schema [${schemaName}]`, 'success');
  
  // Read table creation script
  const sqlPath = path.resolve(__dirname, '../sql/01-raw-tables.sql');
  const sqlContent = fs.readFileSync(sqlPath, 'utf8');
  
  // Replace schema name in SQL
  const modifiedSql = sqlContent
    .replace(/\[etl\]\./g, `[${schemaName}].`)
    .replace(/CREATE TABLE \[etl\]/g, `CREATE TABLE [${schemaName}]`)
    .replace(/DROP TABLE IF EXISTS \[etl\]/g, `DROP TABLE IF EXISTS [${schemaName}]`);
  
  // Execute in batches
  const batches = modifiedSql.split(/^\s*GO\s*$/gim).filter(b => b.trim());
  for (const batch of batches) {
    if (batch.trim()) {
      await pool.request().query(batch);
    }
  }
  
  log(`Created tables in schema [${schemaName}]`, 'success');
}

/**
 * Escape SQL string value
 */
function escapeSqlValue(value: any): string {
  if (value === null || value === undefined) {
    return 'NULL';
  }
  const str = String(value);
  return `N'${str.replace(/'/g, "''").replace(/\0/g, '')}'`;
}

/**
 * Load CSV data into table
 */
async function loadCsvData(
  pool: sql.ConnectionPool | null,
  schemaName: string,
  filePath: string,
  tableName: string,
  limit: number = 0,
  dryRun: boolean = false
): Promise<number> {
  if (dryRun || !pool) {
    log(`Would load ${limit > 0 ? `first ${limit} rows from ` : ''}${path.basename(filePath)} into [${schemaName}].[${tableName}]`, 'info');
    return 0;
  }
  
  const headers = await readCsvHeaders(filePath);
  const columnList = headers.map(h => `[${h}]`).join(', ');
  
  const rows: any[] = [];
  let rowCount = 0;
  
  return new Promise((resolve, reject) => {
    const parser = fs.createReadStream(filePath).pipe(
      parse({ 
        columns: headers,
        skip_empty_lines: true,
        from: 2 // Skip header row
      })
    );
    
    parser.on('data', async (row: any) => {
      if (limit > 0 && rowCount >= limit) {
        parser.destroy();
        return;
      }
      
      rows.push(row);
      rowCount++;
      
      // Batch insert every 1000 rows
      if (rows.length >= 1000) {
        parser.pause();
        try {
          await insertBatch(pool, schemaName, tableName, columnList, headers, rows);
          rows.length = 0;
          parser.resume();
        } catch (err: any) {
          parser.destroy();
          reject(err);
        }
      }
    });
    
    parser.on('end', async () => {
      // Insert remaining rows
      if (rows.length > 0) {
        try {
          await insertBatch(pool, schemaName, tableName, columnList, headers, rows);
        } catch (err: any) {
          reject(err);
          return;
        }
      }
      resolve(rowCount);
    });
    
    parser.on('error', reject);
  });
}

/**
 * Insert a batch of rows
 */
async function insertBatch(
  pool: sql.ConnectionPool,
  schemaName: string,
  tableName: string,
  columnList: string,
  headers: string[],
  rows: any[]
): Promise<void> {
  const batchSize = 100; // Insert 100 rows at a time to avoid query size limits
  
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const values = batch.map(row => {
      const vals = headers.map(h => escapeSqlValue(row[h]));
      return `(${vals.join(', ')})`;
    });
    
    await pool.request().query(`
      INSERT INTO [${schemaName}].[${tableName}] (${columnList})
      VALUES ${values.join(', ')}
    `);
  }
}

// =============================================================================
// Main Function
// =============================================================================

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const zipIndex = args.indexOf('--zip');
  const schemaIndex = args.indexOf('--schema');
  const preview = args.includes('--preview');
  const dryRun = args.includes('--dry-run');
  const skipValidation = args.includes('--skip-validation');
  
  const zipPath = zipIndex !== -1 ? args[zipIndex + 1] : findMostRecentZip();
  const schemaName = schemaIndex !== -1 ? args[schemaIndex + 1] : null;
  
  log('');
  log('═'.repeat(70));
  log('Raw Data Ingest');
  log('═'.repeat(70));
  log(`ZIP File: ${zipPath || 'Not found'}`);
  log(`Schema: ${schemaName || 'Auto-detect'}`);
  log(`Preview Mode: ${preview ? 'Yes (10 records)' : 'No (all records)'}`);
  log(`Dry Run: ${dryRun ? 'Yes' : 'No'}`);
  log('');
  
  if (!zipPath || !fs.existsSync(zipPath)) {
    log('❌ ZIP file not found!', 'error');
    log('Please specify with --zip <path> or ensure a ZIP file exists in ~/Downloads', 'error');
    process.exit(1);
  }
  
  const extractDir = path.join(process.env.TMPDIR || '/tmp', `etl-extract-${Date.now()}`);
  
  try {
    // Extract ZIP
    log('Extracting ZIP file...');
    fs.mkdirSync(extractDir, { recursive: true });
    const extractedFiles = await extractZip(zipPath, extractDir);
    log(`Extracted ${extractedFiles.length} files`, 'success');
    
    // List all extracted filenames
    log('');
    log('═'.repeat(70));
    log('EXTRACTED FILES FROM ZIP');
    log('═'.repeat(70));
    const allFiles = extractedFiles.map(f => path.basename(f)).sort();
    const csvFiles = allFiles.filter(f => f.toLowerCase().endsWith('.csv'));
    const otherFiles = allFiles.filter(f => !f.toLowerCase().endsWith('.csv'));
    
    if (csvFiles.length > 0) {
      log(`CSV Files (${csvFiles.length}):`, 'info');
      csvFiles.forEach(f => log(`  ✓ ${f}`, 'info'));
    }
    if (otherFiles.length > 0) {
      log(`Other Files (${otherFiles.length}):`, 'info');
      otherFiles.forEach(f => log(`  - ${f}`, 'info'));
    }
    log('═'.repeat(70));
    log('');
    
    // Only connect to database if not in dry-run mode
    let pool: sql.ConnectionPool | null = null;
    let targetSchema: string;
    
    if (dryRun) {
      targetSchema = schemaName || 'raw_data1'; // Use default for dry-run
      log(`Using schema: [${targetSchema}] (dry-run)`, 'success');
    } else {
      pool = await sql.connect(getConfig());
      targetSchema = schemaName || await findNextSchema(pool);
      log(`Using schema: [${targetSchema}]`, 'success');
    }
    
    // Create schema and tables
    if (pool) {
      await createSchemaAndTables(pool, targetSchema, dryRun);
    }
    
    // Process each file mapping
    const limit = preview ? 10 : 0;
    let totalRows = 0;
    
    for (const mapping of FILE_MAPPINGS) {
      log(`Searching for files with prefix "${mapping.prefix}"...`);
      const matchingFiles = findMatchingFiles(extractedFiles, mapping.prefix);
      
      if (matchingFiles.length === 0) {
        log(`  ⚠️  No files found for prefix "${mapping.prefix}"`, 'warn');
        // Show what CSV files are available for debugging
        const csvFiles = extractedFiles
          .map(f => path.basename(f))
          .filter(f => f.toLowerCase().endsWith('.csv'))
          .map(f => `    - ${f}`)
          .join('\n');
        if (csvFiles) {
          log(`  Available CSV files:\n${csvFiles}`, 'info');
        }
        continue;
      }
      
      log(`  Found ${matchingFiles.length} file(s) matching "${mapping.prefix}":`, 'success');
      matchingFiles.forEach(f => log(`    - ${path.basename(f)}`, 'info'));
      
      for (const filePath of matchingFiles) {
        log(`Processing ${path.basename(filePath)}...`);
        
        // Validate headers
        if (!skipValidation) {
          const headers = await readCsvHeaders(filePath);
          const validation = validateHeaders(headers, mapping.expectedColumns);
          
          if (!validation.valid) {
            log(`  ⚠️  Column validation warnings:`, 'warn');
            if (validation.missing.length > 0) {
              log(`     Missing: ${validation.missing.join(', ')}`, 'warn');
            }
            if (validation.extra.length > 0) {
              log(`     Extra: ${validation.extra.join(', ')}`, 'warn');
            }
          } else {
            log(`  ✅ Column headers validated`, 'success');
          }
        }
        
        // Load data
        const rowCount = await loadCsvData(
          pool,
          targetSchema,
          filePath,
          mapping.tableName,
          limit,
          dryRun
        );
        
        totalRows += rowCount;
        log(`  Loaded ${rowCount.toLocaleString()} rows into [${targetSchema}].[${mapping.tableName}]`, 'success');
      }
    }
    
    log('');
    log(`Total rows loaded: ${totalRows.toLocaleString()}`, 'success');
    log(`Schema: [${targetSchema}]`, 'success');
    
    if (preview) {
      log('');
      log('⚠️  Preview mode: Only 10 records per table were loaded', 'warn');
      log('Run without --preview to load all data', 'info');
    }
    
  } catch (err: any) {
    log(`Error: ${err.message}`, 'error');
    console.error(err);
    process.exit(1);
  } finally {
    // Cleanup
    if (fs.existsSync(extractDir)) {
      fs.rmSync(extractDir, { recursive: true, force: true });
    }
    if (pool) {
      await pool.close();
    }
  }
}

main().catch(err => {
  log(`Fatal error: ${err.message}`, 'error');
  console.error(err);
  process.exit(1);
});
