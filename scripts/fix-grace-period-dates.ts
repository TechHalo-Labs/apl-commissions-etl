/**
 * Fix Grace Period Dates - Bug #36
 * ===================================
 * Corrects far-future expiration dates (>2050-01-01) in:
 * - BrokerLicenses
 * - BrokerAppointments
 * - BrokerEOInsurances
 * 
 * This script is part of the Bootstrap ETL process and runs after data export.
 * 
 * Usage:
 *   npx tsx scripts/fix-grace-period-dates.ts
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Parse a SQL Server connection string into mssql config
 * Format: Server=...;Database=...;User Id=...;Password=...;TrustServerCertificate=...;Encrypt=...;
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
      enableArithAbort: true,
    }
  };
}

/**
 * Get SQL Server configuration from environment
 * REQUIRES: $SQLSERVER connection string OR individual env vars
 * NO DEFAULTS - Will exit with error if not configured
 */
function getSqlConfig(): sql.config {
  const connStr = process.env.SQLSERVER;
  
  if (connStr) {
    const parsed = parseConnectionString(connStr);
    if (!parsed.server || !parsed.database || !parsed.user || !parsed.password) {
      console.error('❌ Invalid $SQLSERVER connection string. Expected format:');
      console.error('   Server=<host>;Database=<db>;User Id=<user>;Password=<pwd>;TrustServerCertificate=True;Encrypt=True;');
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
        enableArithAbort: true,
      },
      pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30000,
      }
    };
  }
  
  // Fall back to individual environment variables
  const server = process.env.SQLSERVER_HOST || process.env.SQL_SERVER;
  const database = process.env.SQLSERVER_DATABASE || process.env.SQL_DATABASE;
  const user = process.env.SQLSERVER_USER || process.env.SQL_USER;
  const password = process.env.SQLSERVER_PASSWORD || process.env.SQL_PASSWORD;
  
  if (!server || !database || !user || !password) {
    console.error('');
    console.error('❌ SQL Server connection not configured!');
    console.error('');
    console.error('Please set one of the following:');
    console.error('');
    console.error('Option 1: Single connection string (recommended)');
    console.error('  export SQLSERVER="Server=<host>;Database=<db>;User Id=<user>;Password=<pwd>;TrustServerCertificate=True;Encrypt=True;"');
    console.error('');
    console.error('Option 2: Individual environment variables');
    console.error('  export SQLSERVER_HOST=<host>');
    console.error('  export SQLSERVER_DATABASE=<db>');
    console.error('  export SQLSERVER_USER=<user>');
    console.error('  export SQLSERVER_PASSWORD=<pwd>');
    console.error('');
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
      enableArithAbort: true,
    },
    pool: {
      max: 10,
      min: 0,
      idleTimeoutMillis: 30000,
    }
  };
}

const config: sql.config = getSqlConfig();

function log(message: string, type: 'info' | 'success' | 'error' | 'warn' = 'info'): void {
  const timestamp = new Date().toISOString();
  const colors = {
    info: '\x1b[36m',    // Cyan
    success: '\x1b[32m', // Green
    error: '\x1b[31m',   // Red
    warn: '\x1b[33m',    // Yellow
    reset: '\x1b[0m',
  };
  
  const prefix = type === 'info' ? 'ℹ️' : type === 'success' ? '✅' : type === 'error' ? '❌' : '⚠️';
  console.log(`${colors[type]}${prefix} [${timestamp}] ${message}${colors.reset}`);
}

async function executeSqlFile(pool: sql.ConnectionPool, filePath: string): Promise<void> {
  if (!fs.existsSync(filePath)) {
    throw new Error(`SQL file not found: ${filePath}`);
  }
  
  const sqlContent = fs.readFileSync(filePath, 'utf8');
  
  // Split by GO statements (SQL Server batch separator)
  const batches = sqlContent
    .split(/^\s*GO\s*$/gim)
    .filter(batch => batch.trim().length > 0);
  
  log(`Executing ${path.basename(filePath)} (${batches.length} batches)...`);
  
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i].trim();
    if (!batch) continue;
    
    try {
      const request = pool.request();
      
      // Handle PRINT statements and result sets
      request.on('info', (info: any) => {
        log(`  ${info.message}`, 'info');
      });
      
      const result = await request.batch(batch);
      
      // If result has rows, log summary
      if (result.recordset && result.recordset.length > 0) {
        log(`  Batch ${i + 1} completed: ${result.recordset.length} result rows`);
      }
    } catch (err: any) {
      log(`Error in batch ${i + 1}: ${err.message}`, 'error');
      log(`Failed SQL batch:\n${batch.substring(0, 500)}...`, 'error');
      throw err;
    }
  }
  
  log(`${path.basename(filePath)} completed`, 'success');
}

async function analyzeAffectedRecords(pool: sql.ConnectionPool): Promise<void> {
  log('');
  log('='.repeat(60));
  log('ANALYZING AFFECTED RECORDS');
  log('='.repeat(60));
  
  const analysisQueries = [
    {
      table: 'BrokerLicenses',
      query: `
        SELECT 
            'BrokerLicenses' as TableName,
            COUNT(*) as AffectedCount,
            MIN(ExpirationDate) as MinBadDate,
            MAX(ExpirationDate) as MaxBadDate
        FROM [dbo].[BrokerLicenses]
        WHERE ExpirationDate > '2050-01-01'
      `,
    },
    {
      table: 'BrokerAppointments',
      query: `
        SELECT 
            'BrokerAppointments' as TableName,
            COUNT(*) as AffectedCount,
            MIN(ExpirationDate) as MinBadDate,
            MAX(ExpirationDate) as MaxBadDate
        FROM [dbo].[BrokerAppointments]
        WHERE ExpirationDate IS NOT NULL 
          AND ExpirationDate > '2050-01-01'
      `,
    },
    {
      table: 'BrokerEOInsurances',
      query: `
        SELECT 
            'BrokerEOInsurances' as TableName,
            COUNT(*) as AffectedCount,
            MIN(ExpirationDate) as MinBadDate,
            MAX(ExpirationDate) as MaxBadDate
        FROM [dbo].[BrokerEOInsurances]
        WHERE ExpirationDate > '2050-01-01'
      `,
    },
  ];
  
  for (const { table, query } of analysisQueries) {
    try {
      const result = await pool.request().query(query);
      if (result.recordset && result.recordset.length > 0) {
        const row = result.recordset[0];
        log(`${table}: ${row.AffectedCount} affected records (${row.MinBadDate} to ${row.MaxBadDate})`);
      }
    } catch (err: any) {
      log(`Error analyzing ${table}: ${err.message}`, 'error');
    }
  }
}

async function main(): Promise<void> {
  let pool: sql.ConnectionPool | null = null;
  
  try {
    // Connect to SQL Server
    log('Connecting to SQL Server...');
    pool = await sql.connect(config);
    log('Connected', 'success');
    
    // Analyze affected records
    await analyzeAffectedRecords(pool);
    
    // Execute fix script
    log('');
    log('='.repeat(60));
    log('FIXING GRACE PERIOD DATES');
    log('='.repeat(60));
    
    const scriptPath = path.resolve(__dirname, '../../scripts/fix-grace-period-dates.sql');
    await executeSqlFile(pool, scriptPath);
    
    // Final verification
    log('');
    log('='.repeat(60));
    log('VERIFICATION');
    log('='.repeat(60));
    await analyzeAffectedRecords(pool);
    
    log('');
    log('='.repeat(60));
    log('GRACE PERIOD DATE FIX COMPLETED', 'success');
    log('='.repeat(60));
    
  } catch (err: any) {
    log(`Fix failed: ${err.message}`, 'error');
    console.error(err);
    process.exit(1);
  } finally {
    if (pool) {
      await pool.close();
      log('Connection closed');
    }
  }
}

main();
