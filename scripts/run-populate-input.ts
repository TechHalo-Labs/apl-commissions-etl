/**
 * Setup input/staging tables and populate input tables from raw data
 * Used when raw data is already loaded to etl.raw_* tables
 * 
 * Runs:
 * 1. 02-input-tables.sql - Creates input_* tables (does NOT touch raw_* tables)
 * 2. 03-staging-tables.sql - Creates stg_* tables
 * 3. 03a-prestage-tables.sql - Creates prestage tables
 * 4. 03b-conformance-table.sql - Creates conformance table
 * 5. populate-input-tables.sql - Populates input from raw
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';

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
  
  console.error('❌ SQLSERVER environment variable not set');
  process.exit(1);
}

async function runSqlScript(pool: sql.ConnectionPool, scriptPath: string, name: string): Promise<void> {
  console.log(`\n📄 Running ${name}...`);
  
  let sqlContent = fs.readFileSync(scriptPath, 'utf8');
  
  // Replace variables
  sqlContent = sqlContent.replace(/\$\(ETL_SCHEMA\)/g, 'etl');
  sqlContent = sqlContent.replace(/\$\(SOURCE_SCHEMA\)/g, 'etl');
  sqlContent = sqlContent.replace(/\$\(PRODUCTION_SCHEMA\)/g, 'dbo');
  
  // Split by GO and execute each batch
  const batches = sqlContent.split(/^\s*GO\s*$/gim).filter(b => b.trim());
  
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i].trim();
    if (!batch) continue;
    
    try {
      const result = await pool.request().query(batch);
      if (result.recordset && result.recordset.length > 0 && result.recordset.length <= 10) {
        console.table(result.recordset);
      }
    } catch (err: any) {
      // Ignore "object already exists" errors
      if (err.message.includes('already an object named') || err.message.includes('already exists')) {
        continue;
      }
      console.error(`❌ Error in ${name} batch ${i + 1}: ${err.message}`);
      throw err;
    }
  }
  
  console.log(`✅ ${name} completed`);
}

async function main() {
  console.log('');
  console.log('═'.repeat(70));
  console.log('Setup Tables and Populate Input from Raw Data');
  console.log('═'.repeat(70));
  console.log('');
  
  const config = getSqlConfig();
  console.log(`Server: ${config.server}`);
  console.log(`Database: ${config.database}`);
  console.log('');
  
  const pool = await sql.connect(config);
  console.log('✅ Connected to SQL Server');
  
  try {
    const sqlDir = path.join(__dirname, '../sql');
    
    // 1. Create input tables (does NOT touch raw tables)
    await runSqlScript(pool, path.join(sqlDir, '02-input-tables.sql'), '02-input-tables.sql');
    
    // 2. Create staging tables
    await runSqlScript(pool, path.join(sqlDir, '03-staging-tables.sql'), '03-staging-tables.sql');
    
    // 3. Create prestage tables
    await runSqlScript(pool, path.join(sqlDir, '03a-prestage-tables.sql'), '03a-prestage-tables.sql');
    
    // 4. Create conformance table
    await runSqlScript(pool, path.join(sqlDir, '03b-conformance-table.sql'), '03b-conformance-table.sql');
    
    // 5. Populate input tables from raw
    await runSqlScript(pool, path.join(sqlDir, 'ingest/populate-input-tables.sql'), 'populate-input-tables.sql');
    
    // Verify counts
    console.log('\n');
    console.log('═'.repeat(70));
    console.log('VERIFICATION');
    console.log('═'.repeat(70));
    
    const verifyResult = await pool.request().query(`
      SELECT 'raw_certificate_info' AS tbl, COUNT(*) AS cnt FROM [etl].[raw_certificate_info]
      UNION ALL SELECT 'input_certificate_info', COUNT(*) FROM [etl].[input_certificate_info]
      UNION ALL SELECT 'raw_schedule_rates', COUNT(*) FROM [etl].[raw_schedule_rates]
      UNION ALL SELECT 'raw_perf_groups', COUNT(*) FROM [etl].[raw_perf_groups]
      UNION ALL SELECT 'raw_individual_brokers', COUNT(*) FROM [etl].[raw_individual_brokers]
      UNION ALL SELECT 'raw_org_brokers', COUNT(*) FROM [etl].[raw_org_brokers]
    `);
    console.table(verifyResult.recordset);
    
  } catch (err: any) {
    console.error(`❌ Error: ${err.message}`);
    process.exit(1);
  } finally {
    await pool.close();
  }
  
  console.log('');
  console.log('═'.repeat(70));
  console.log('COMPLETE - Ready for transforms');
  console.log('═'.repeat(70));
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
