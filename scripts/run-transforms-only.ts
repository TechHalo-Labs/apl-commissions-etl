#!/usr/bin/env tsx
/**
 * Run transforms only (skip data loading)
 * Use this when raw/input tables are already populated
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';

function parseConnectionString(connStr: string): Partial<sql.config> {
  const parts: Record<string, string> = {};
  connStr.split(';').forEach(part => {
    const [key, ...valueParts] = part.split('=');
    if (key && valueParts.length > 0) {
      parts[key.trim().toLowerCase()] = valueParts.join('=').trim();
    }
  });
  const encrypt = parts['encrypt'];
  const trustCert = parts['trustservercertificate'];
  return {
    server: parts['server'] || parts['data source'],
    database: parts['database'] || parts['initial catalog'],
    user: parts['user id'] || parts['uid'],
    password: parts['password'] || parts['pwd'],
    options: {
      encrypt: encrypt === undefined ? true : encrypt.toLowerCase() === 'true',
      trustServerCertificate: trustCert === undefined ? true : trustCert.toLowerCase() === 'true',
      enableArithAbort: true,
    },
    pool: { max: 10, min: 0, idleTimeoutMillis: 30000 },
    requestTimeout: 600000, // 10 minutes
  };
}

function getSqlConfig(): sql.config {
  const connStr = process.env.SQLSERVER;
  if (!connStr) {
    console.error('SQLSERVER environment variable not set');
    process.exit(1);
  }
  return parseConnectionString(connStr) as sql.config;
}

function log(message: string, level: 'info' | 'success' | 'warn' | 'error' = 'info') {
  const timestamp = new Date().toISOString();
  const prefix = level === 'success' ? '✅' : level === 'warn' ? '⚠️' : level === 'error' ? '❌' : '📋';
  console.log(`[${timestamp}] ${prefix}  ${message}`);
}

const transforms = [
  // Phase 1: Reference tables
  'sql/transforms/00-references.sql',
  
  // Phase 2: Core entities
  'sql/transforms/01-brokers.sql',
  'sql/transforms/12-licenses.sql',
  'sql/transforms/13-eo-insurances.sql',
  'sql/transforms/02-groups.sql',
  'sql/transforms/03-products.sql',
  'sql/transforms/04-schedules.sql',
  
  // Phase 3: Tiered proposal creation
  'sql/transforms/06a-proposals-simple-groups.sql',
  'sql/transforms/06b-proposals-non-conformant.sql',
  'sql/transforms/06c-proposals-plan-differentiated.sql',
  'sql/transforms/06d-proposals-year-differentiated.sql',
  'sql/transforms/06e-proposals-granular.sql',
  'sql/transforms/06f-consolidate-proposals.sql',
  'sql/transforms/06g-normalize-proposal-date-ranges.sql',
  
  // Phase 4: Hierarchies and splits
  'sql/transforms/07-hierarchies.sql',
  'sql/transforms/08-hierarchy-splits.sql',
  
  // Phase 5: Policies and transactions
  'sql/transforms/09-policies.sql',
  'sql/transforms/10-premium-transactions.sql',
  
  // Phase 6: Policy hierarchy assignments
  'sql/transforms/11-policy-hierarchy-assignments.sql',
  
  // Phase 7: Additional entities
  'sql/transforms/11-fees.sql',
  'sql/transforms/12-broker-banking-infos.sql',
];

async function executeSqlFile(pool: sql.ConnectionPool, filePath: string) {
  const fullPath = path.join(process.cwd(), filePath);
  const fileName = path.basename(filePath);
  
  log(`Executing ${filePath}...`);
  
  if (!fs.existsSync(fullPath)) {
    log(`File not found: ${fullPath}`, 'error');
    throw new Error(`File not found: ${fullPath}`);
  }
  
  const sqlContent = fs.readFileSync(fullPath, 'utf8');
  
  // Split by GO statements (case insensitive)
  const batches = sqlContent.split(/^\s*GO\s*$/im).filter(b => b.trim().length > 0);
  
  log(`  ${batches.length} batch(es) found`);
  
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i].trim();
    if (batch.length === 0) continue;
    
    try {
      await pool.request().query(batch);
    } catch (err: any) {
      log(`Error in batch ${i + 1}: ${err.message}`, 'error');
      throw err;
    }
  }
  
  log(`  ${fileName} completed`, 'success');
}

async function main() {
  console.log('='.repeat(60));
  console.log('ETL TRANSFORMS ONLY');
  console.log('='.repeat(60));
  console.log('');
  
  const config = getSqlConfig();
  const pool = await sql.connect(config);
  
  try {
    log('Connected to SQL Server');
    
    // Quick check that input tables have data
    const certCount = await pool.request().query(`SELECT COUNT(*) as cnt FROM [etl].[input_certificate_info]`);
    log(`Input tables ready: ${certCount.recordset[0].cnt.toLocaleString()} certificates`, 'success');
    
    log('');
    log('='.repeat(60));
    log('STEP 1: Data Transforms');
    log('='.repeat(60));
    log('');
    
    for (const transformFile of transforms) {
      await executeSqlFile(pool, transformFile);
      log('');
    }
    
    log('');
    log('='.repeat(60));
    log('TRANSFORMS COMPLETED SUCCESSFULLY', 'success');
    log('='.repeat(60));
    
  } catch (err: any) {
    log(`Pipeline failed: ${err.message}`, 'error');
    console.error(err);
    process.exit(1);
  } finally {
    await pool.close();
    log('Connection closed');
  }
}

main();
