#!/usr/bin/env tsx
/**
 * Audit column mapping between new_data schema and etl.raw_* tables
 * Compares actual column names to identify mismatches
 */

import * as sql from 'mssql';
import { readFile } from 'fs/promises';
import { resolve } from 'path';

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
      console.error('❌ Invalid $SQLSERVER connection string');
      process.exit(1);
    }
    return {
      server: parsed.server!,
      database: parsed.database!,
      user: parsed.user!,
      password: parsed.password!,
      options: {
        encrypt: parsed.options?.encrypt ?? true,
        trustServerCertificate: parsed.options?.trustServerCertificate ?? true,
      },
      requestTimeout: 30000,
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
    requestTimeout: 30000,
    connectionTimeout: 30000,
  };
}

async function getColumns(pool: sql.ConnectionPool, schema: string, table: string): Promise<string[]> {
  try {
    const result = await pool.request().query(`
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME = '${table}'
      ORDER BY ORDINAL_POSITION
    `);
    return result.recordset.map((row: any) => row.COLUMN_NAME);
  } catch (err: any) {
    if (err.message.includes('Invalid object name')) {
      return []; // Table doesn't exist
    }
    throw err;
  }
}

interface TableMapping {
  newDataTable: string;
  rawTable: string;
}

const mappings: TableMapping[] = [
  { newDataTable: 'CertificateInfo', rawTable: 'raw_certificate_info' },
  { newDataTable: 'CommissionsDetail', rawTable: 'raw_commissions_detail' },
  { newDataTable: 'IndividualRoster', rawTable: 'raw_individual_brokers' },
  { newDataTable: 'OrganizationRoster', rawTable: 'raw_org_brokers' },
  { newDataTable: 'BrokerLicenses', rawTable: 'raw_licenses' },
  { newDataTable: 'BrokerEO', rawTable: 'raw_eo_insurance' },
  { newDataTable: 'PerfScheduleModel', rawTable: 'raw_schedule_rates' },
  { newDataTable: 'PerfGroupModel', rawTable: 'raw_perf_groups' },
];

async function main() {
  const config = getSqlConfig();
  const pool = await sql.connect(config);
  
  console.log('\n' + '='.repeat(80));
  console.log('COLUMN MAPPING AUDIT: new_data → etl.raw_*');
  console.log('='.repeat(80));
  console.log(`Server: ${config.server}`);
  console.log(`Database: ${config.database}`);
  console.log('');
  
  let allMatch = true;
  
  for (const { newDataTable, rawTable } of mappings) {
    console.log(`\n${'─'.repeat(80)}`);
    console.log(`${newDataTable} → ${rawTable}`);
    console.log('─'.repeat(80));
    
    const newDataColumns = await getColumns(pool, 'new_data', newDataTable);
    const rawColumns = await getColumns(pool, 'etl', rawTable);
    
    if (newDataColumns.length === 0) {
      console.log(`  ⚠️  Table [new_data].[${newDataTable}] does not exist`);
      continue;
    }
    
    if (rawColumns.length === 0) {
      console.log(`  ⚠️  Table [etl].[${rawTable}] does not exist`);
      continue;
    }
    
    // Note: new_data tables have an extra "Id" column (IDENTITY)
    const newDataDataColumns = newDataColumns.filter(c => c !== 'Id');
    
    // Compare columns
    const missingInRaw = newDataDataColumns.filter(c => !rawColumns.includes(c));
    const missingInNewData = rawColumns.filter(c => !newDataDataColumns.includes(c));
    const common = newDataDataColumns.filter(c => rawColumns.includes(c));
    
    console.log(`  new_data columns: ${newDataDataColumns.length} (excluding Id)`);
    console.log(`  raw columns: ${rawColumns.length}`);
    console.log(`  Common columns: ${common.length}`);
    
    if (missingInRaw.length > 0) {
      console.log(`  ❌ Missing in raw: ${missingInRaw.join(', ')}`);
      allMatch = false;
    }
    
    if (missingInNewData.length > 0) {
      console.log(`  ❌ Missing in new_data: ${missingInNewData.join(', ')}`);
      allMatch = false;
    }
    
    if (missingInRaw.length === 0 && missingInNewData.length === 0) {
      console.log(`  ✅ All columns match!`);
    } else {
      console.log(`\n  Common columns:`);
      common.forEach(col => console.log(`    • ${col}`));
    }
  }
  
  // Check premiums separately
  console.log(`\n${'─'.repeat(80)}`);
  console.log(`premiums → raw_premiums`);
  console.log('─'.repeat(80));
  const premiumNewData = await getColumns(pool, 'new_data', 'premiums');
  const premiumRaw = await getColumns(pool, 'etl', 'raw_premiums');
  
  if (premiumNewData.length === 0) {
    console.log(`  ⚠️  Table [new_data].[premiums] does not exist`);
  } else if (premiumRaw.length === 0) {
    console.log(`  ⚠️  Table [etl].[raw_premiums] does not exist`);
  } else {
    const premiumNewDataData = premiumNewData.filter(c => c !== 'Id');
    const missingInRaw = premiumNewDataData.filter(c => !premiumRaw.includes(c));
    const missingInNewData = premiumRaw.filter(c => !premiumNewDataData.includes(c));
    
    if (missingInRaw.length === 0 && missingInNewData.length === 0) {
      console.log(`  ✅ All columns match!`);
    } else {
      if (missingInRaw.length > 0) {
        console.log(`  ❌ Missing in raw: ${missingInRaw.join(', ')}`);
        allMatch = false;
      }
      if (missingInNewData.length > 0) {
        console.log(`  ❌ Missing in new_data: ${missingInNewData.join(', ')}`);
        allMatch = false;
      }
    }
  }
  
  console.log('\n' + '='.repeat(80));
  if (allMatch) {
    console.log('✅ ALL COLUMNS MATCH - Direct SELECT * copy will work');
  } else {
    console.log('❌ COLUMN MISMATCHES DETECTED - Need explicit column mapping');
  }
  console.log('='.repeat(80));
  console.log('');
  
  await pool.close();
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
