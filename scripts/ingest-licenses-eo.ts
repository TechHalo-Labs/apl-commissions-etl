#!/usr/bin/env tsx
/**
 * Fast bulk ingest of BrokerLicenses and BrokerEO from CSV files
 * Handles BOM (Byte Order Mark) in CSV headers
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import { parse } from 'csv-parse/sync';

function parseConnectionString(connStr: string) {
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
    requestTimeout: 600000,
  };
}

async function main() {
  const connStr = process.env.SQLSERVER;
  if (!connStr) {
    throw new Error('SQLSERVER environment variable not set');
  }
  
  const config = parseConnectionString(connStr);
  const pool = await sql.connect(config);
  
  console.log('\n📋 Step 1: Truncate tables');
  console.log('='.repeat(80));
  await pool.request().query(`TRUNCATE TABLE [new_data].[BrokerLicenses]`);
  await pool.request().query(`TRUNCATE TABLE [new_data].[BrokerEO]`);
  console.log('✅ Tables truncated');
  
  console.log('\n📋 Step 2: Load BrokerLicenses from CSV');
  console.log('='.repeat(80));
  const licFile = process.env.HOME + '/Downloads/newdata/BrokerLicenseExtract_20260116.csv';
  let licContent = fs.readFileSync(licFile, 'utf8');
  
  // Remove BOM if present
  if (licContent.charCodeAt(0) === 0xFEFF) {
    licContent = licContent.substring(1);
  }
  
  const licRecords = parse(licContent, { columns: true, skip_empty_lines: true });
  console.log(`Found ${licRecords.length} license records`);
  
  // Build bulk insert using table-valued parameter
  const table = new sql.Table('[new_data].[BrokerLicenses]');
  table.columns.add('PartyUniqueId', sql.NVarChar(sql.MAX), { nullable: true });
  table.columns.add('StateCode', sql.NVarChar(sql.MAX), { nullable: true });
  table.columns.add('CurrentStatus', sql.NVarChar(sql.MAX), { nullable: true });
  table.columns.add('LicenseCode', sql.NVarChar(sql.MAX), { nullable: true });
  table.columns.add('LicenseEffectiveDate', sql.NVarChar(sql.MAX), { nullable: true });
  table.columns.add('LicenseExpirationDate', sql.NVarChar(sql.MAX), { nullable: true });
  table.columns.add('IsResidenceLicense', sql.NVarChar(sql.MAX), { nullable: true });
  table.columns.add('LicenseNumber', sql.NVarChar(sql.MAX), { nullable: true });
  table.columns.add('ApplicableCounty', sql.NVarChar(sql.MAX), { nullable: true });
  
  for (const rec of licRecords) {
    table.rows.add(
      rec.PartyUniqueId || null,
      rec.StateCode || null,
      rec.CurrentStatus || null,
      rec.LicenseCode || null,
      rec.LicenseEffectiveDate || null,
      rec.LicenseExpirationDate || null,
      rec.IsResidenceLicense || null,
      rec.LicenseNumber || null,
      rec.ApplicableCounty || null
    );
  }
  
  const request = new sql.Request(pool);
  await request.bulk(table);
  console.log(`✅ Inserted ${licRecords.length} license records`);
  
  console.log('\n📋 Step 3: Load BrokerEO from CSV');
  console.log('='.repeat(80));
  const eoFile = process.env.HOME + '/Downloads/newdata/BrokerEO_20260116.csv';
  let eoContent = fs.readFileSync(eoFile, 'utf8');
  
  // Remove BOM if present
  if (eoContent.charCodeAt(0) === 0xFEFF) {
    eoContent = eoContent.substring(1);
  }
  
  const eoRecords = parse(eoContent, { columns: true, skip_empty_lines: true });
  console.log(`Found ${eoRecords.length} E&O records`);
  
  const eoTable = new sql.Table('[new_data].[BrokerEO]');
  eoTable.columns.add('PartyUniqueId', sql.NVarChar(sql.MAX), { nullable: true });
  eoTable.columns.add('CarrierName', sql.NVarChar(sql.MAX), { nullable: true });
  eoTable.columns.add('PolicyId', sql.NVarChar(sql.MAX), { nullable: true });
  eoTable.columns.add('FromDate', sql.NVarChar(sql.MAX), { nullable: true });
  eoTable.columns.add('ToDate', sql.NVarChar(sql.MAX), { nullable: true });
  eoTable.columns.add('DeductibleAmount', sql.NVarChar(sql.MAX), { nullable: true });
  eoTable.columns.add('ClaimMaxAmount', sql.NVarChar(sql.MAX), { nullable: true });
  eoTable.columns.add('AnnualMaxAmount', sql.NVarChar(sql.MAX), { nullable: true });
  eoTable.columns.add('PolicyMaxAmount', sql.NVarChar(sql.MAX), { nullable: true });
  eoTable.columns.add('LiabilityLimit', sql.NVarChar(sql.MAX), { nullable: true });
  
  for (const rec of eoRecords) {
    eoTable.rows.add(
      rec.PartyUniqueId || null,
      rec.CarrierName || null,
      rec.PolicyId || null,
      rec.FromDate || null,
      rec.ToDate || null,
      rec.DeductibleAmount || null,
      rec.ClaimMaxAmount || null,
      rec.AnnualMaxAmount || null,
      rec.PolicyMaxAmount || null,
      rec.LiabilityLimit || null
    );
  }
  
  const eoRequest = new sql.Request(pool);
  await eoRequest.bulk(eoTable);
  console.log(`✅ Inserted ${eoRecords.length} E&O records`);
  
  console.log('\n📊 Step 4: Verify data');
  console.log('='.repeat(80));
  const verify = await pool.request().query(`
    SELECT 'BrokerLicenses' as [Table], 
           COUNT(*) as [Total],
           COUNT(DISTINCT PartyUniqueId) as [Distinct PartyUniqueId],
           SUM(CASE WHEN PartyUniqueId IS NOT NULL AND PartyUniqueId != '' THEN 1 ELSE 0 END) as [Non-NULL PartyUniqueId]
    FROM [new_data].[BrokerLicenses]
    UNION ALL
    SELECT 'BrokerEO',
           COUNT(*),
           COUNT(DISTINCT PartyUniqueId),
           SUM(CASE WHEN PartyUniqueId IS NOT NULL AND PartyUniqueId != '' THEN 1 ELSE 0 END)
    FROM [new_data].[BrokerEO]
  `);
  console.table(verify.recordset);
  
  console.log('\n📋 Sample loaded data:');
  const sample = await pool.request().query(`
    SELECT TOP 5 PartyUniqueId, StateCode, LicenseCode
    FROM [new_data].[BrokerLicenses]
    WHERE PartyUniqueId IS NOT NULL AND PartyUniqueId != ''
  `);
  console.table(sample.recordset);
  
  console.log('\n✅ CSV ingestion complete!');
  
  await pool.close();
}

main().catch(err => {
  console.error('\n❌ Failed:', err);
  process.exit(1);
});
