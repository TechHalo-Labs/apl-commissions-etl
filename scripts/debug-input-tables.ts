#!/usr/bin/env tsx
/**
 * Debug script to populate input tables from raw tables
 * Handles 'NULL' string to actual NULL conversion
 */

import * as sql from 'mssql';

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
  };
}

function getSqlConfig(): sql.config {
  const connStr = process.env.SQLSERVER;
  
  if (connStr) {
    return {
      ...parseConnectionString(connStr),
      pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30000,
      },
      requestTimeout: 300000, // 5 minutes
    } as sql.config;
  }
  
  // Fall back to individual environment variables
  const server = process.env.SQLSERVER_HOST;
  const database = process.env.SQLSERVER_DATABASE;
  const user = process.env.SQLSERVER_USER;
  const password = process.env.SQLSERVER_PASSWORD;
  
  if (!server || !database || !user || !password) {
    console.error('❌ Missing required environment variables');
    console.error('Either set SQLSERVER connection string, or all of:');
    console.error('  SQLSERVER_HOST, SQLSERVER_DATABASE, SQLSERVER_USER, SQLSERVER_PASSWORD');
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
    },
    requestTimeout: 300000,
  };
}

async function main() {
  console.log('🔧 DEBUG: Input Table Population');
  console.log('='.repeat(60));
  
  const config = getSqlConfig();
  const pool = await sql.connect(config);
  
  try {
    // Step 1: Check raw table data
    console.log('\n📊 Step 1: Check raw data...');
    const rawCertCount = await pool.request().query(`SELECT COUNT(*) as cnt FROM [etl].[raw_certificate_info]`);
    console.log(`  raw_certificate_info: ${rawCertCount.recordset[0].cnt.toLocaleString()} rows`);
    
    const rawCommCount = await pool.request().query(`SELECT COUNT(*) as cnt FROM [etl].[raw_commissions_detail]`);
    console.log(`  raw_commissions_detail: ${rawCommCount.recordset[0].cnt.toLocaleString()} rows`);
    
    // Step 2: Sample raw data to see what 'NULL' looks like
    console.log('\n🔍 Step 2: Sample raw_certificate_info data...');
    const sample = await pool.request().query(`
      SELECT TOP 3 
        CertificateId, 
        CertSplitSeq, 
        CertSplitPercent,
        CertStatus,
        RecStatus
      FROM [etl].[raw_certificate_info]
    `);
    console.log('  Sample rows:');
    sample.recordset.forEach((row, i) => {
      console.log(`    Row ${i + 1}:`, JSON.stringify(row));
    });
    
    // Step 3: Check for literal 'NULL' strings
    console.log('\n🔍 Step 3: Check for literal NULL strings...');
    const nullCheck = await pool.request().query(`
      SELECT 
        COUNT(CASE WHEN CertificateId = 'NULL' THEN 1 END) as CertId_NULL,
        COUNT(CASE WHEN CertSplitSeq = 'NULL' THEN 1 END) as SplitSeq_NULL,
        COUNT(CASE WHEN CertSplitPercent = 'NULL' THEN 1 END) as SplitPct_NULL
      FROM [etl].[raw_certificate_info]
    `);
    console.log('  Literal NULL string counts:', nullCheck.recordset[0]);
    
    // Step 4: Truncate input tables
    console.log('\n🗑️  Step 4: Truncate input tables...');
    await pool.request().query(`TRUNCATE TABLE [etl].[input_certificate_info]`);
    console.log('  ✅ input_certificate_info truncated');
    
    await pool.request().query(`TRUNCATE TABLE [etl].[input_commission_details]`);
    console.log('  ✅ input_commission_details truncated');
    
    // Step 5: Try simple insert (will likely fail)
    console.log('\n📥 Step 5: Try simple INSERT (expecting failure)...');
    try {
      await pool.request().query(`
        INSERT INTO [etl].[input_certificate_info]
        SELECT TOP 10 * FROM [etl].[raw_certificate_info]
        WHERE LTRIM(RTRIM(CertStatus)) = 'A' AND LTRIM(RTRIM(RecStatus)) = 'A'
      `);
      console.log('  ✅ Simple insert worked! (unexpected)');
    } catch (err: any) {
      console.log(`  ❌ Expected error: ${err.message}`);
      console.log('  This confirms we need type conversion');
    }
    
    // Step 6: Get column info for input_certificate_info
    console.log('\n📋 Step 6: Get column schemas...');
    const inputCols = await pool.request().query(`
      SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'input_certificate_info'
      ORDER BY ORDINAL_POSITION
    `);
    
    console.log('  input_certificate_info columns:');
    const intColumns: string[] = [];
    const decimalColumns: string[] = [];
    const dateColumns: string[] = [];
    
    inputCols.recordset.forEach((col: any) => {
      console.log(`    ${col.COLUMN_NAME}: ${col.DATA_TYPE} (nullable: ${col.IS_NULLABLE})`);
      if (col.DATA_TYPE === 'int' || col.DATA_TYPE === 'bigint') {
        intColumns.push(col.COLUMN_NAME);
      } else if (col.DATA_TYPE === 'decimal' || col.DATA_TYPE === 'numeric') {
        decimalColumns.push(col.COLUMN_NAME);
      } else if (col.DATA_TYPE === 'date' || col.DATA_TYPE === 'datetime') {
        dateColumns.push(col.COLUMN_NAME);
      }
    });
    
    console.log(`\n  Found ${intColumns.length} INT columns: ${intColumns.slice(0, 5).join(', ')}...`);
    console.log(`  Found ${decimalColumns.length} DECIMAL columns: ${decimalColumns.slice(0, 5).join(', ')}...`);
    console.log(`  Found ${dateColumns.length} DATE columns: ${dateColumns.slice(0, 5).join(', ')}...`);
    
    // Step 7: Build conversion query
    console.log('\n🔧 Step 7: Build conversion query...');
    const rawCols = await pool.request().query(`
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'raw_certificate_info'
      ORDER BY ORDINAL_POSITION
    `);
    
    // Find which INT columns are NOT NULL
    const notNullIntCols = inputCols.recordset
      .filter((col: any) => (col.DATA_TYPE === 'int' || col.DATA_TYPE === 'bigint') && col.IS_NULLABLE === 'NO')
      .map((col: any) => col.COLUMN_NAME);
    
    console.log(`  NOT NULL INT columns: ${notNullIntCols.join(', ')}`);
    
    const selectList = rawCols.recordset.map((col: any) => {
      const colName = col.COLUMN_NAME;
      
      if (intColumns.includes(colName)) {
        // Use 0 as default for NOT NULL INT columns
        const defaultValue = notNullIntCols.includes(colName) ? '0' : 'NULL';
        return `TRY_CAST(CASE WHEN LTRIM(RTRIM(ISNULL([${colName}], ''))) IN ('', 'NULL') THEN ${defaultValue} ELSE [${colName}] END AS INT) AS [${colName}]`;
      } else if (decimalColumns.includes(colName)) {
        return `TRY_CAST(CASE WHEN LTRIM(RTRIM(ISNULL([${colName}], ''))) IN ('', 'NULL') THEN NULL ELSE [${colName}] END AS DECIMAL(18,4)) AS [${colName}]`;
      } else if (dateColumns.includes(colName)) {
        return `TRY_CAST(CASE WHEN LTRIM(RTRIM(ISNULL([${colName}], ''))) IN ('', 'NULL') THEN NULL ELSE [${colName}] END AS DATE) AS [${colName}]`;
      } else {
        return `CASE WHEN LTRIM(RTRIM(ISNULL([${colName}], ''))) IN ('', 'NULL') THEN NULL ELSE [${colName}] END AS [${colName}]`;
      }
    }).join(',\n      ');
    
    console.log('  Conversion query built with type safety');
    
    // Step 8: Try conversion insert
    console.log('\n📥 Step 8: Try conversion INSERT...');
    const insertQuery = `
      INSERT INTO [etl].[input_certificate_info]
      SELECT 
      ${selectList}
      FROM [etl].[raw_certificate_info]
      WHERE LTRIM(RTRIM(CertStatus)) = 'A' 
        AND LTRIM(RTRIM(RecStatus)) = 'A'
    `;
    
    const result = await pool.request().query(insertQuery);
    const rowsInserted = result.rowsAffected?.[0] ?? 0;
    console.log(`  ✅ Success! Inserted ${rowsInserted.toLocaleString()} rows`);
    
    // Step 9: Verify
    console.log('\n✅ Step 9: Verify input_certificate_info...');
    const verifyCount = await pool.request().query(`SELECT COUNT(*) as cnt FROM [etl].[input_certificate_info]`);
    console.log(`  input_certificate_info now has ${verifyCount.recordset[0].cnt.toLocaleString()} rows`);
    
    console.log('\n' + '='.repeat(60));
    console.log('✅ DEBUG COMPLETE - Input table population successful!');
    console.log('='.repeat(60));
    
  } catch (err: any) {
    console.error('\n❌ ERROR:', err.message);
    console.error(err);
    process.exit(1);
  } finally {
    await pool.close();
  }
}

main();
