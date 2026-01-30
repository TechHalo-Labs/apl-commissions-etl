/**
 * Debug script to check premium data availability
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);

  try {
    console.log('Checking raw_premiums availability...\n');

    // Check poc_etl.raw_premiums
    try {
      const pocResult = await pool.request().query(`
        SELECT TOP 5 
            'poc_etl.raw_premiums' AS SchemaTable,
            Policy,
            Amount,
            DatePost,
            DatePaidTo
        FROM poc_etl.raw_premiums
        WHERE Policy IS NOT NULL
        ORDER BY Policy;
      `);
      console.log(`✅ poc_etl.raw_premiums exists: ${pocResult.recordset.length} sample rows`);
      if (pocResult.recordset.length > 0) {
        console.log('Sample:', pocResult.recordset[0]);
      }
    } catch (e: any) {
      console.log(`❌ poc_etl.raw_premiums: ${e.message}`);
    }

    // Check etl.raw_premiums
    try {
      const etlResult = await pool.request().query(`
        SELECT TOP 5 
            'etl.raw_premiums' AS SchemaTable,
            Policy,
            Amount,
            DatePost,
            DatePaidTo
        FROM etl.raw_premiums
        WHERE Policy IS NOT NULL
        ORDER BY Policy;
      `);
      console.log(`✅ etl.raw_premiums exists: ${etlResult.recordset.length} sample rows`);
      if (etlResult.recordset.length > 0) {
        console.log('Sample:', etlResult.recordset[0]);
      }
    } catch (e: any) {
      console.log(`❌ etl.raw_premiums: ${e.message}`);
    }

    // Check certificates for group 15185
    console.log('\nChecking certificates for group 15185...');
    const certResult = await pool.request().query(`
      SELECT TOP 10
          CertificateId,
          GroupId,
          CertStatus,
          RecStatus
      FROM new_data.CertificateInfo
      WHERE GroupId = '15185'
        AND CertStatus = 'A'
        AND RecStatus = 'A';
    `);
    console.log(`Found ${certResult.recordset.length} certificates`);
    if (certResult.recordset.length > 0) {
      const certIds = certResult.recordset.map((r: any) => r.CertificateId.toString()).join(',');
      console.log(`Sample CertificateIds: ${certIds.substring(0, 100)}...`);
    }

    // Check if premiums exist for these certificates in poc_etl
    if (certResult.recordset.length > 0) {
      const sampleCertId = certResult.recordset[0].CertificateId;
      console.log(`\nChecking premiums for certificate ${sampleCertId}...`);
      
      const premCheck = await pool.request()
        .input('certId', sql.BigInt, BigInt(sampleCertId))
        .query(`
          SELECT TOP 10
              Policy,
              Amount,
              DatePost,
              DatePaidTo
          FROM poc_etl.raw_premiums
          WHERE TRY_CAST(Policy AS BIGINT) = @certId;
        `);
      console.log(`Found ${premCheck.recordset.length} premium transactions for certificate ${sampleCertId}`);
      
      if (premCheck.recordset.length === 0) {
        // Try without cast
        const premCheck2 = await pool.request()
          .input('certId', sql.NVarChar, sampleCertId.toString())
          .query(`
            SELECT TOP 10
                Policy,
                Amount,
                DatePost,
                DatePaidTo
            FROM poc_etl.raw_premiums
            WHERE Policy = @certId;
          `);
        console.log(`Found ${premCheck2.recordset.length} premium transactions (string match)`);
      }
    }

    // Check total count in raw_premiums
    console.log('\nChecking total counts...');
    const countResult = await pool.request().query(`
      SELECT 
          COUNT(*) AS TotalRows,
          COUNT(DISTINCT Policy) AS UniquePolicies
      FROM poc_etl.raw_premiums
      WHERE Policy IS NOT NULL AND LTRIM(RTRIM(Policy)) <> '';
    `);
    console.log(`Total rows: ${countResult.recordset[0].TotalRows}`);
    console.log(`Unique policies: ${countResult.recordset[0].UniquePolicies}`);

  } catch (error: any) {
    console.error('Error:', error.message);
    console.error(error.stack);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
