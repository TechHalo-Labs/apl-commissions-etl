/**
 * Test what the ingest script sees
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './scripts/lib/config-loader';

async function testIngest() {
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  
  console.log('\n═══════════════════════════════════════════════════════════');
  console.log('TEST INGEST SCRIPT');
  console.log('═══════════════════════════════════════════════════════════\n');
  
  console.log('SOURCE_SCHEMA:', config.database.schemas.source);
  console.log('ETL_SCHEMA:', config.database.schemas.processing);
  console.log('');

  try {
    const pool = await sql.connect(sqlConfig);

    // Test what the ingest script will see
    console.log('Testing source data visibility:\n');
    
    const sourceTest = `
      DECLARE @cert_count BIGINT;
      SELECT @cert_count = COUNT(*) FROM [new_data].[raw_certificate_info];
      SELECT @cert_count AS cert_count;
      
      DECLARE @group_count BIGINT;
      SELECT @group_count = COUNT(*) FROM [new_data].[raw_perf_groups];
      SELECT @group_count AS group_count;
    `;
    
    const result = await pool.request().query(sourceTest);
    console.log('cert_count:', result.recordsets[0][0].cert_count);
    console.log('group_count:', result.recordsets[1][0].group_count);
    
    // Check etl workspace before copy
    console.log('\n\nETL workspace BEFORE manual copy:');
    const beforeQuery = `
      SELECT COUNT(*) AS count FROM [etl].[raw_certificate_info]
    `;
    const beforeResult = await pool.request().query(beforeQuery);
    console.log('etl.raw_certificate_info count:', beforeResult.recordset[0].count);
    
    // Try manual copy
    console.log('\n\nAttempting manual copy...');
    const copyQuery = `
      TRUNCATE TABLE [etl].[raw_certificate_info];
      INSERT INTO [etl].[raw_certificate_info]
      SELECT * FROM [new_data].[raw_certificate_info];
      SELECT @@ROWCOUNT AS rows_copied;
    `;
    
    const copyResult = await pool.request().query(copyQuery);
    console.log('Rows copied:', copyResult.recordsets[1][0].rows_copied);
    
    // Check etl workspace after copy
    console.log('\nETL workspace AFTER manual copy:');
    const afterResult = await pool.request().query(beforeQuery);
    console.log('etl.raw_certificate_info count:', afterResult.recordset[0].count);

    await pool.close();
  } catch (error: any) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

testIngest();
