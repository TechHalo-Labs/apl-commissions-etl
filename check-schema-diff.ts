/**
 * Check schema differences
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './scripts/lib/config-loader';

async function checkSchema() {
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  
  try {
    const pool = await sql.connect(sqlConfig);

    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('SCHEMA COMPARISON');
    console.log('═══════════════════════════════════════════════════════════\n');

    // Get columns from new_data.CertificateInfo
    const newDataQuery = `
      SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = 'new_data' AND TABLE_NAME = 'CertificateInfo'
      ORDER BY ORDINAL_POSITION
    `;
    
    const newDataResult = await pool.request().query(newDataQuery);
    console.log('new_data.CertificateInfo columns:', newDataResult.recordset.length);
    
    // Get columns from etl.raw_certificate_info
    const etlQuery = `
      SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'raw_certificate_info'
      ORDER BY ORDINAL_POSITION
    `;
    
    const etlResult = await pool.request().query(etlQuery);
    console.log('etl.raw_certificate_info columns:', etlResult.recordset.length);
    console.log('');
    
    // Get columns from poc_raw_data.raw_certificate_info for reference
    const pocQuery = `
      SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = 'poc_raw_data' AND TABLE_NAME = 'raw_certificate_info'
      ORDER BY ORDINAL_POSITION
    `;
    
    const pocResult = await pool.request().query(pocQuery);
    console.log('poc_raw_data.raw_certificate_info columns:', pocResult.recordset.length);
    console.log('');

    // Check if poc_raw_data matches etl schema
    console.log('Comparing etl vs poc_raw_data structures...');
    const etlCols = etlResult.recordset.map(r => r.COLUMN_NAME).sort();
    const pocCols = pocResult.recordset.map(r => r.COLUMN_NAME).sort();
    
    const match = JSON.stringify(etlCols) === JSON.stringify(pocCols);
    console.log('Structures match:', match);
    
    if (match) {
      console.log('\n✅ etl.raw_certificate_info matches poc_raw_data.raw_certificate_info');
      console.log('Solution: Use poc_raw_data as SOURCE_SCHEMA instead of new_data');
    } else {
      console.log('\n❌ Schemas do not match');
    }

    await pool.close();
  } catch (error: any) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

checkSchema();
