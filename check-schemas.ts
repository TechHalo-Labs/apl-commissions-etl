/**
 * Check what tables exist in new_data and poc_raw_data schemas
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './scripts/lib/config-loader';

async function checkSchemas() {
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  
  try {
    const pool = await sql.connect(sqlConfig);

    console.log('\nTables in new_data schema:');
    console.log('═══════════════════════════════════════════════════════════\n');
    
    const newDataQuery = `
      SELECT TABLE_NAME 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_SCHEMA = 'new_data' 
      AND TABLE_TYPE = 'BASE TABLE'
      ORDER BY TABLE_NAME
    `;
    
    const newDataResult = await pool.request().query(newDataQuery);
    newDataResult.recordset.forEach(row => console.log(`  - ${row.TABLE_NAME}`));

    console.log('\n\nTables in poc_raw_data schema:');
    console.log('═══════════════════════════════════════════════════════════\n');
    
    const pocQuery = `
      SELECT TABLE_NAME 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_SCHEMA = 'poc_raw_data' 
      AND TABLE_TYPE = 'BASE TABLE'
      ORDER BY TABLE_NAME
    `;
    
    const pocResult = await pool.request().query(pocQuery);
    pocResult.recordset.forEach(row => console.log(`  - ${row.TABLE_NAME}`));

    await pool.close();
  } catch (error: any) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

checkSchemas();
