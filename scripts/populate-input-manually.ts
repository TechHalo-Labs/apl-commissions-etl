import * as sql from 'mssql';
import * as path from 'path';
import { loadConfig, getSqlConfig } from './lib/config-loader';
import { executeSQLScript } from './lib/sql-executor';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n🔄 Populating input tables from raw data...\n');
    console.log('   Source: [etl].[raw_certificate_info] (1.5M+ records)');
    console.log('   Target: [etl].[input_certificate_info]');
    console.log('   Process: raw → prep → input (with conformance filtering)\n');
    
    const scriptPath = path.join(__dirname, '../sql/02-input-tables.sql');
    
    const result = await executeSQLScript({
      scriptPath,
      pool,
      config,
      debugMode: false,
      pocMode: false
    });
    
    if (result.success) {
      console.log(`\n✅ Input tables populated in ${result.duration.toFixed(2)}s`);
      console.log(`   Records processed: ${result.recordsAffected?.toLocaleString()}\n`);
    } else {
      console.log(`\n❌ Failed: ${result.error?.message}\n`);
      process.exit(1);
    }
    
    // Verify results
    const counts = await pool.request().query(`
      SELECT 
        (SELECT COUNT(*) FROM [etl].[prep_certificate_info]) as PrepCerts,
        (SELECT COUNT(*) FROM [etl].[input_certificate_info]) as InputCerts,
        (SELECT COUNT(*) FROM [etl].[prep_commission_details]) as PrepComm,
        (SELECT COUNT(*) FROM [etl].[input_commission_details]) as InputComm
    `);
    
    const data = counts.recordset[0];
    console.log('Verification:');
    console.log(`   prep_certificate_info:   ${data.PrepCerts.toLocaleString()} records`);
    console.log(`   input_certificate_info:  ${data.InputCerts.toLocaleString()} records`);
    console.log(`   prep_commission_details: ${data.PrepComm.toLocaleString()} records`);
    console.log(`   input_commission_details: ${data.InputComm.toLocaleString()} records\n`);
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
