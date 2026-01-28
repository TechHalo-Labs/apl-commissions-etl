import * as sql from 'mssql';
import * as path from 'path';
import { loadConfig, getSqlConfig } from './lib/config-loader';
import { executeSQLScript } from './lib/sql-executor';

async function main() {
  // Load POC config
  const config = loadConfig({}, 'appsettings.poc.json');
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n🔄 Populating POC input tables...\n');
    console.log('   Config: appsettings.poc.json');
    console.log('   Source: [poc_raw_data].[raw_certificate_info] (1.5M+ records)');
    console.log('   Target: [poc_etl].[input_certificate_info]');
    console.log(`   POC Mode: ${config.database.pocMode}`);
    console.log(`   Debug Mode: ${config.debugMode.enabled}\n`);
    
    const scriptPath = path.join(__dirname, '../sql/02-input-tables.sql');
    
    const result = await executeSQLScript({
      scriptPath,
      pool,
      config,
      debugMode: false,
      pocMode: config.database.pocMode || false
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
        (SELECT COUNT(*) FROM [poc_etl].[prep_certificate_info]) as PrepCerts,
        (SELECT COUNT(*) FROM [poc_etl].[input_certificate_info]) as InputCerts,
        (SELECT COUNT(*) FROM [poc_etl].[prep_commission_details]) as PrepComm,
        (SELECT COUNT(*) FROM [poc_etl].[input_commission_details]) as InputComm
    `);
    
    const data = counts.recordset[0];
    console.log('Verification:');
    console.log(`   [poc_etl].prep_certificate_info:   ${data.PrepCerts.toLocaleString()} records`);
    console.log(`   [poc_etl].input_certificate_info:  ${data.InputCerts.toLocaleString()} records`);
    console.log(`   [poc_etl].prep_commission_details: ${data.PrepComm.toLocaleString()} records`);
    console.log(`   [poc_etl].input_commission_details: ${data.InputComm.toLocaleString()} records\n`);
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
