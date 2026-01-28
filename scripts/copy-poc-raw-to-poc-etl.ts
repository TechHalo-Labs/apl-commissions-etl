/**
 * Copy raw tables from [poc_raw_data] to [poc_etl]
 * The transform scripts expect raw_* tables in the processing schema
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n════════════════════════════════════════════════');
    console.log('  Copying: [poc_raw_data].[raw_*] → [poc_etl].[raw_*]');
    console.log('════════════════════════════════════════════════\n');
    
    const tables = [
      'raw_certificate_info',
      'raw_commissions_detail',
      'raw_premiums',
      'raw_schedule_rates',
      'raw_individual_brokers',
      'raw_org_brokers',
      'raw_licenses',
      'raw_eo_insurance',
      'raw_perf_groups',
      'raw_fees'
    ];
    
    let totalCopied = 0;
    
    for (const table of tables) {
      try {
        const check = await pool.request().query(`
          SELECT COUNT(*) as cnt FROM [poc_raw_data].[${table}]
        `);
        
        const sourceCount = check.recordset[0].cnt;
        
        if (sourceCount === 0) {
          console.log(`⏭️  ${table}: Empty, skipping`);
          continue;
        }
        
        // Clear and copy
        console.log(`📋 ${table}: Copying ${sourceCount.toLocaleString()} records...`);
        
        const startTime = Date.now();
        await pool.request().query(`TRUNCATE TABLE [poc_etl].[${table}]`);
        await pool.request().query(`
          INSERT INTO [poc_etl].[${table}]
          SELECT * FROM [poc_raw_data].[${table}]
        `);
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`   ✅ Copied in ${duration}s`);
        
        totalCopied += sourceCount;
        
      } catch (e: any) {
        if (e.message.includes('Invalid object name')) {
          console.log(`⚠️  ${table}: Not found, skipping`);
        } else {
          console.log(`❌ ${table}: ${e.message}`);
        }
      }
    }
    
    console.log('\n════════════════════════════════════════════════');
    console.log(`✅ Copy Complete: ${totalCopied.toLocaleString()} records`);
    console.log('   Raw tables now in [poc_etl] ready for transforms');
    console.log('════════════════════════════════════════════════\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
