/**
 * Copy full raw data from [raw_data] to [poc_raw_data]
 * This provides POC schemas with complete dataset for testing
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n════════════════════════════════════════════════');
    console.log('  Copying Full Dataset: [raw_data] → [poc_raw_data]');
    console.log('════════════════════════════════════════════════\n');
    
    const mappings = [
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
    
    for (const table of mappings) {
      try {
        // Check source count
        const check = await pool.request().query(`
          SELECT COUNT(*) as cnt FROM [raw_data].[${table}]
        `);
        
        const sourceCount = check.recordset[0].cnt;
        
        if (sourceCount === 0) {
          console.log(`⏭️  ${table}: Empty, skipping`);
          continue;
        }
        
        // Clear target table
        await pool.request().query(`TRUNCATE TABLE [poc_raw_data].[${table}]`);
        
        // Copy data
        console.log(`📋 ${table}: Copying ${sourceCount.toLocaleString()} records...`);
        
        const startTime = Date.now();
        await pool.request().query(`
          INSERT INTO [poc_raw_data].[${table}]
          SELECT * FROM [raw_data].[${table}]
        `);
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`   ✅ Copied in ${duration}s`);
        
        totalCopied += sourceCount;
        
      } catch (e: any) {
        console.log(`❌ ${table}: ${e.message}`);
      }
    }
    
    console.log('\n════════════════════════════════════════════════');
    console.log(`✅ Full Dataset Copied: ${totalCopied.toLocaleString()} records`);
    console.log('════════════════════════════════════════════════\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
