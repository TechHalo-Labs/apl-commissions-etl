/**
 * Copy raw data from [raw_data] schema to [etl].[raw_*] tables
 * This bridges the gap between the old ETL system (raw_data schema)
 * and the new ETL system (etl.raw_* tables)
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n════════════════════════════════════════════════');
    console.log('  Copying Raw Data: [raw_data] → [etl].[raw_*]');
    console.log('════════════════════════════════════════════════\n');
    
    const mappings = [
      { source: 'raw_certificate_info', target: 'raw_certificate_info' },
      { source: 'raw_commissions_detail', target: 'raw_commissions_detail' },
      { source: 'raw_premiums', target: 'raw_premiums' },
      { source: 'raw_schedule_rates', target: 'raw_schedule_rates' },
      { source: 'raw_individual_brokers', target: 'raw_individual_brokers' },
      { source: 'raw_org_brokers', target: 'raw_org_brokers' },
      { source: 'raw_licenses', target: 'raw_licenses' },
      { source: 'raw_eo_insurance', target: 'raw_eo_insurance' },
      { source: 'raw_perf_groups', target: 'raw_perf_groups' },
      { source: 'raw_fees', target: 'raw_fees' }
    ];
    
    let totalCopied = 0;
    
    for (const map of mappings) {
      try {
        // Check if source table exists and has data
        const check = await pool.request().query(`
          SELECT COUNT(*) as cnt FROM [raw_data].[${map.source}]
        `);
        
        const sourceCount = check.recordset[0].cnt;
        
        if (sourceCount === 0) {
          console.log(`⏭️  ${map.source}: Empty, skipping`);
          continue;
        }
        
        // Clear target table
        await pool.request().query(`TRUNCATE TABLE [etl].[${map.target}]`);
        
        // Copy data
        console.log(`📋 ${map.source}: Copying ${sourceCount.toLocaleString()} records...`);
        
        const startTime = Date.now();
        await pool.request().query(`
          INSERT INTO [etl].[${map.target}]
          SELECT * FROM [raw_data].[${map.source}]
        `);
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`   ✅ Copied in ${duration}s`);
        
        totalCopied += sourceCount;
        
      } catch (e: any) {
        if (e.message.includes('Invalid object name')) {
          console.log(`⚠️  ${map.source}: Table not found in [raw_data], skipping`);
        } else {
          console.log(`❌ ${map.source}: ${e.message}`);
        }
      }
    }
    
    console.log('\n════════════════════════════════════════════════');
    console.log(`✅ Copy Complete: ${totalCopied.toLocaleString()} total records`);
    console.log('════════════════════════════════════════════════\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
