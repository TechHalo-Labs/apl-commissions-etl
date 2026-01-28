/**
 * Copy raw data from [poc_etl] schema to [etl].[raw_*] and [etl].[input_*] tables
 * This is the primary ingest mechanism for the production ETL pipeline
 * 
 * Source: poc_etl schema (contains pre-processed raw data)
 * Target: etl schema (working schema for transforms)
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n════════════════════════════════════════════════');
    console.log('  DATA INGEST: [poc_etl] → [etl]');
    console.log('════════════════════════════════════════════════\n');
    
    // Mappings for raw tables (poc_etl.raw_* → etl.raw_*)
    const rawMappings = [
      { source: 'raw_certificate_info', target: 'raw_certificate_info' },
      { source: 'raw_schedule_rates', target: 'raw_schedule_rates' },
      { source: 'raw_perf_groups', target: 'raw_perf_groups' },
      { source: 'raw_premiums', target: 'raw_premiums' },
      { source: 'raw_individual_brokers', target: 'raw_individual_brokers' },
      { source: 'raw_org_brokers', target: 'raw_org_brokers' },
      { source: 'raw_licenses', target: 'raw_licenses' },
      { source: 'raw_eo_insurance', target: 'raw_eo_insurance' },
      { source: 'raw_commissions_detail', target: 'raw_commissions_detail' },
      { source: 'raw_fees', target: 'raw_fees' }
    ];
    
    let totalCopied = 0;
    const startTime = Date.now();
    
    console.log('Phase 1: Copying raw tables from poc_etl...\n');
    
    for (const map of rawMappings) {
      try {
        // Check if source table exists and has data
        const check = await pool.request().query(`
          SELECT COUNT(*) as cnt FROM [poc_etl].[${map.source}]
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
        
        const copyStartTime = Date.now();
        await pool.request().query(`
          INSERT INTO [etl].[${map.target}]
          SELECT * FROM [poc_etl].[${map.source}]
        `);
        
        const duration = ((Date.now() - copyStartTime) / 1000).toFixed(2);
        console.log(`   ✅ Copied in ${duration}s\n`);
        
        totalCopied += sourceCount;
        
      } catch (e: any) {
        if (e.message.includes('Invalid object name')) {
          console.log(`⚠️  ${map.source}: Table not found in [poc_etl], skipping\n`);
        } else {
          console.log(`❌ ${map.source}: ${e.message}\n`);
          throw e;
        }
      }
    }
    
    console.log('\n────────────────────────────────────────────────');
    console.log(`Phase 1 Complete: ${totalCopied.toLocaleString()} raw records copied`);
    console.log('────────────────────────────────────────────────\n');
    
    // Phase 2: Populate input tables from raw tables
    console.log('Phase 2: Populating input tables...\n');
    
    // input_certificate_info from raw_certificate_info
    console.log('📋 Populating input_certificate_info...');
    await pool.request().query(`TRUNCATE TABLE [etl].[input_certificate_info]`);
    
    const certResult = await pool.request().query(`
      INSERT INTO [etl].[input_certificate_info]
      SELECT * FROM [etl].[raw_certificate_info]
    `);
    
    console.log(`   ✅ ${certResult.rowsAffected[0].toLocaleString()} records\n`);
    
    // Verify unique schedules for reference
    const schedCheck = await pool.request().query(`
      SELECT COUNT(DISTINCT CommissionsSchedule) as unique_schedules
      FROM [etl].[input_certificate_info]
      WHERE CommissionsSchedule IS NOT NULL AND CommissionsSchedule != ''
    `);
    
    console.log(`   📊 ${schedCheck.recordset[0].unique_schedules} unique schedules referenced\n`);
    
    console.log('────────────────────────────────────────────────');
    console.log('Phase 2 Complete: Input tables populated');
    console.log('────────────────────────────────────────────────\n');
    
    const totalDuration = ((Date.now() - startTime) / 1000).toFixed(2);
    
    console.log('════════════════════════════════════════════════');
    console.log(`✅ INGEST COMPLETE: ${totalDuration}s total`);
    console.log('════════════════════════════════════════════════\n');
    
    console.log('Next steps:');
    console.log('  1. Run transforms: npx tsx scripts/run-pipeline.ts --skip-ingest');
    console.log('  2. Or full pipeline: npx tsx scripts/run-pipeline.ts');
    console.log('');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
