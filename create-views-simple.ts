/**
 * Create raw_* views in new_data schema - Simple approach
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './scripts/lib/config-loader';

async function createViews() {
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  
  console.log('\n═══════════════════════════════════════════════════════════');
  console.log('CREATE RAW VIEWS IN new_data SCHEMA');
  console.log('═══════════════════════════════════════════════════════════\n');

  try {
    const pool = await sql.connect(sqlConfig);
    
    // Drop and create each view individually
    const views = [
      {
        name: 'raw_certificate_info',
        source: 'CertificateInfo',
        sourceSchema: 'new_data'
      },
      {
        name: 'raw_perf_groups',
        source: 'raw_perf_groups',
        sourceSchema: 'poc_raw_data'
      },
      {
        name: 'raw_schedule_rates',
        source: 'raw_schedule_rates',
        sourceSchema: 'poc_raw_data'
      },
      {
        name: 'raw_premiums',
        source: 'raw_premiums',
        sourceSchema: 'poc_raw_data'
      },
      {
        name: 'raw_individual_brokers',
        source: 'raw_individual_brokers',
        sourceSchema: 'poc_raw_data'
      },
      {
        name: 'raw_org_brokers',
        source: 'raw_org_brokers',
        sourceSchema: 'poc_raw_data'
      },
      {
        name: 'raw_licenses',
        source: 'raw_licenses',
        sourceSchema: 'poc_raw_data'
      },
      {
        name: 'raw_eo_insurance',
        source: 'raw_eo_insurance',
        sourceSchema: 'poc_raw_data'
      }
    ];
    
    for (const view of views) {
      console.log(`Creating new_data.${view.name}...`);
      
      // Drop if exists
      await pool.request().query(`
        IF OBJECT_ID('new_data.${view.name}', 'V') IS NOT NULL
          DROP VIEW new_data.${view.name}
      `);
      
      // Create view
      await pool.request().query(`
        CREATE VIEW new_data.${view.name} AS
        SELECT * FROM ${view.sourceSchema}.${view.source}
      `);
      
      console.log(`  ✅ Created: new_data.${view.name} → ${view.sourceSchema}.${view.source}`);
    }

    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('VERIFICATION');
    console.log('═══════════════════════════════════════════════════════════\n');

    const verifyQuery = `
      SELECT 'raw_certificate_info' AS ViewName, COUNT(*) AS CountRows FROM new_data.raw_certificate_info
      UNION ALL SELECT 'raw_perf_groups', COUNT(*) FROM new_data.raw_perf_groups
      UNION ALL SELECT 'raw_schedule_rates', COUNT(*) FROM new_data.raw_schedule_rates
      UNION ALL SELECT 'raw_premiums', COUNT(*) FROM new_data.raw_premiums
      UNION ALL SELECT 'raw_individual_brokers', COUNT(*) FROM new_data.raw_individual_brokers
      UNION ALL SELECT 'raw_org_brokers', COUNT(*) FROM new_data.raw_org_brokers
    `;
    
    const verifyResult = await pool.request().query(verifyQuery);
    console.table(verifyResult.recordset);

    console.log('\n✅ All views created and verified!\n');

    await pool.close();
  } catch (error: any) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

createViews();
