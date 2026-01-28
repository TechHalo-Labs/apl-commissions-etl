/**
 * Verify Staging Data After ETL Run
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './scripts/lib/config-loader';

async function verifyStaging() {
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  
  console.log('\n═══════════════════════════════════════════════════════════');
  console.log('ETL STAGING VERIFICATION REPORT');
  console.log('═══════════════════════════════════════════════════════════\n');

  try {
    const pool = await sql.connect(sqlConfig);

    // 1. Check source data
    console.log('1. SOURCE DATA');
    console.log('─────────────────────────────────────────────────────────');
    
    const sourceQuery = `
      SELECT 'Source: new_data.CertificateInfo' AS TableName, COUNT(*) AS CountRows 
      FROM new_data.CertificateInfo
      UNION ALL SELECT 'Source: poc_raw_data.raw_perf_groups', COUNT(*) 
      FROM poc_raw_data.raw_perf_groups
    `;
    
    const sourceResult = await pool.request().query(sourceQuery);
    console.table(sourceResult.recordset);

    // 2. Check ETL workspace
    console.log('\n2. ETL WORKSPACE');
    console.log('─────────────────────────────────────────────────────────');
    
    const etlQuery = `
      SELECT 'etl.raw_certificate_info' AS TableName, COUNT(*) AS CountRows 
      FROM etl.raw_certificate_info
      UNION ALL SELECT 'etl.raw_perf_groups', COUNT(*) 
      FROM etl.raw_perf_groups
      UNION ALL SELECT 'etl.input_certificate_info', COUNT(*) 
      FROM etl.input_certificate_info
    `;
    
    const etlResult = await pool.request().query(etlQuery);
    console.table(etlResult.recordset);

    // 3. Check staging entities
    console.log('\n3. STAGING ENTITIES');
    console.log('─────────────────────────────────────────────────────────');
    
    const stagingQuery = `
      SELECT 'stg_groups' AS Entity, COUNT(*) AS Count FROM etl.stg_groups
      UNION ALL SELECT 'stg_proposals', COUNT(*) FROM etl.stg_proposals
      UNION ALL SELECT 'stg_hierarchies', COUNT(*) FROM etl.stg_hierarchies
      UNION ALL SELECT 'stg_hierarchy_versions', COUNT(*) FROM etl.stg_hierarchy_versions
      UNION ALL SELECT 'stg_hierarchy_participants', COUNT(*) FROM etl.stg_hierarchy_participants
      UNION ALL SELECT 'stg_policies', COUNT(*) FROM etl.stg_policies
      UNION ALL SELECT 'stg_policy_hierarchy_assignments', COUNT(*) FROM etl.stg_policy_hierarchy_assignments
      UNION ALL SELECT 'stg_premium_transactions', COUNT(*) FROM etl.stg_premium_transactions
    `;
    
    const stagingResult = await pool.request().query(stagingQuery);
    console.table(stagingResult.recordset);

    // 4. Check conformance statistics
    console.log('\n4. CONFORMANCE STATISTICS');
    console.log('─────────────────────────────────────────────────────────');
    
    const conformanceQuery = `
      SELECT 
        ISNULL(GroupClassification, 'No Data') AS GroupClassification,
        COUNT(*) AS GroupCount
      FROM etl.GroupConformanceStatistics
      GROUP BY GroupClassification
    `;
    
    const conformanceResult = await pool.request().query(conformanceQuery);
    if (conformanceResult.recordset.length > 0) {
      console.table(conformanceResult.recordset);
    } else {
      console.log('   No conformance statistics found');
    }

    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('VERIFICATION COMPLETE');
    console.log('═══════════════════════════════════════════════════════════\n');

    await pool.close();
  } catch (error: any) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

verifyStaging();
