import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));

  try {
    console.log('\n═══════════════════════════════════════════════');
    console.log('  Group Counts at Each Stage');
    console.log('═══════════════════════════════════════════════\n');

    // Check unique groups in various stages
    const result = await pool.request().query(`
      SELECT 
        'poc_raw_data.raw_perf_groups (all)' as stage,
        COUNT(*) as count
      FROM poc_raw_data.raw_perf_groups
      
      UNION ALL
      
      SELECT 
        'poc_raw_data.raw_perf_groups (unique GroupNum)' as stage,
        COUNT(DISTINCT GroupNum) as count
      FROM poc_raw_data.raw_perf_groups
      WHERE GroupNum IS NOT NULL
      
      UNION ALL
      
      SELECT 
        'new_data.PerfGroupModel' as stage,
        COUNT(*) as count
      FROM new_data.PerfGroupModel
      
      UNION ALL
      
      SELECT
        'etl.stg_groups' as stage,
        COUNT(*) as count
      FROM etl.stg_groups
      
      UNION ALL
      
      SELECT
        'dbo.EmployerGroups' as stage,
        COUNT(*) as count
      FROM dbo.EmployerGroups;
    `);

    console.table(result.recordset);
    
    // Check why groups are missing
    console.log('\n═══════════════════════════════════════════════');
    console.log('  Missing Groups Analysis');
    console.log('═══════════════════════════════════════════════\n');
    
    const missingAnalysis = await pool.request().query(`
      SELECT TOP 10
        g.GroupNum,
        g.GroupName,
        g.StateAbbreviation as SitusState,
        CASE WHEN stg.Id IS NULL THEN 'Missing from etl.stg_groups' ELSE 'Present' END as status
      FROM poc_raw_data.raw_perf_groups g
      LEFT JOIN etl.stg_groups stg ON stg.Id = g.GroupNum
      WHERE stg.Id IS NULL
      ORDER BY g.GroupNum;
    `);
    
    console.log('\nSample of Missing Groups:');
    console.table(missingAnalysis.recordset);
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
