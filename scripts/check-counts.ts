/**
 * Quick count checker for ETL tables
 */
import * as sql from 'mssql';

const config: sql.config = {
  server: 'halo-sql.database.windows.net',
  database: 'halo-sqldb',
  user: '***REMOVED***',
  password: '***REMOVED***',
  options: { encrypt: true, trustServerCertificate: true },
  requestTimeout: 30000
};

async function main() {
  const pool = await sql.connect(config);
  
  console.log('\n=== RAW TABLES (etl schema) ===');
  const rawResult = await pool.request().query(`
    SELECT 'raw_premiums' as tbl, COUNT(*) as cnt FROM [etl].[raw_premiums]
    UNION ALL SELECT 'raw_certificate_info', COUNT(*) FROM [etl].[raw_certificate_info]
    UNION ALL SELECT 'raw_individual_brokers', COUNT(*) FROM [etl].[raw_individual_brokers]
    UNION ALL SELECT 'raw_org_brokers', COUNT(*) FROM [etl].[raw_org_brokers]
    UNION ALL SELECT 'raw_schedule_rates', COUNT(*) FROM [etl].[raw_schedule_rates]
    UNION ALL SELECT 'raw_commissions_detail', COUNT(*) FROM [etl].[raw_commissions_detail]
    ORDER BY 1
  `);
  console.table(rawResult.recordset);

  console.log('\n=== STAGING TABLES (etl schema) ===');
  const stgResult = await pool.request().query(`
    SELECT 'stg_brokers' as tbl, COUNT(*) as cnt FROM [etl].[stg_brokers]
    UNION ALL SELECT 'stg_groups', COUNT(*) FROM [etl].[stg_groups]
    UNION ALL SELECT 'stg_policies', COUNT(*) FROM [etl].[stg_policies]
    UNION ALL SELECT 'stg_proposals', COUNT(*) FROM [etl].[stg_proposals]
    UNION ALL SELECT 'stg_hierarchies', COUNT(*) FROM [etl].[stg_hierarchies]
    UNION ALL SELECT 'stg_hierarchy_participants', COUNT(*) FROM [etl].[stg_hierarchy_participants]
    UNION ALL SELECT 'stg_premium_transactions', COUNT(*) FROM [etl].[stg_premium_transactions]
    UNION ALL SELECT 'stg_schedules', COUNT(*) FROM [etl].[stg_schedules]
    UNION ALL SELECT 'stg_schedule_rates', COUNT(*) FROM [etl].[stg_schedule_rates]
    ORDER BY 1
  `);
  console.table(stgResult.recordset);

  console.log('\n=== PRODUCTION TABLES (dbo schema) ===');
  const prodResult = await pool.request().query(`
    SELECT 'Brokers' as tbl, COUNT(*) as cnt FROM [dbo].[Brokers]
    UNION ALL SELECT 'Group', COUNT(*) FROM [dbo].[Group]
    UNION ALL SELECT 'Policies', COUNT(*) FROM [dbo].[Policies]
    UNION ALL SELECT 'Proposals', COUNT(*) FROM [dbo].[Proposals]
    UNION ALL SELECT 'Hierarchies', COUNT(*) FROM [dbo].[Hierarchies]
    UNION ALL SELECT 'HierarchyParticipants', COUNT(*) FROM [dbo].[HierarchyParticipants]
    UNION ALL SELECT 'PremiumTransactions', COUNT(*) FROM [dbo].[PremiumTransactions]
    UNION ALL SELECT 'Schedules', COUNT(*) FROM [dbo].[Schedules]
    UNION ALL SELECT 'ScheduleRates', COUNT(*) FROM [dbo].[ScheduleRates]
    ORDER BY 1
  `);
  console.table(prodResult.recordset);

  await pool.close();
}

main().catch(console.error);

