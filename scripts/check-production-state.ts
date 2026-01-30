/**
 * Check current production state
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);

  try {
    const query = `
      SELECT 'Brokers' AS TableName, COUNT(*) AS [RowCount] FROM [dbo].[Brokers]
      UNION ALL SELECT 'BrokerLicenses', COUNT(*) FROM [dbo].[BrokerLicenses]
      UNION ALL SELECT 'EmployerGroups', COUNT(*) FROM [dbo].[EmployerGroups]
      UNION ALL SELECT 'Products', COUNT(*) FROM [dbo].[Products]
      UNION ALL SELECT 'Plans', COUNT(*) FROM [dbo].[Plans]
      UNION ALL SELECT 'Schedules', COUNT(*) FROM [dbo].[Schedules]
      UNION ALL SELECT 'Proposals', COUNT(*) FROM [dbo].[Proposals]
      UNION ALL SELECT 'Policies', COUNT(*) FROM [dbo].[Policies]
      UNION ALL SELECT 'Hierarchies', COUNT(*) FROM [dbo].[Hierarchies]
      UNION ALL SELECT 'PremiumTransactions', COUNT(*) FROM [dbo].[PremiumTransactions]
      ORDER BY TableName;
    `;

    const result = await pool.request().query(query);
    
    console.log('\n📊 PRODUCTION STATE AFTER EXPORT:\n');
    console.log('═'.repeat(50));
    result.recordset.forEach((r: any) => {
      console.log(`  ${r.TableName.padEnd(25)}: ${r.RowCount.toLocaleString().padStart(10)} rows`);
    });
    console.log('═'.repeat(50));
    console.log('');

  } catch (error: any) {
    console.error('Error:', error.message);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
