/**
 * Create and Populate stg_excluded_groups Table
 * Flags groups that should be excluded from export
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);
  const etlSchema = config.database.schemas.processing || 'etl';

  try {
    console.log('\n📋 Creating stg_excluded_groups table...\n');

    // Drop and recreate table
    await pool.request().query(`
      DROP TABLE IF EXISTS [${etlSchema}].[stg_excluded_groups];
    `);

    await pool.request().query(`
      CREATE TABLE [${etlSchema}].[stg_excluded_groups] (
          GroupId NVARCHAR(100) NOT NULL,
          GroupName NVARCHAR(500),
          ExclusionReason NVARCHAR(500),
          CreationTime DATETIME2 DEFAULT GETUTCDATE(),
          CONSTRAINT PK_stg_excluded_groups PRIMARY KEY (GroupId)
      );
    `);
    console.log('✅ Created table');

    // Populate excluded groups
    console.log('\n📋 Populating excluded groups...\n');

    const insertResult = await pool.request().query(`
      INSERT INTO [${etlSchema}].[stg_excluded_groups] (
          GroupId,
          GroupName,
          ExclusionReason
      )
      SELECT DISTINCT
          sg.Id AS GroupId,
          sg.Name AS GroupName,
          'Universal Trucking group' AS ExclusionReason
      FROM [${etlSchema}].[stg_groups] sg
      WHERE 
          -- Universal Trucking groups ONLY
          -- NOTE: G0000 and G00000 (DTC groups) are now INCLUDED
          sg.Name LIKE 'Universal Truck%';
    `);

    const excludedCount = insertResult.rowsAffected[0] || 0;
    console.log(`✅ Populated ${excludedCount.toLocaleString()} excluded groups`);

    // Show breakdown
    const breakdown = await pool.request().query(`
      SELECT 
          ExclusionReason,
          COUNT(*) AS GroupCount
      FROM [${etlSchema}].[stg_excluded_groups]
      GROUP BY ExclusionReason
      ORDER BY GroupCount DESC;
    `);

    console.log('\n📊 Breakdown by exclusion reason:');
    console.log('─'.repeat(60));
    breakdown.recordset.forEach((r: any) => {
      console.log(`  ${r.ExclusionReason.padEnd(30)}: ${r.GroupCount.toLocaleString()}`);
    });

    // Show sample
    const sample = await pool.request().query(`
      SELECT TOP 10
          GroupId,
          GroupName,
          ExclusionReason
      FROM [${etlSchema}].[stg_excluded_groups]
      ORDER BY ExclusionReason, GroupId;
    `);

    console.log('\n📋 Sample excluded groups:');
    console.log('─'.repeat(60));
    sample.recordset.forEach((r: any) => {
      console.log(`  ${r.GroupId.padEnd(15)} | ${(r.GroupName || '').substring(0, 40).padEnd(40)} | ${r.ExclusionReason}`);
    });

    console.log('\n✅ stg_excluded_groups table ready!\n');

  } catch (error: any) {
    console.error('\n❌ Error:', error.message);
    throw error;
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
