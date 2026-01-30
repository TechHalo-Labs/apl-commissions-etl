/**
 * Check staging table data to see why exports are failing
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);

  const etlSchema = config.database.schemas.processing || 'etl';

  try {
    console.log('\n📊 CHECKING STAGING DATA:\n');
    console.log('═'.repeat(60));

    // Check proposals
    const proposals = await pool.request().query(`
      SELECT COUNT(*) AS cnt FROM [${etlSchema}].[stg_proposals]
    `);
    console.log(`stg_proposals: ${proposals.recordset[0].cnt.toLocaleString()} rows`);

    // Check hierarchies
    const hierarchies = await pool.request().query(`
      SELECT COUNT(*) AS cnt FROM [${etlSchema}].[stg_hierarchies]
    `);
    console.log(`stg_hierarchies: ${hierarchies.recordset[0].cnt.toLocaleString()} rows`);

    // Check production
    console.log('\n📊 CHECKING PRODUCTION DATA:\n');
    console.log('═'.repeat(60));

    const prodProposals = await pool.request().query(`
      SELECT COUNT(*) AS cnt FROM [dbo].[Proposals]
    `);
    console.log(`dbo.Proposals: ${prodProposals.recordset[0].cnt.toLocaleString()} rows`);

    const prodHierarchies = await pool.request().query(`
      SELECT COUNT(*) AS cnt FROM [dbo].[Hierarchies]
    `);
    console.log(`dbo.Hierarchies: ${prodHierarchies.recordset[0].cnt.toLocaleString()} rows`);

    // Check if there are any proposals in staging that aren't in production
    console.log('\n🔍 CHECKING FOR MISSING EXPORTS:\n');
    console.log('═'.repeat(60));

    const missingProposals = await pool.request().query(`
      SELECT COUNT(*) AS cnt
      FROM [${etlSchema}].[stg_proposals] s
      WHERE s.Id NOT IN (SELECT Id FROM [dbo].[Proposals])
    `);
    console.log(`Missing Proposals (in staging but not production): ${missingProposals.recordset[0].cnt.toLocaleString()}`);

    const missingHierarchies = await pool.request().query(`
      SELECT COUNT(*) AS cnt
      FROM [${etlSchema}].[stg_hierarchies] s
      WHERE s.Id NOT IN (SELECT Id FROM [dbo].[Hierarchies])
    `);
    console.log(`Missing Hierarchies (in staging but not production): ${missingHierarchies.recordset[0].cnt.toLocaleString()}`);

    // Sample data from staging
    console.log('\n📋 SAMPLE STAGING DATA:\n');
    console.log('═'.repeat(60));

    const sampleProposals = await pool.request().query(`
      SELECT TOP 5 Id, ProposalNumber, GroupId, Status
      FROM [${etlSchema}].[stg_proposals]
      ORDER BY Id
    `);
    console.log('\nSample Proposals:');
    sampleProposals.recordset.forEach((r: any) => {
      console.log(`  ${r.Id} | ${r.ProposalNumber} | Group: ${r.GroupId} | Status: ${r.Status}`);
    });

    const sampleHierarchies = await pool.request().query(`
      SELECT TOP 5 Id, Name, GroupId, Status
      FROM [${etlSchema}].[stg_hierarchies]
      ORDER BY Id
    `);
    console.log('\nSample Hierarchies:');
    sampleHierarchies.recordset.forEach((r: any) => {
      console.log(`  ${r.Id} | ${r.Name} | Group: ${r.GroupId} | Status: ${r.Status}`);
    });

  } catch (error: any) {
    console.error('Error:', error.message);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
