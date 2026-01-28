/**
 * Consolidation Verification Script
 * 
 * Verifies the proposal consolidation process by checking:
 * - Retained vs consumed proposal counts
 * - Referential integrity
 * - Split data consistency
 * - Example consolidations
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function verifyConsolidation() {
  console.log('\n🔍 Consolidation Verification');
  console.log('═'.repeat(60));
  
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);
  
  try {
    // Count pre-stage proposals
    console.log('\n📊 Step 1: Counting proposals...');
    const prestageResult = await pool.request().query(`
      SELECT 
        COUNT(*) AS total,
        SUM(CASE WHEN IsRetained = 1 THEN 1 ELSE 0 END) AS retained,
        SUM(CASE WHEN IsRetained = 0 AND ConsumedByProposalId IS NOT NULL THEN 1 ELSE 0 END) AS consumed,
        SUM(CASE WHEN IsRetained = 0 AND ConsumedByProposalId IS NULL THEN 1 ELSE 0 END) AS unconsolidated
      FROM [prestage].[prestage_proposals]
    `);
    
    // Count staging proposals
    const stagingResult = await pool.request().query(`
      SELECT COUNT(*) AS total FROM [etl].[stg_proposals]
    `);
    
    const prestage = prestageResult.recordset[0];
    const staging = stagingResult.recordset[0];
    
    console.log(`   Pre-stage total:        ${prestage.total}`);
    console.log(`   Pre-stage retained:     ${prestage.retained}`);
    console.log(`   Pre-stage consumed:     ${prestage.consumed}`);
    console.log(`   Pre-stage unconsolidated: ${prestage.unconsolidated}`);
    console.log(`   Staging total:          ${staging.total}`);
    
    // Verify match
    let hasErrors = false;
    
    if (prestage.retained === staging.total) {
      console.log('   ✅ Retained count matches staging count');
    } else {
      console.error('   ❌ Consolidation mismatch!');
      hasErrors = true;
    }
    
    // Check for orphaned consumed proposals
    console.log('\n🔗 Step 2: Checking referential integrity...');
    const orphanedResult = await pool.request().query(`
      SELECT COUNT(*) AS orphaned
      FROM [prestage].[prestage_proposals]
      WHERE ConsumedByProposalId IS NOT NULL
        AND ConsumedByProposalId NOT IN (
          SELECT Id FROM [prestage].[prestage_proposals] WHERE IsRetained = 1
        )
    `);
    
    if (orphanedResult.recordset[0].orphaned === 0) {
      console.log('   ✅ No orphaned consumed proposals');
    } else {
      console.error(`   ❌ Found ${orphanedResult.recordset[0].orphaned} orphaned consumed proposals`);
      hasErrors = true;
    }
    
    // Check split data integrity
    console.log('\n📋 Step 3: Checking split data integrity...');
    const splitIntegrityResult = await pool.request().query(`
      SELECT 
        (SELECT COUNT(*) FROM [etl].[stg_premium_split_versions]) AS staging_versions,
        (SELECT COUNT(*) FROM [prestage].[prestage_premium_split_versions] 
         WHERE ProposalId IN (SELECT Id FROM [prestage].[prestage_proposals] WHERE IsRetained = 1)) AS expected_versions,
        (SELECT COUNT(*) FROM [etl].[stg_premium_split_participants]) AS staging_participants,
        (SELECT COUNT(*) FROM [prestage].[prestage_premium_split_participants] psp
         INNER JOIN [prestage].[prestage_premium_split_versions] psv ON psv.Id = psp.VersionId
         WHERE psv.ProposalId IN (SELECT Id FROM [prestage].[prestage_proposals] WHERE IsRetained = 1)) AS expected_participants
    `);
    
    const splitIntegrity = splitIntegrityResult.recordset[0];
    
    if (splitIntegrity.staging_versions === splitIntegrity.expected_versions) {
      console.log(`   ✅ Split versions match (${splitIntegrity.staging_versions})`);
    } else {
      console.error(`   ❌ Split versions mismatch: staging=${splitIntegrity.staging_versions}, expected=${splitIntegrity.expected_versions}`);
      hasErrors = true;
    }
    
    if (splitIntegrity.staging_participants === splitIntegrity.expected_participants) {
      console.log(`   ✅ Split participants match (${splitIntegrity.staging_participants})`);
    } else {
      console.error(`   ❌ Split participants mismatch: staging=${splitIntegrity.staging_participants}, expected=${splitIntegrity.expected_participants}`);
      hasErrors = true;
    }
    
    // Show consolidation statistics
    console.log('\n📈 Step 4: Consolidation statistics...');
    const statsResult = await pool.request().query(`
      SELECT 
        COUNT(DISTINCT SplitConfigurationMD5) AS unique_configs,
        AVG(CAST(proposals_per_config AS FLOAT)) AS avg_proposals_per_config,
        MAX(proposals_per_config) AS max_proposals_per_config
      FROM (
        SELECT 
          SplitConfigurationMD5,
          COUNT(*) AS proposals_per_config
        FROM [prestage].[prestage_proposals]
        WHERE SplitConfigurationMD5 IS NOT NULL
        GROUP BY SplitConfigurationMD5
      ) config_counts
    `);
    
    const stats = statsResult.recordset[0];
    console.log(`   Unique split configurations: ${stats.unique_configs}`);
    console.log(`   Avg proposals per config: ${stats.avg_proposals_per_config?.toFixed(2)}`);
    console.log(`   Max proposals per config: ${stats.max_proposals_per_config}`);
    
    // Show reduction ratio
    const reductionRatio = ((1 - (prestage.retained / prestage.total)) * 100).toFixed(1);
    console.log(`   Consolidation reduction: ${reductionRatio}% (${prestage.total} → ${prestage.retained})`);
    
    // Show example consolidations
    console.log('\n📝 Step 5: Example consolidations...');
    const exampleResult = await pool.request().query(`
      SELECT TOP 3
        ConsumedByProposalId,
        COUNT(*) AS consumed_count
      FROM [prestage].[prestage_proposals]
      WHERE ConsumedByProposalId IS NOT NULL
      GROUP BY ConsumedByProposalId
      ORDER BY COUNT(*) DESC
    `);
    
    if (exampleResult.recordset.length > 0) {
      console.log('   Top consolidated proposals:');
      for (const row of exampleResult.recordset) {
        console.log(`     ${row.ConsumedByProposalId}: ${row.consumed_count} consumed`);
        
        // Show detail for first example
        if (row === exampleResult.recordset[0]) {
          const detailResult = await pool.request().query(`
            SELECT 
              Id,
              GroupId,
              DateRangeFrom,
              DateRangeTo,
              LEFT(ProductCodes, 100) AS ProductCodes,
              ConsolidationReason
            FROM [prestage].[prestage_proposals]
            WHERE ConsumedByProposalId = '${row.ConsumedByProposalId}'
            ORDER BY DateRangeFrom
          `);
          
          console.log('     Details:');
          for (const detail of detailResult.recordset) {
            console.log(`       - ${detail.Id}: ${detail.DateRangeFrom}-${detail.DateRangeTo || 'NULL'}, ${detail.ProductCodes}`);
          }
        }
      }
    } else {
      console.log('   No consolidations found (all proposals retained as-is)');
    }
    
    // Show sample split configuration JSON
    console.log('\n📄 Step 6: Sample split configuration JSON...');
    const sampleResult = await pool.request().query(`
      SELECT TOP 1
        Id,
        GroupId,
        LEFT(SplitConfigurationJSON, 500) AS ConfigSample
      FROM [prestage].[prestage_proposals]
      WHERE SplitConfigurationJSON IS NOT NULL
      ORDER BY Id
    `);
    
    if (sampleResult.recordset.length > 0) {
      const sample = sampleResult.recordset[0];
      console.log(`   Sample proposal: ${sample.Id} (${sample.GroupId})`);
      console.log(`   Config preview: ${sample.ConfigSample}...`);
    }
    
    // Final summary
    console.log('\n═'.repeat(60));
    if (hasErrors) {
      console.error('❌ VERIFICATION FAILED - Issues found above\n');
      process.exit(1);
    } else {
      console.log('✅ VERIFICATION PASSED - All checks successful\n');
      process.exit(0);
    }
    
  } catch (error) {
    console.error('\n❌ Verification error:', error);
    process.exit(1);
  } finally {
    await pool.close();
  }
}

// Run verification
verifyConsolidation().catch(console.error);
