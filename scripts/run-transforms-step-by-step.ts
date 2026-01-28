/**
 * Run Transform Phase Step-by-Step with Verification Pauses
 * 
 * Executes each transform script individually and waits for user confirmation
 * before proceeding to the next step. Allows verification of results between steps.
 * 
 * Usage:
 *   npx tsx scripts/run-transforms-step-by-step.ts
 *   npx tsx scripts/run-transforms-step-by-step.ts --config appsettings.poc2.json
 * 
 * Features:
 * - Executes one transform at a time
 * - Shows verification results after each step
 * - Pauses for user confirmation before continuing
 * - Can abort at any step
 * - Shows detailed row counts and data samples
 */

import * as sql from 'mssql';
import * as path from 'path';
import * as readline from 'readline';
import { loadConfig, getSqlConfig } from './lib/config-loader';
import { executeSQLScript } from './lib/sql-executor';

const scriptsDir = path.join(__dirname, '../sql');

interface TransformStep {
  name: string;
  script: string;
  description: string;
  purpose: string;
  expectedResults: string[];
  testQueries: string;
  verification: string;
}

const transformSteps: TransformStep[] = [
  {
    name: 'Step 1: References',
    script: path.join(scriptsDir, 'transforms/00-references.sql'),
    description: 'Creates foundational reference data for states and products',
    purpose: 'Establishes lookup tables used throughout the ETL for data validation and enrichment. States are needed for situs state validation, products for policy categorization.',
    expectedResults: [
      '~50 states/territories in stg_states',
      '~100-200 product definitions in stg_products',
      'All states should have proper codes (e.g., FL, TX, CA)'
    ],
    testQueries: `
-- Check state data
SELECT Code, Name, Country FROM [$(ETL_SCHEMA)].[stg_states] ORDER BY Code;

-- Check product data  
SELECT Code, Name, Category FROM [$(ETL_SCHEMA)].[stg_products] ORDER BY Code;

-- Verify no duplicates
SELECT Code, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_states] GROUP BY Code HAVING COUNT(*) > 1;
    `,
    verification: `
      SELECT 'stg_states' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[stg_states];
      SELECT 'stg_products' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[stg_products];
      SELECT TOP 3 Code, Name FROM [$(ETL_SCHEMA)].[stg_states] ORDER BY Code;
    `
  },
  {
    name: 'Step 2: Brokers',
    script: path.join(scriptsDir, 'transforms/01-brokers.sql'),
    description: 'Transforms broker data from both individual and organization rosters',
    purpose: 'Creates the master broker registry by combining individual agents and broker organizations. Sets ExternalPartyId (UniquePartyId) which is the primary identifier for brokers. Ensures all brokers have Status=Active for commission processing.',
    expectedResults: [
      '~12,000 total brokers (mix of individuals and organizations)',
      '~95%+ should have ExternalPartyId populated',
      'All brokers should have Status=0 (Active)',
      'Names should be properly formatted (not empty)'
    ],
    testQueries: `
-- Check broker type distribution
SELECT BrokerType, COUNT(*) AS cnt, 
       COUNT(DISTINCT ExternalPartyId) AS unique_ids
FROM [$(ETL_SCHEMA)].[stg_brokers]
GROUP BY BrokerType;

-- Check for missing critical data
SELECT 
    SUM(CASE WHEN ExternalPartyId IS NULL THEN 1 ELSE 0 END) AS missing_external_id,
    SUM(CASE WHEN Name IS NULL OR Name = '' THEN 1 ELSE 0 END) AS missing_name,
    SUM(CASE WHEN Status != 0 THEN 1 ELSE 0 END) AS inactive_status
FROM [$(ETL_SCHEMA)].[stg_brokers];

-- Sample brokers
SELECT TOP 10 Id, Name, BrokerType, ExternalPartyId, Status 
FROM [$(ETL_SCHEMA)].[stg_brokers] 
ORDER BY Id;
    `,
    verification: `
      SELECT 
        'stg_brokers' AS [table],
        COUNT(*) AS total_brokers,
        SUM(CASE WHEN BrokerType = 'Individual' THEN 1 ELSE 0 END) AS individuals,
        SUM(CASE WHEN BrokerType = 'Organization' THEN 1 ELSE 0 END) AS organizations,
        SUM(CASE WHEN ExternalPartyId IS NOT NULL THEN 1 ELSE 0 END) AS with_external_id
      FROM [$(ETL_SCHEMA)].[stg_brokers];
      
      SELECT TOP 3 Id, Name, BrokerType, ExternalPartyId, [Status] 
      FROM [$(ETL_SCHEMA)].[stg_brokers] 
      ORDER BY Id;
    `
  },
  {
    name: 'Step 3: Groups',
    script: path.join(scriptsDir, 'transforms/02-groups.sql'),
    description: 'Transforms employer groups from PerfGroupModel with primary broker assignment',
    purpose: 'Creates employer group records with proper names (not generic), group sizes, and links to primary brokers via BrokerUniqueId from raw_perf_groups. PrimaryBrokerId is critical for proposal broker assignment.',
    expectedResults: [
      '~3,000-3,500 employer groups',
      '~95%+ should have PrimaryBrokerId populated (from perf groups)',
      'GroupNames should be real names, not "Group XXXXX"',
      'GroupSize should be > 0 for most groups'
    ],
    testQueries: `
-- Check group data quality
SELECT 
    COUNT(*) AS total_groups,
    SUM(CASE WHEN PrimaryBrokerId IS NOT NULL THEN 1 ELSE 0 END) AS with_primary_broker,
    SUM(CASE WHEN GroupSize > 0 THEN 1 ELSE 0 END) AS with_group_size,
    SUM(CASE WHEN Name NOT LIKE 'Group %' THEN 1 ELSE 0 END) AS with_real_names
FROM [$(ETL_SCHEMA)].[stg_groups];

-- Check situs state distribution
SELECT SitusState, COUNT(*) AS cnt
FROM [$(ETL_SCHEMA)].[stg_groups]
GROUP BY SitusState
ORDER BY cnt DESC;

-- Sample groups with broker info
SELECT TOP 10 Id, Code, Name, GroupSize, PrimaryBrokerId, SitusState
FROM [$(ETL_SCHEMA)].[stg_groups]
WHERE PrimaryBrokerId IS NOT NULL
ORDER BY GroupSize DESC;
    `,
    verification: `
      SELECT 
        'stg_groups' AS [table],
        COUNT(*) AS total_groups,
        SUM(CASE WHEN PrimaryBrokerId IS NOT NULL THEN 1 ELSE 0 END) AS with_primary_broker,
        SUM(CASE WHEN GroupSize > 0 THEN 1 ELSE 0 END) AS with_group_size
      FROM [$(ETL_SCHEMA)].[stg_groups];
      
      SELECT TOP 3 Id, Code, Name, GroupSize, PrimaryBrokerId, SitusState 
      FROM [$(ETL_SCHEMA)].[stg_groups] 
      WHERE PrimaryBrokerId IS NOT NULL
      ORDER BY Id;
    `
  },
  {
    name: 'Step 4: Products',
    script: path.join(scriptsDir, 'transforms/03-products.sql'),
    description: 'Transforms product definitions and categories',
    purpose: 'Creates product catalog used for policy classification and commission rate lookup. Products link to schedules for commission calculation.',
    expectedResults: [
      '~100-200 product records',
      'Products categorized (Dental, Vision, Life, etc.)',
      'All products should have valid codes'
    ],
    testQueries: `
-- Check product categories
SELECT Category, COUNT(*) AS cnt
FROM [$(ETL_SCHEMA)].[stg_products]
GROUP BY Category
ORDER BY cnt DESC;

-- Sample products
SELECT TOP 20 Code, Name, Category 
FROM [$(ETL_SCHEMA)].[stg_products] 
ORDER BY Code;
    `,
    verification: `
      SELECT 
        'stg_products' AS [table],
        COUNT(*) AS total_products,
        COUNT(DISTINCT Category) AS unique_categories
      FROM [$(ETL_SCHEMA)].[stg_products];
      
      SELECT TOP 5 Code, Name, Category FROM [$(ETL_SCHEMA)].[stg_products] ORDER BY Code;
    `
  },
  {
    name: 'Step 5: Schedules ⚠️ CRITICAL',
    script: path.join(scriptsDir, 'transforms/04-schedules.sql'),
    description: '⚠️ CRITICAL: Transforms commission schedules and rates (must succeed!)',
    purpose: 'Creates commission rate schedules from raw_schedule_rates. This step MUST find schedules in input data, or downstream steps will fail. Uses permanent work tables to avoid sqlcmd batching issues. Schedule rates define commission percentages for first-year and renewal commissions.',
    expectedResults: [
      '~600-700 unique schedules',
      '~10,000+ schedule rates (first-year + renewal)',
      'CRITICAL: If schedules = 0, ETL has failed - check raw data exists',
      'Rates should have FirstYearRate and RenewalRate populated'
    ],
    testQueries: `
-- CRITICAL: Check schedule count (should be > 0!)
SELECT COUNT(*) AS total_schedules FROM [$(ETL_SCHEMA)].[stg_schedules];

-- Check schedule rate distribution
SELECT 
    COUNT(*) AS total_rates,
    SUM(CASE WHEN FirstYearRate > 0 THEN 1 ELSE 0 END) AS with_first_year,
    SUM(CASE WHEN RenewalRate > 0 THEN 1 ELSE 0 END) AS with_renewal,
    AVG(CAST(FirstYearRate AS FLOAT)) AS avg_first_year_rate,
    AVG(CAST(RenewalRate AS FLOAT)) AS avg_renewal_rate
FROM [$(ETL_SCHEMA)].[stg_schedule_rates];

-- Sample schedules
SELECT TOP 10 s.Id, s.Name, s.ExternalId, COUNT(sr.Id) AS rate_count
FROM [$(ETL_SCHEMA)].[stg_schedules] s
LEFT JOIN [$(ETL_SCHEMA)].[stg_schedule_rates] sr ON sr.ScheduleId = s.Id
GROUP BY s.Id, s.Name, s.ExternalId
ORDER BY rate_count DESC;

-- ⚠️ If this returns 0 schedules, STOP and investigate raw data!
    `,
    verification: `
      SELECT 
        'stg_schedules' AS [table],
        COUNT(*) AS total_schedules
      FROM [$(ETL_SCHEMA)].[stg_schedules];
      
      SELECT 
        'stg_schedule_rates' AS [table],
        COUNT(*) AS total_rates,
        SUM(CASE WHEN FirstYearRate > 0 OR RenewalRate > 0 THEN 1 ELSE 0 END) AS heaped_rates,
        SUM(CASE WHEN FirstYearRate = 0 AND RenewalRate = 0 THEN 1 ELSE 0 END) AS level_only_rates
      FROM [$(ETL_SCHEMA)].[stg_schedule_rates];
      
      -- CRITICAL: Verify schedules were found
      SELECT 
        CASE 
          WHEN COUNT(*) >= 500 THEN '✅ PASS: Found ' + CAST(COUNT(*) AS VARCHAR) + ' schedules'
          WHEN COUNT(*) > 0 THEN '⚠️ WARNING: Only ' + CAST(COUNT(*) AS VARCHAR) + ' schedules found (expected 600+)'
          ELSE '❌ FAIL: No schedules found!'
        END AS schedule_validation
      FROM [$(ETL_SCHEMA)].[stg_schedules];
      
      SELECT TOP 5 Id, ExternalId, Name, ProductCount FROM [$(ETL_SCHEMA)].[stg_schedules] ORDER BY Id;
    `
  },
  {
    name: 'Step 6a: Proposals (Simple Groups)',
    script: path.join(scriptsDir, 'transforms/06a-proposals-simple-groups.sql'),
    description: 'Creates proposals for simple groups',
    verification: `
      SELECT COUNT(*) AS simple_proposals FROM [$(ETL_SCHEMA)].[stg_proposals] WHERE [Source] LIKE '%Simple%';
      SELECT TOP 3 Id, GroupId, ProposedEffectiveDate FROM [$(ETL_SCHEMA)].[stg_proposals] WHERE [Source] LIKE '%Simple%';
    `
  },
  {
    name: 'Step 6b: Proposals (Non-Conformant)',
    script: path.join(scriptsDir, 'transforms/06b-proposals-non-conformant.sql'),
    description: 'Creates proposals for non-conformant groups',
    verification: `
      SELECT COUNT(*) AS non_conformant_proposals FROM [$(ETL_SCHEMA)].[stg_proposals] WHERE [Source] LIKE '%Non-Conformant%';
    `
  },
  {
    name: 'Step 6c: Proposals (Plan-Differentiated)',
    script: path.join(scriptsDir, 'transforms/06c-proposals-plan-differentiated.sql'),
    description: 'Creates proposals differentiated by plan code',
    verification: `
      SELECT COUNT(*) AS plan_differentiated FROM [$(ETL_SCHEMA)].[stg_proposals] WHERE [Source] LIKE '%Plan%';
    `
  },
  {
    name: 'Step 6d: Proposals (Year-Differentiated)',
    script: path.join(scriptsDir, 'transforms/06d-proposals-year-differentiated.sql'),
    description: 'Creates proposals differentiated by certificate effective year',
    verification: `
      SELECT COUNT(*) AS year_differentiated FROM [$(ETL_SCHEMA)].[stg_proposals] WHERE [Source] LIKE '%Year%';
    `
  },
  {
    name: 'Step 6e: Proposals (Granular)',
    script: path.join(scriptsDir, 'transforms/06e-proposals-granular.sql'),
    description: 'Creates granular proposals for remaining groups',
    verification: `
      SELECT COUNT(*) AS granular_proposals FROM [$(ETL_SCHEMA)].[stg_proposals] WHERE [Source] LIKE '%Granular%';
      
      -- Summary by source
      SELECT 
        [Source],
        COUNT(*) AS proposal_count
      FROM [$(ETL_SCHEMA)].[stg_proposals]
      GROUP BY [Source]
      ORDER BY COUNT(*) DESC;
    `
  },
  {
    name: 'Step 6f: Consolidate Proposals',
    script: path.join(scriptsDir, 'transforms/06f-consolidate-proposals.sql'),
    description: 'Consolidates proposals where applicable (OPTIONAL - can be skipped)',
    verification: `
      SELECT COUNT(*) AS total_proposals_after_consolidation FROM [$(ETL_SCHEMA)].[stg_proposals];
    `
  },
  {
    name: 'Step 6g: Normalize Date Ranges',
    script: path.join(scriptsDir, 'transforms/06g-normalize-proposal-date-ranges.sql'),
    description: 'Normalizes proposal effective date ranges',
    verification: `
      SELECT 
        COUNT(*) AS proposals_with_end_dates,
        COUNT(CASE WHEN EffectiveDateTo IS NULL THEN 1 END) AS open_ended_proposals
      FROM [$(ETL_SCHEMA)].[stg_proposals];
    `
  },
  {
    name: 'Step 6z: Update Broker Names',
    script: path.join(scriptsDir, 'transforms/06z-update-proposal-broker-names.sql'),
    description: 'Updates BrokerName on proposals from Brokers table',
    verification: `
      SELECT 
        COUNT(*) AS proposals_with_broker_name,
        COUNT(CASE WHEN BrokerName IS NULL OR BrokerName = '' THEN 1 END) AS proposals_without_broker_name
      FROM [$(ETL_SCHEMA)].[stg_proposals];
    `
  },
  {
    name: 'Step 7: Hierarchies',
    script: path.join(scriptsDir, 'transforms/07-hierarchies.sql'),
    description: 'Creates hierarchies with splits (CRITICAL - must NOT consolidate by StructureSignature)',
    verification: `
      SELECT 
        'stg_hierarchies' AS [table],
        COUNT(*) AS total_hierarchies,
        COUNT(DISTINCT ProposalId) AS unique_proposals,
        SUM(CASE WHEN [Status] = 'Active' THEN 1 ELSE 0 END) AS active_count
      FROM [$(ETL_SCHEMA)].[stg_hierarchies];
      
      -- Verify participants have ScheduleId
      SELECT 
        'stg_hierarchy_participants' AS [table],
        COUNT(*) AS total_participants,
        SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) AS with_schedule,
        SUM(CASE WHEN ScheduleId IS NULL THEN 1 ELSE 0 END) AS without_schedule,
        CAST(SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS schedule_link_percent
      FROM [$(ETL_SCHEMA)].[stg_hierarchy_participants];
      
      -- CRITICAL: Check schedule linking
      SELECT 
        CASE 
          WHEN CAST(SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) >= 95 
            THEN '✅ PASS: ' + CAST(CAST(SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS VARCHAR) + '% schedules linked'
          WHEN CAST(SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) >= 80 
            THEN '⚠️ WARNING: Only ' + CAST(CAST(SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS VARCHAR) + '% schedules linked'
          ELSE '❌ FAIL: ' + CAST(CAST(SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS VARCHAR) + '% schedules linked (expected 95%+)'
        END AS schedule_validation
      FROM [$(ETL_SCHEMA)].[stg_hierarchy_participants];
      
      SELECT TOP 3 Id, ProposalId, [Status], BrokerId FROM [$(ETL_SCHEMA)].[stg_hierarchies];
    `
  },
  {
    name: 'Step 8: Hierarchy Splits',
    script: path.join(scriptsDir, 'transforms/08-hierarchy-splits.sql'),
    description: 'Creates premium split versions',
    verification: `
      SELECT COUNT(*) AS split_versions FROM [$(ETL_SCHEMA)].[stg_premium_split_versions];
      SELECT COUNT(*) AS split_participants FROM [$(ETL_SCHEMA)].[stg_premium_split_participants];
    `
  },
  {
    name: 'Step 9: Policies',
    script: path.join(scriptsDir, 'transforms/09-policies.sql'),
    description: 'Transforms policies/certificates',
    verification: `
      SELECT 
        COUNT(*) AS total_policies,
        COUNT(DISTINCT groupId) AS unique_groups,
        COUNT(DISTINCT productId) AS unique_products
      FROM [$(ETL_SCHEMA)].[stg_policies];
      
      SELECT TOP 3 Id, groupId, productId, effectiveDate FROM [$(ETL_SCHEMA)].[stg_policies];
    `
  },
  {
    name: 'Step 10: Premium Transactions',
    script: path.join(scriptsDir, 'transforms/10-premium-transactions.sql'),
    description: 'Transforms premium transactions',
    verification: `
      SELECT COUNT(*) AS total_premiums FROM [$(ETL_SCHEMA)].[stg_premium_transactions];
      SELECT SUM(Amount) AS total_premium_amount FROM [$(ETL_SCHEMA)].[stg_premium_transactions];
    `
  },
  {
    name: 'Step 11: Policy Hierarchy Assignments',
    script: path.join(scriptsDir, 'transforms/11-policy-hierarchy-assignments.sql'),
    description: 'Creates PHA for non-conformant policies (CRITICAL - must NOT filter by SplitBrokerSeq=1)',
    verification: `
      SELECT 
        'stg_policy_hierarchy_assignments' AS [table],
        COUNT(*) AS total_assignments,
        COUNT(DISTINCT PolicyId) AS unique_policies,
        COUNT(DISTINCT HierarchyId) AS unique_hierarchies
      FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments];
      
      -- Check for multiple PHA per policy (expected for multiple earnings)
      SELECT 
        PolicyId,
        COUNT(*) AS assignment_count
      FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments]
      GROUP BY PolicyId
      HAVING COUNT(*) > 1
      ORDER BY COUNT(*) DESC;
      
      -- CRITICAL: Verify PHA participants have ScheduleId
      SELECT 
        'PHA Participants' AS [table],
        COUNT(*) AS total,
        SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) AS with_schedule,
        CAST(SUM(CASE WHEN ScheduleId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS schedule_percent
      FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_participants];
    `
  },
  {
    name: 'Step 12: Audit & Cleanup',
    script: path.join(scriptsDir, 'transforms/99-audit-and-cleanup.sql'),
    description: 'Audits data quality and fixes any issues (Broker IDs, Schedule linking)',
    verification: `
      -- Final audit summary
      SELECT 'Brokers' AS entity, COUNT(*) AS count FROM [$(ETL_SCHEMA)].[stg_brokers];
      SELECT 'Groups' AS entity, COUNT(*) AS count FROM [$(ETL_SCHEMA)].[stg_groups];
      SELECT 'Schedules' AS entity, COUNT(*) AS count FROM [$(ETL_SCHEMA)].[stg_schedules];
      SELECT 'Proposals' AS entity, COUNT(*) AS count FROM [$(ETL_SCHEMA)].[stg_proposals];
      SELECT 'Hierarchies' AS entity, COUNT(*) AS count FROM [$(ETL_SCHEMA)].[stg_hierarchies];
      SELECT 'Policies' AS entity, COUNT(*) AS count FROM [$(ETL_SCHEMA)].[stg_policies];
      SELECT 'PHA' AS entity, COUNT(*) AS count FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments];
      
      -- Data quality checks
      SELECT 
        'Broker ID Population' AS check_name,
        CAST(SUM(CASE WHEN p.BrokerUniquePartyId IS NOT NULL AND p.BrokerUniquePartyId != '' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS percent_populated,
        CASE 
          WHEN CAST(SUM(CASE WHEN p.BrokerUniquePartyId IS NOT NULL AND p.BrokerUniquePartyId != '' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) >= 95 THEN '✅ PASS'
          ELSE '⚠️ WARNING'
        END AS status
      FROM [$(ETL_SCHEMA)].[stg_proposals] p;
    `
  }
];

/**
 * Prompt user for confirmation
 */
function askToContinue(): Promise<boolean> {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
  
  return new Promise((resolve) => {
    rl.question('\nContinue to next step? (y/n/s to skip remaining): ', (answer) => {
      rl.close();
      const ans = answer.toLowerCase();
      if (ans === 's' || ans === 'skip') {
        resolve(false);
      } else {
        resolve(ans === 'y' || ans === 'yes');
      }
    });
  });
}

/**
 * Execute verification query and display results
 */
async function runVerification(pool: sql.ConnectionPool, verificationSQL: string, schemaName: string) {
  console.log('\n┌─────────────────────────────────────────────┐');
  console.log('│         VERIFICATION RESULTS                │');
  console.log('└─────────────────────────────────────────────┘\n');
  
  try {
    // Replace schema variable with actual schema name
    const actualSQL = verificationSQL.replace(/\$\(ETL_SCHEMA\)/g, schemaName);
    
    const result = await pool.request().query(actualSQL);
    
    // Display each recordset
    if (Array.isArray(result.recordsets)) {
      result.recordsets.forEach((recordset, index) => {
        if (recordset.length > 0) {
          console.log(`\nResult Set ${index + 1}:`);
          console.table(recordset);
        }
      });
    }
  } catch (error: any) {
    console.error('⚠️  Verification query failed:', error.message);
  }
}

/**
 * Main step-by-step execution
 */
async function main() {
  const args = process.argv.slice(2);
  const configFile = args.includes('--config') ? args[args.indexOf('--config') + 1] : undefined;
  
  const config = loadConfig(undefined, configFile);
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n════════════════════════════════════════════════════════');
    console.log('      TRANSFORM PHASE - STEP-BY-STEP EXECUTION         ');
    console.log('════════════════════════════════════════════════════════\n');
    console.log('This script will execute each transform step individually.');
    console.log('After each step, verification results will be shown.');
    console.log('You can review the results before continuing.\n');
    console.log(`📚 For detailed step descriptions and test queries, see:`);
    console.log(`   STEP-BY-STEP-TEST-GUIDE.md (comprehensive testing guide)`);
    console.log(`\nTotal steps: ${transformSteps.length}`);
    console.log('════════════════════════════════════════════════════════\n');
    
    for (let i = 0; i < transformSteps.length; i++) {
      const step = transformSteps[i];
      
      console.log('\n╔════════════════════════════════════════════════════════╗');
      console.log(`║  ${step.name.padEnd(54)}║`);
      console.log('╚════════════════════════════════════════════════════════╝');
      console.log(`\n📋 Description: ${step.description}`);
      console.log(`\n🎯 Purpose:\n   ${step.purpose.replace(/\n/g, '\n   ')}`);
      console.log(`\n✅ Expected Results:`);
      step.expectedResults.forEach(result => {
        console.log(`   • ${result}`);
      });
      console.log(`\n📄 Script: ${path.basename(step.script)}\n`);
      
      // Execute the script
      console.log('⏳ Executing...\n');
      const startTime = Date.now();
      
      try {
        const result = await executeSQLScript({
          config,
          pool,
          scriptPath: step.script,
          stepId: `manual-step-${i + 1}`,
          debugMode: false,
          pocMode: config.database.pocMode === true
        });
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`\n✅ Step completed in ${duration}s`);
        if (result.recordsAffected && result.recordsAffected > 0) {
          console.log(`📊 Records affected: ${result.recordsAffected.toLocaleString()}`);
        }
        
        // Run verification
        if (step.verification) {
          await runVerification(pool, step.verification, config.database.schemas.processing);
        }
        
        // Show test queries
        if (step.testQueries) {
          console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
          console.log('🔍 HOW TO TEST RESULTS:');
          console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
          console.log('\nCopy and run these queries to verify data quality:\n');
          // Replace schema variable for display
          const displayQueries = step.testQueries.replace(/\$\(ETL_SCHEMA\)/g, config.database.schemas.processing);
          console.log(displayQueries);
          console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        }
        
        // Ask to continue (except after last step)
        if (i < transformSteps.length - 1) {
          const shouldContinue = await askToContinue();
          
          if (!shouldContinue) {
            console.log('\n⏸️  Execution paused by user.');
            console.log('Remaining steps can be run individually or via full pipeline.\n');
            process.exit(0);
          }
        }
        
      } catch (error: any) {
        console.error(`\n❌ Step failed: ${error.message}`);
        console.error('\nYou can:');
        console.error('  1. Fix the issue in the SQL script');
        console.error('  2. Re-run this script to retry from this step');
        console.error('  3. Run individual SQL script: sqlcmd -i ' + step.script);
        console.error('  4. Check logs for detailed error information');
        process.exit(1);
      }
    }
    
    console.log('\n════════════════════════════════════════════════════════');
    console.log('       ✅ ALL TRANSFORM STEPS COMPLETED!               ');
    console.log('════════════════════════════════════════════════════════\n');
    console.log('Next steps:');
    console.log('  1. Review final verification results above');
    console.log('  2. Run export: npx tsx scripts/run-pipeline.ts --skip-ingest --skip-transform');
    console.log('  3. Or manual export: See EXPORT-STEP-BY-STEP-GUIDE.md\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
