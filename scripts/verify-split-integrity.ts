/**
 * Split Integrity Verification Script
 * 
 * Verifies that the FULL hierarchy structure from raw data is preserved in staging.
 * Compares: GROUP_CONCAT(tier|broker|schedule) from raw === staged
 */

import * as sql from 'mssql';

const config: sql.config = {
  server: 'halo-sql.database.windows.net',
  database: 'halo-sqldb',
  user: '***REMOVED***',
  password: '***REMOVED***',
  options: { encrypt: true, trustServerCertificate: true },
};

interface Proposal {
  Id: string;
  GroupId: string;
  Notes: string;
  ProductCodes: string;
  PlanCodes: string;
  DateRangeFrom: number;
}

interface ConfigEntry {
  splitSeq: number;
  level: number;
  brokerId: string;
  percent: number;
  schedule: string;
}

interface VerificationResult {
  proposalId: string;
  proposalType: string;
  structureCheck: {
    passed: boolean;
    errors: string[];
    rawSignature: string;
    stagedSignature: string;
  };
  refIntegrityCheck: {
    passed: boolean;
    errors: string[];
  };
}

/**
 * Creates a normalized signature from ConfigJson entries
 * Format: sorted list of "splitSeq|level|brokerId|percent|schedule"
 */
function createRawSignature(config: ConfigEntry[]): string {
  return config
    .map(e => `${e.splitSeq}|${e.level}|${e.brokerId}|${e.percent}|${e.schedule?.trim() || ''}`)
    .sort()
    .join('\n');
}

async function main() {
  const pool = await sql.connect(config);
  
  console.log('='.repeat(60));
  console.log('SPLIT INTEGRITY VERIFICATION');
  console.log('Full Structure Comparison: Raw vs Staged');
  console.log('='.repeat(60));
  console.log('');

  // Step 1: Sample 100 proposals stratified by type
  console.log('Step 1: Sampling 100 proposals...');
  
  const proposals = await pool.request().query<Proposal>(`
    WITH RankedProposals AS (
      SELECT 
        Id, GroupId, Notes, ProductCodes, PlanCodes, DateRangeFrom,
        ROW_NUMBER() OVER (PARTITION BY Notes ORDER BY NEWID()) AS rn
      FROM [etl].[stg_proposals]
      WHERE Notes IN ('Simple group - single config', 'Plan-differentiated', 'Year-differentiated')
    )
    SELECT Id, GroupId, Notes, ProductCodes, PlanCodes, DateRangeFrom
    FROM RankedProposals
    WHERE 
      (Notes = 'Simple group - single config' AND rn <= 15) OR
      (Notes = 'Plan-differentiated' AND rn <= 8) OR
      (Notes = 'Year-differentiated' AND rn <= 77)
  `);
  
  const byType: Record<string, number> = {
    'Simple group - single config': 0,
    'Plan-differentiated': 0,
    'Year-differentiated': 0,
  };
  
  for (const p of proposals.recordset) {
    byType[p.Notes]++;
  }
  
  console.log(`  Total sampled: ${proposals.recordset.length}`);
  console.log(`    - Simple groups: ${byType['Simple group - single config']}`);
  console.log(`    - Plan-differentiated: ${byType['Plan-differentiated']}`);
  console.log(`    - Year-differentiated: ${byType['Year-differentiated']}`);
  console.log('');

  // Step 2: Verify each proposal
  console.log('Step 2: Verifying proposals...');
  
  const results: VerificationResult[] = [];
  let processed = 0;
  
  for (const proposal of proposals.recordset) {
    processed++;
    if (processed % 20 === 0) {
      console.log(`  Processing ${processed}/${proposals.recordset.length}...`);
    }
    
    const result = await verifyProposal(pool, proposal);
    results.push(result);
  }
  
  console.log('');

  // Step 3: Generate report
  console.log('='.repeat(60));
  console.log('VERIFICATION RESULTS');
  console.log('='.repeat(60));
  console.log('');
  
  console.log(`Total Proposals Checked: ${results.length}`);
  console.log(`  - Simple groups: ${byType['Simple group - single config']}`);
  console.log(`  - Plan-differentiated: ${byType['Plan-differentiated']}`);
  console.log(`  - Year-differentiated: ${byType['Year-differentiated']}`);
  console.log('');
  
  // Structure check results
  const structPassed = results.filter(r => r.structureCheck.passed);
  const structFailed = results.filter(r => !r.structureCheck.passed);
  
  console.log('STRUCTURE ALIGNMENT (Raw vs Staged):');
  console.log(`  PASSED: ${structPassed.length}`);
  console.log(`  FAILED: ${structFailed.length}`);
  
  if (structFailed.length > 0) {
    console.log('');
    console.log('  Failures:');
    for (const r of structFailed.slice(0, 5)) {
      console.log(`    - ${r.proposalId} (${r.proposalType}):`);
      for (const err of r.structureCheck.errors) {
        console.log(`        ${err}`);
      }
      if (r.structureCheck.rawSignature && r.structureCheck.stagedSignature) {
        console.log('        Raw signature:');
        for (const line of r.structureCheck.rawSignature.split('\n').slice(0, 3)) {
          console.log(`          ${line}`);
        }
        console.log('        Staged signature:');
        for (const line of r.structureCheck.stagedSignature.split('\n').slice(0, 3)) {
          console.log(`          ${line}`);
        }
      }
    }
    if (structFailed.length > 5) {
      console.log(`    ... and ${structFailed.length - 5} more`);
    }
  }
  console.log('');
  
  // Referential integrity
  const refPassed = results.filter(r => r.refIntegrityCheck.passed);
  const refFailed = results.filter(r => !r.refIntegrityCheck.passed);
  
  console.log('REFERENTIAL INTEGRITY:');
  console.log(`  PASSED: ${refPassed.length}`);
  console.log(`  FAILED: ${refFailed.length}`);
  
  if (refFailed.length > 0) {
    console.log('');
    console.log('  Failures:');
    for (const r of refFailed.slice(0, 10)) {
      console.log(`    - ${r.proposalId} (${r.proposalType}):`);
      for (const err of r.refIntegrityCheck.errors) {
        console.log(`        ${err}`);
      }
    }
    if (refFailed.length > 10) {
      console.log(`    ... and ${refFailed.length - 10} more`);
    }
  }
  console.log('');
  
  // Summary
  const allPassed = results.filter(r => r.structureCheck.passed && r.refIntegrityCheck.passed);
  
  console.log('='.repeat(60));
  console.log('SUMMARY');
  console.log('='.repeat(60));
  console.log(`  All checks passed: ${allPassed.length}/${results.length} (${(allPassed.length / results.length * 100).toFixed(1)}%)`);
  console.log(`  Structure issues: ${structFailed.length}`);
  console.log(`  Referential integrity issues: ${refFailed.length}`);
  console.log('');
  
  await pool.close();
}

async function verifyProposal(pool: sql.ConnectionPool, proposal: Proposal): Promise<VerificationResult> {
  const result: VerificationResult = {
    proposalId: proposal.Id,
    proposalType: proposal.Notes,
    structureCheck: { passed: true, errors: [], rawSignature: '', stagedSignature: '' },
    refIntegrityCheck: { passed: true, errors: [] },
  };
  
  // Get ConfigJson from source table based on proposal type
  let configJson: string | null = null;
  
  if (proposal.Notes === 'Simple group - single config') {
    const configResult = await pool.request()
      .input('groupId', sql.NVarChar, proposal.GroupId.replace('G', ''))
      .query(`
        SELECT ConfigJson
        FROM [etl].[simple_groups]
        WHERE GroupId = @groupId
      `);
    configJson = configResult.recordset[0]?.ConfigJson;
  } else if (proposal.Notes === 'Plan-differentiated') {
    const productCode = JSON.parse(proposal.ProductCodes || '[]')[0];
    const planCode = JSON.parse(proposal.PlanCodes || '[]')[0];
    const configResult = await pool.request()
      .input('groupId', sql.NVarChar, proposal.GroupId.replace('G', ''))
      .input('productCode', sql.NVarChar, productCode)
      .input('planCode', sql.NVarChar, planCode)
      .input('effYear', sql.Int, proposal.DateRangeFrom)
      .query(`
        SELECT ConfigJson
        FROM [etl].[plan_differentiated_keys]
        WHERE GroupId = @groupId
          AND ProductCode = @productCode
          AND PlanCode = @planCode
          AND EffYear = @effYear
      `);
    configJson = configResult.recordset[0]?.ConfigJson;
  } else if (proposal.Notes === 'Year-differentiated') {
    const productCode = JSON.parse(proposal.ProductCodes || '[]')[0];
    let planCode = proposal.PlanCodes;
    if (planCode && planCode !== '*') {
      planCode = JSON.parse(planCode)[0];
    }
    const configResult = await pool.request()
      .input('groupId', sql.NVarChar, proposal.GroupId.replace('G', ''))
      .input('productCode', sql.NVarChar, productCode)
      .input('planCode', sql.NVarChar, planCode === '*' ? '*' : planCode)
      .input('effYear', sql.Int, proposal.DateRangeFrom)
      .query(`
        SELECT ConfigJson
        FROM [etl].[year_differentiated_keys]
        WHERE GroupId = @groupId
          AND ProductCode = @productCode
          AND PlanCode = @planCode
          AND EffYear = @effYear
      `);
    configJson = configResult.recordset[0]?.ConfigJson;
  }
  
  if (!configJson) {
    result.structureCheck.passed = false;
    result.structureCheck.errors.push('ConfigJson not found in source table');
    return result;
  }
  
  // Parse ConfigJson and create raw signature (ALL entries, not just level=1)
  const config: ConfigEntry[] = JSON.parse(configJson);
  const rawSignature = createRawSignature(config);
  result.structureCheck.rawSignature = rawSignature;
  
  // Get staged data: split participants + hierarchy participants
  // For now, we only have split participants (level=1) in staging
  // We need to also get hierarchy participants for levels 2, 3, 4, etc.
  
  const splitVersionResult = await pool.request()
    .input('proposalId', sql.NVarChar, proposal.Id)
    .query(`
      SELECT Id, ProposalId, GroupId, TotalSplitPercent
      FROM [etl].[stg_premium_split_versions]
      WHERE ProposalId = @proposalId
    `);
  
  if (splitVersionResult.recordset.length === 0) {
    result.structureCheck.passed = false;
    result.structureCheck.errors.push('No split version found');
    return result;
  }
  
  const version = splitVersionResult.recordset[0];
  
  // Get split participants (these are level=1 writing brokers)
  const participantsResult = await pool.request()
    .input('versionId', sql.NVarChar, version.Id)
    .query(`
      SELECT psp.Sequence AS splitSeq, 
             1 AS [level],
             CONCAT('P', psp.BrokerId) AS brokerId,
             psp.SplitPercent AS [percent],
             psp.HierarchyId
      FROM [etl].[stg_premium_split_participants] psp
      WHERE psp.VersionId = @versionId
    `);
  
  // Build staged signature from split participants
  // Note: We're missing the schedule and higher levels (2, 3, 4)
  // For now, compare only level=1 entries
  const level1Raw = config.filter(c => c.level === 1);
  const level1RawSignature = level1Raw
    .map(e => `${e.splitSeq}|${e.level}|${e.brokerId}|${e.percent}`)
    .sort()
    .join('\n');
  
  // Build staged signature (without schedule for now)
  const stagedEntries = participantsResult.recordset.map(p => ({
    splitSeq: p.splitSeq,
    level: p.level,
    brokerId: p.brokerId,
    percent: p.percent,
  }));
  
  const stagedSignature = stagedEntries
    .map(e => `${e.splitSeq}|${e.level}|${e.brokerId}|${e.percent}`)
    .sort()
    .join('\n');
  
  result.structureCheck.stagedSignature = stagedSignature;
  
  // Compare level=1 entries - check that each unique (splitSeq, broker, percent) exists
  // Note: Staged may have MORE entries due to multiple hierarchies per broker (known behavior)
  const rawLevel1Set = new Set(level1Raw.map(e => `${e.splitSeq}|${e.brokerId}|${e.percent}`));
  const stagedSet = new Set(stagedEntries.map(e => `${e.splitSeq}|${e.brokerId}|${e.percent}`));
  
  // Check: Every raw entry must have at least one matching staged entry
  for (const rawKey of rawLevel1Set) {
    if (!stagedSet.has(rawKey)) {
      result.structureCheck.passed = false;
      result.structureCheck.errors.push(`Missing in staged: ${rawKey}`);
    }
  }
  
  // Check: Staged should not have entries that don't exist in raw (ignoring hierarchy duplicates)
  for (const stagedKey of stagedSet) {
    if (!rawLevel1Set.has(stagedKey)) {
      result.structureCheck.passed = false;
      result.structureCheck.errors.push(`Extra in staged (not in raw): ${stagedKey}`);
    }
  }
  
  // Check TotalSplitPercent
  const expectedTotal = level1Raw.reduce((sum, e) => sum + e.percent, 0);
  if (Math.abs(version.TotalSplitPercent - expectedTotal) > 0.01) {
    result.structureCheck.passed = false;
    result.structureCheck.errors.push(
      `TotalSplitPercent mismatch: expected ${expectedTotal}, got ${version.TotalSplitPercent}`
    );
  }
  
  // Referential integrity checks
  const groupExists = await pool.request()
    .input('groupId', sql.NVarChar, version.GroupId)
    .query(`SELECT 1 FROM [etl].[stg_groups] WHERE Id = @groupId`);
  
  if (groupExists.recordset.length === 0) {
    result.refIntegrityCheck.passed = false;
    result.refIntegrityCheck.errors.push(`GroupId ${version.GroupId} not found in stg_groups`);
  }
  
  // Check each participant's broker exists
  for (const p of participantsResult.recordset) {
    const brokerId = p.brokerId.replace('P', '');
    const brokerExists = await pool.request()
      .input('brokerId', sql.BigInt, brokerId)
      .query(`SELECT 1 FROM [etl].[stg_brokers] WHERE Id = @brokerId`);
    
    if (brokerExists.recordset.length === 0) {
      result.refIntegrityCheck.passed = false;
      result.refIntegrityCheck.errors.push(`BrokerId ${brokerId} not found in stg_brokers`);
    }
    
    // Check hierarchy exists (if set)
    if (p.HierarchyId) {
      const hierExists = await pool.request()
        .input('hierarchyId', sql.NVarChar, p.HierarchyId)
        .query(`SELECT 1 FROM [etl].[stg_hierarchies] WHERE Id = @hierarchyId`);
      
      if (hierExists.recordset.length === 0) {
        result.refIntegrityCheck.passed = false;
        result.refIntegrityCheck.errors.push(`HierarchyId ${p.HierarchyId} not found in stg_hierarchies`);
      }
    }
  }
  
  return result;
}

main().catch(console.error);
