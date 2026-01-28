/**
 * Proposal Consolidation Algorithm
 * 
 * Consolidates pre-stage proposals using iterative in-memory algorithm with
 * explicit rules. Provides full audit trail and human-readable logic.
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from '../lib/config-loader';

interface PrestageProposal {
  Id: string;
  ProposalNumber: string;
  Status: number;
  SubmittedDate: Date;
  ProposedEffectiveDate: Date;
  SpecialCase: boolean;
  SpecialCaseCode: number;
  SitusState: string | null;
  ProductId: string | null;
  ProductName: string | null;
  BrokerId: number | null;
  BrokerUniquePartyId: string | null;
  BrokerName: string | null;
  GroupId: string;
  GroupName: string | null;
  ContractId: string | null;
  RejectionReason: string | null;
  Notes: string | null;
  ProductCodes: string;
  PlanCodes: string;
  SplitConfigHash: string;
  DateRangeFrom: number;
  DateRangeTo: number | null;
  PlanCodeConstraints: string | null;
  EnablePlanCodeFiltering: boolean;
  EffectiveDateFrom: Date;
  EffectiveDateTo: Date | null;
  EnableEffectiveDateFiltering: boolean;
  ConstrainingEffectiveDateFrom: Date | null;
  ConstrainingEffectiveDateTo: Date | null;
  SplitConfigurationJSON: string | null;
  SplitConfigurationMD5: string | null;
  IsRetained: boolean;
  ConsumedByProposalId: string | null;
  ConsolidationReason: string | null;
  CreationTime: Date;
  IsDeleted: boolean;
}

interface ConsolidationResult {
  retained: PrestageProposal[];
  consumed: Map<string, { proposalId: string; reason: string }>;
}

/**
 * Main consolidation function
 * @param pool SQL connection pool
 * @param processingSchema Schema to use for staging tables (e.g., 'etl' or 'poc_etl')
 */
export async function consolidateProposals(pool: sql.ConnectionPool, processingSchema: string = 'etl'): Promise<void> {
  console.log('\n🔄 Starting proposal consolidation...');
  console.log(`   Using schema: ${processingSchema}`);
  console.log('═'.repeat(60));
  
  try {
    // Step 1: Load all pre-stage proposals into memory
    console.log('\n📥 Step 1: Loading pre-stage proposals...');
    const proposals = await loadPrestageProposals(pool);
    console.log(`   ✓ Loaded ${proposals.length} pre-stage proposals`);
    
    // Step 2: Sort by GroupId, EffectiveDateFrom, SplitConfigurationMD5
    console.log('\n🔀 Step 2: Sorting proposals...');
    proposals.sort((a, b) => {
      if (a.GroupId !== b.GroupId) return a.GroupId.localeCompare(b.GroupId);
      if (a.EffectiveDateFrom.getTime() !== b.EffectiveDateFrom.getTime()) 
        return a.EffectiveDateFrom.getTime() - b.EffectiveDateFrom.getTime();
      return (a.SplitConfigurationMD5 || '').localeCompare(b.SplitConfigurationMD5 || '');
    });
    console.log(`   ✓ Proposals sorted by GroupId → EffectiveDateFrom → SplitConfigurationMD5`);
    
    // Step 3: Consolidate using iterative algorithm
    console.log('\n🔨 Step 3: Running iterative consolidation algorithm...');
    const result = consolidateIteratively(proposals);
    console.log(`   ✓ Consolidated: ${result.retained.length} retained, ${result.consumed.size} consumed`);
    
    // Step 4: Update pre-stage with consolidation results
    console.log('\n💾 Step 4: Updating pre-stage with consolidation flags...');
    await updatePrestageWithConsolidation(pool, result);
    console.log(`   ✓ Updated ${result.retained.length} retained proposals`);
    console.log(`   ✓ Updated ${result.consumed.size} consumed proposals`);
    
    // Step 5: Copy retained proposals to staging
    console.log('\n📤 Step 5: Copying retained proposals to staging...');
    await copyToStaging(pool, result.retained, processingSchema);
    console.log(`   ✓ Copied ${result.retained.length} proposals to staging`);
    
    // Step 5.5: Assign intelligent display names
    console.log('\n🏷️  Step 5.5: Assigning display names...');
    await assignDisplayNames(pool, processingSchema);
    console.log(`   ✓ Assigned display names to ${result.retained.length} proposals`);
    
    // Step 6: Copy hierarchies to staging
    console.log('\n🏗️  Step 6: Copying hierarchies to staging...');
    await copyHierarchiesToStaging(pool, processingSchema);
    const hierarchyStats = await getHierarchyStats(pool, processingSchema);
    console.log(`   ✓ Copied ${hierarchyStats.hierarchies} hierarchies`);
    console.log(`   ✓ Copied ${hierarchyStats.hierarchyVersions} hierarchy versions`);
    console.log(`   ✓ Copied ${hierarchyStats.hierarchyParticipants} hierarchy participants`);
    
    // Step 7: Copy split data for retained proposals
    console.log('\n📋 Step 7: Copying split versions and participants...');
    await copySplitDataToStaging(pool, processingSchema);
    const splitStats = await getSplitDataStats(pool, processingSchema);
    console.log(`   ✓ Copied ${splitStats.splitVersions} split versions`);
    console.log(`   ✓ Copied ${splitStats.splitParticipants} split participants`);
    
    console.log('\n═'.repeat(60));
    console.log('✅ Consolidation complete!\n');
    
  } catch (error) {
    console.error('\n❌ Consolidation failed:', error);
    throw error;
  }
}

/**
 * Iterative consolidation algorithm
 */
function consolidateIteratively(proposals: PrestageProposal[]): ConsolidationResult {
  const retained: PrestageProposal[] = [];
  const consumed = new Map<string, { proposalId: string; reason: string }>();
  
  let currentGroup: string | null = null;
  let retainedProposal: PrestageProposal | null = null;
  const retainedProductCodes = new Set<string>();
  const retainedPlanCodes = new Set<string>();
  
  let consolidationCount = 0;
  
  for (const proposal of proposals) {
    // Rule 1: Different group → close retained, start new
    if (currentGroup !== proposal.GroupId) {
      if (retainedProposal) {
        retained.push(retainedProposal);
      }
      
      currentGroup = proposal.GroupId;
      retainedProposal = { ...proposal };
      retainedProductCodes.clear();
      retainedPlanCodes.clear();
      parseCodesIntoSet(proposal.ProductCodes, retainedProductCodes);
      parseCodesIntoSet(proposal.PlanCodes, retainedPlanCodes);
      continue;
    }
    
    // Rule 2: Different split config → close retained, start new
    if (retainedProposal!.SplitConfigurationMD5 !== proposal.SplitConfigurationMD5) {
      retained.push(retainedProposal!);
      retainedProposal = { ...proposal };
      retainedProductCodes.clear();
      retainedPlanCodes.clear();
      parseCodesIntoSet(proposal.ProductCodes, retainedProductCodes);
      parseCodesIntoSet(proposal.PlanCodes, retainedPlanCodes);
      continue;
    }
    
    // Rule 3: Conflicting plan codes → close retained, start new
    const proposalPlanCodes = new Set<string>();
    parseCodesIntoSet(proposal.PlanCodes, proposalPlanCodes);
    
    if (hasPlanConflict(retainedPlanCodes, proposalPlanCodes)) {
      retained.push(retainedProposal!);
      retainedProposal = { ...proposal };
      retainedProductCodes.clear();
      retainedPlanCodes.clear();
      parseCodesIntoSet(proposal.ProductCodes, retainedProductCodes);
      parseCodesIntoSet(proposal.PlanCodes, retainedPlanCodes);
      continue;
    }
    
    // Rule 4: Same config → extend dates and accumulate products
    // Merge non-contiguous date ranges (e.g., 2020-2021 + 2022-2023 = 2020-2023)
    retainedProposal!.DateRangeFrom = Math.min(
      retainedProposal!.DateRangeFrom, 
      proposal.DateRangeFrom
    );
    retainedProposal!.DateRangeTo = proposal.DateRangeTo === null 
      ? null 
      : retainedProposal!.DateRangeTo === null
        ? proposal.DateRangeTo
        : Math.max(retainedProposal!.DateRangeTo, proposal.DateRangeTo);
        
    retainedProposal!.EffectiveDateFrom = new Date(
      Math.min(
        retainedProposal!.EffectiveDateFrom.getTime(),
        proposal.EffectiveDateFrom.getTime()
      )
    );
    
    if (proposal.EffectiveDateTo) {
      if (!retainedProposal!.EffectiveDateTo) {
        retainedProposal!.EffectiveDateTo = proposal.EffectiveDateTo;
      } else {
        retainedProposal!.EffectiveDateTo = new Date(
          Math.max(
            retainedProposal!.EffectiveDateTo.getTime(),
            proposal.EffectiveDateTo.getTime()
          )
        );
      }
    }
    
    // Accumulate product codes
    parseCodesIntoSet(proposal.ProductCodes, retainedProductCodes);
    retainedProposal!.ProductCodes = JSON.stringify([...retainedProductCodes].sort());
    
    // Accumulate plan codes
    parseCodesIntoSet(proposal.PlanCodes, retainedPlanCodes);
    retainedProposal!.PlanCodes = JSON.stringify([...retainedPlanCodes].sort());
    
    // Mark as consumed
    consumed.set(proposal.Id, {
      proposalId: retainedProposal!.Id,
      reason: 'Same split configuration, extended date range and accumulated products'
    });
    
    consolidationCount++;
    
    if (consolidationCount % 100 === 0) {
      process.stdout.write(`\r   Processed: ${consolidationCount} consolidations`);
    }
  }
  
  // Don't forget the last retained proposal
  if (retainedProposal) {
    retained.push(retainedProposal);
  }
  
  if (consolidationCount > 0) {
    console.log(`\r   Processed: ${consolidationCount} consolidations`);
  }
  
  return { retained, consumed };
}

/**
 * Check if two plan code sets have a conflict
 */
function hasPlanConflict(set1: Set<string>, set2: Set<string>): boolean {
  // Wildcard (*) matches everything, so no conflict
  if (set1.has('*') || set2.has('*')) return false;
  
  // Check if any plan code in set2 is already in set1 but with different context
  // If sets overlap but aren't identical, it's a conflict
  const intersection = new Set([...set1].filter(x => set2.has(x)));
  if (intersection.size === 0) return false;  // No overlap = no conflict
  if (intersection.size === set1.size && intersection.size === set2.size) return false;  // Identical = no conflict
  return true;  // Partial overlap = conflict
}

/**
 * Parse JSON code array into a Set
 */
function parseCodesIntoSet(jsonStr: string, targetSet: Set<string>): void {
  if (jsonStr === '*') {
    targetSet.add('*');
    return;
  }
  try {
    const arr = JSON.parse(jsonStr);
    if (Array.isArray(arr)) {
      arr.forEach((code: string) => targetSet.add(code));
    } else {
      targetSet.add(jsonStr);
    }
  } catch (e) {
    // Invalid JSON, treat as single code
    targetSet.add(jsonStr);
  }
}

/**
 * Load all pre-stage proposals from database
 */
async function loadPrestageProposals(pool: sql.ConnectionPool): Promise<PrestageProposal[]> {
  const result = await pool.request().query<PrestageProposal>(`
    SELECT 
      Id, ProposalNumber, [Status], SubmittedDate, ProposedEffectiveDate,
      SpecialCase, SpecialCaseCode, SitusState, ProductId, ProductName,
      BrokerId, BrokerUniquePartyId, BrokerName, GroupId, GroupName,
      ContractId, RejectionReason, Notes, ProductCodes, PlanCodes,
      SplitConfigHash, DateRangeFrom, DateRangeTo, PlanCodeConstraints,
      EnablePlanCodeFiltering, EffectiveDateFrom, EffectiveDateTo,
      EnableEffectiveDateFiltering, ConstrainingEffectiveDateFrom,
      ConstrainingEffectiveDateTo, SplitConfigurationJSON, SplitConfigurationMD5,
      IsRetained, ConsumedByProposalId, ConsolidationReason,
      CreationTime, IsDeleted
    FROM [prestage].[prestage_proposals]
    ORDER BY GroupId, EffectiveDateFrom, SplitConfigurationMD5
  `);
  return result.recordset;
}

/**
 * Update pre-stage proposals with consolidation flags
 */
async function updatePrestageWithConsolidation(
  pool: sql.ConnectionPool, 
  result: ConsolidationResult
): Promise<void> {
  // Use batch updates for better performance
  const batchSize = 100;
  
  // Update retained proposals
  for (let i = 0; i < result.retained.length; i += batchSize) {
    const batch = result.retained.slice(i, i + batchSize);
    const ids = batch.map(p => p.Id).join("','");
    
    await pool.request().query(`
      UPDATE [prestage].[prestage_proposals] 
      SET IsRetained = 1 
      WHERE Id IN ('${ids}')
    `);
  }
  
  // Update consumed proposals (use CASE statement for batch updates)
  const consumedArray = Array.from(result.consumed.entries());
  for (let i = 0; i < consumedArray.length; i += batchSize) {
    const batch = consumedArray.slice(i, i + batchSize);
    const ids = batch.map(([id]) => `'${id}'`).join(',');
    
    // Build CASE statements for ConsumedByProposalId and ConsolidationReason
    const consumedByCases = batch.map(([id, { proposalId: retainedId }]) => 
      `WHEN Id = '${id}' THEN '${retainedId.replace(/'/g, "''")}'`
    ).join(' ');
    
    const reasonCases = batch.map(([id, { reason }]) => 
      `WHEN Id = '${id}' THEN '${reason.replace(/'/g, "''")}'`
    ).join(' ');
    
    await pool.request().query(`
      UPDATE [prestage].[prestage_proposals] 
      SET 
        IsRetained = 0,
        ConsumedByProposalId = CASE ${consumedByCases} END,
        ConsolidationReason = CASE ${reasonCases} END
      WHERE Id IN (${ids})
    `);
  }
}

/**
 * Copy retained proposals to staging
 */
async function copyToStaging(
  pool: sql.ConnectionPool, 
  retained: PrestageProposal[],
  processingSchema: string
): Promise<void> {
  // Truncate staging proposals
  await pool.request().query(`TRUNCATE TABLE [${processingSchema}].[stg_proposals]`);
  
  // Insert retained proposals into staging (batch for performance)
  const batchSize = 50;
  
  for (let i = 0; i < retained.length; i += batchSize) {
    const batch = retained.slice(i, i + batchSize);
    
    // Build bulk INSERT statement
    const values = batch.map(p => `(
      '${p.Id.replace(/'/g, "''")}',
      '${p.ProposalNumber.replace(/'/g, "''")}',
      ${p.Status},
      '${p.SubmittedDate.toISOString()}',
      '${p.ProposedEffectiveDate.toISOString()}',
      ${p.SpecialCase ? 1 : 0},
      ${p.SpecialCaseCode},
      ${p.SitusState ? `'${p.SitusState.replace(/'/g, "''")}'` : 'NULL'},
      ${p.ProductId ? `'${p.ProductId.replace(/'/g, "''")}'` : 'NULL'},
      ${p.ProductName ? `'${p.ProductName.replace(/'/g, "''")}'` : 'NULL'},
      ${p.BrokerId || 'NULL'},
      ${p.BrokerUniquePartyId ? `'${p.BrokerUniquePartyId.replace(/'/g, "''")}'` : 'NULL'},
      ${p.BrokerName ? `'${p.BrokerName.replace(/'/g, "''")}'` : 'NULL'},
      '${p.GroupId.replace(/'/g, "''")}',
      ${p.GroupName ? `'${p.GroupName.replace(/'/g, "''")}'` : 'NULL'},
      ${p.ContractId ? `'${p.ContractId.replace(/'/g, "''")}'` : 'NULL'},
      ${p.RejectionReason ? `'${p.RejectionReason.replace(/'/g, "''")}'` : 'NULL'},
      ${p.Notes ? `'${p.Notes.replace(/'/g, "''")}'` : 'NULL'},
      '${p.ProductCodes.replace(/'/g, "''")}',
      '${p.PlanCodes.replace(/'/g, "''")}',
      '${p.SplitConfigHash.replace(/'/g, "''")}',
      ${p.DateRangeFrom},
      ${p.DateRangeTo || 'NULL'},
      ${p.PlanCodeConstraints ? `'${p.PlanCodeConstraints.replace(/'/g, "''")}'` : 'NULL'},
      ${p.EnablePlanCodeFiltering ? 1 : 0},
      '${p.EffectiveDateFrom.toISOString()}',
      ${p.EffectiveDateTo ? `'${p.EffectiveDateTo.toISOString()}'` : 'NULL'},
      ${p.EnableEffectiveDateFiltering ? 1 : 0},
      ${p.ConstrainingEffectiveDateFrom ? `'${p.ConstrainingEffectiveDateFrom.toISOString()}'` : 'NULL'},
      ${p.ConstrainingEffectiveDateTo ? `'${p.ConstrainingEffectiveDateTo.toISOString()}'` : 'NULL'},
      GETUTCDATE(),
      0
    )`).join(',\n');
    
    await pool.request().query(`
      INSERT INTO [${processingSchema}].[stg_proposals] (
        Id, ProposalNumber, [Status], SubmittedDate, ProposedEffectiveDate,
        SpecialCase, SpecialCaseCode, SitusState, ProductId, ProductName,
        BrokerId, BrokerUniquePartyId, BrokerName, GroupId, GroupName,
        ContractId, RejectionReason, Notes, ProductCodes, PlanCodes,
        SplitConfigHash, DateRangeFrom, DateRangeTo, PlanCodeConstraints,
        EnablePlanCodeFiltering, EffectiveDateFrom, EffectiveDateTo,
        EnableEffectiveDateFiltering, ConstrainingEffectiveDateFrom,
        ConstrainingEffectiveDateTo, CreationTime, IsDeleted
      )
      VALUES ${values}
    `);
  }
}

/**
 * Assign intelligent display names to proposals using SQL window functions
 * Format: {GroupName or GroupId} - {EffectiveDateFrom YYYY-MM-DD} - {SequenceNumber}
 */
async function assignDisplayNames(pool: sql.ConnectionPool, processingSchema: string): Promise<void> {
  await pool.request().query(`
    ;WITH ProposalSequence AS (
      SELECT 
        p.Id,
        p.GroupId,
        COALESCE(g.Name, p.GroupId) AS GroupName,
        CONVERT(VARCHAR(10), p.EffectiveDateFrom, 23) AS EffectiveDate,
        ROW_NUMBER() OVER (
          PARTITION BY p.GroupId 
          ORDER BY p.EffectiveDateFrom, p.Id
        ) AS SequenceNum
      FROM [${processingSchema}].[stg_proposals] p
      LEFT JOIN [${processingSchema}].[stg_groups] g ON g.Id = p.GroupId
    )
    UPDATE p
    SET DisplayName = LEFT(ps.GroupName + ' - ' + ps.EffectiveDate + ' - ' + CAST(ps.SequenceNum AS VARCHAR), 100)
    FROM [${processingSchema}].[stg_proposals] p
    INNER JOIN ProposalSequence ps ON ps.Id = p.Id
  `);
}

/**
 * Copy hierarchies to staging
 */
async function copyHierarchiesToStaging(pool: sql.ConnectionPool, processingSchema: string): Promise<void> {
  // Truncate staging hierarchy tables
  await pool.request().query(`
    TRUNCATE TABLE [${processingSchema}].[stg_hierarchy_participants];
    TRUNCATE TABLE [${processingSchema}].[stg_hierarchy_versions];
    TRUNCATE TABLE [${processingSchema}].[stg_hierarchies];
  `);
  
  // Copy all hierarchies (hierarchies are not consolidated, only proposals are)
  await pool.request().query(`
    INSERT INTO [${processingSchema}].[stg_hierarchies] 
    SELECT * FROM [prestage].[prestage_hierarchies]
  `);
  
  await pool.request().query(`
    INSERT INTO [${processingSchema}].[stg_hierarchy_versions]
    SELECT * FROM [prestage].[prestage_hierarchy_versions]
  `);
  
  await pool.request().query(`
    INSERT INTO [${processingSchema}].[stg_hierarchy_participants]
    SELECT * FROM [prestage].[prestage_hierarchy_participants]
  `);
}

/**
 * Get hierarchy statistics
 */
async function getHierarchyStats(pool: sql.ConnectionPool, processingSchema: string): Promise<{ hierarchies: number; hierarchyVersions: number; hierarchyParticipants: number }> {
  const hierarchiesResult = await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM [${processingSchema}].[stg_hierarchies]
  `);
  const versionsResult = await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM [${processingSchema}].[stg_hierarchy_versions]
  `);
  const participantsResult = await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM [${processingSchema}].[stg_hierarchy_participants]
  `);
  
  return {
    hierarchies: hierarchiesResult.recordset[0].cnt,
    hierarchyVersions: versionsResult.recordset[0].cnt,
    hierarchyParticipants: participantsResult.recordset[0].cnt
  };
}

/**
 * Copy split versions and participants for retained proposals
 */
async function copySplitDataToStaging(pool: sql.ConnectionPool, processingSchema: string): Promise<void> {
  // Truncate staging split tables
  await pool.request().query(`
    TRUNCATE TABLE [${processingSchema}].[stg_premium_split_participants];
    TRUNCATE TABLE [${processingSchema}].[stg_premium_split_versions];
  `);
  
  // Copy split versions for retained proposals
  await pool.request().query(`
    INSERT INTO [${processingSchema}].[stg_premium_split_versions] 
    SELECT * FROM [prestage].[prestage_premium_split_versions]
    WHERE ProposalId IN (
      SELECT Id FROM [prestage].[prestage_proposals] WHERE IsRetained = 1
    )
  `);
  
  // Copy split participants
  await pool.request().query(`
    INSERT INTO [${processingSchema}].[stg_premium_split_participants]
    SELECT psp.* 
    FROM [prestage].[prestage_premium_split_participants] psp
    INNER JOIN [${processingSchema}].[stg_premium_split_versions] psv ON psv.Id = psp.VersionId
  `);
}

/**
 * Get split data statistics
 */
async function getSplitDataStats(pool: sql.ConnectionPool, processingSchema: string): Promise<{ splitVersions: number; splitParticipants: number }> {
  const versionsResult = await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM [${processingSchema}].[stg_premium_split_versions]
  `);
  const participantsResult = await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM [${processingSchema}].[stg_premium_split_participants]
  `);
  
  return {
    splitVersions: versionsResult.recordset[0].cnt,
    splitParticipants: participantsResult.recordset[0].cnt
  };
}

// Main execution
if (require.main === module) {
  (async () => {
    const config = loadConfig();
    const sqlConfig = getSqlConfig(config);
    const pool = await sql.connect(sqlConfig);
    
    try {
      await consolidateProposals(pool);
      process.exit(0);
    } catch (error) {
      console.error('Fatal error:', error);
      process.exit(1);
    } finally {
      await pool.close();
    }
  })();
}
