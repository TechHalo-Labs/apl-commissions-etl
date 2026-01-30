/**
 * Bulk Insert Optimized Write Function for Proposal Builder
 * 
 * This module provides an optimized writeStagingOutput function that uses
 * SQL Server's native bulk insert protocol (TDS) for maximum performance.
 * 
 * Performance comparison:
 * - Batched INSERTs: ~89 round trips for 8,871 proposals = ~30-60 seconds
 * - Bulk insert: 1 round trip per table (9 total) = ~5-10 seconds
 */

import * as sql from 'mssql';
import { DatabaseConfig, BuilderOptions, StagingOutput } from './proposal-builder';

function brokerExternalToInternal(externalId: string): number {
  const numStr = externalId.replace(/^P/, '');
  const num = parseInt(numStr, 10);
  return isNaN(num) ? 0 : num;
}

export async function writeStagingOutputBulk(
  config: DatabaseConfig,
  output: StagingOutput,
  options: BuilderOptions
): Promise<void> {
  const schema = options.schema || 'etl';
  
  if (options.dryRun) {
    console.log('[DRY RUN] Would write staging output:');
    console.log(`  Proposals: ${output.proposals.length}`);
    console.log(`  Key Mappings: ${output.proposalKeyMappings.length}`);
    console.log(`  Split Versions: ${output.premiumSplitVersions.length}`);
    console.log(`  Split Participants: ${output.premiumSplitParticipants.length}`);
    console.log(`  Hierarchies: ${output.hierarchies.length}`);
    console.log(`  Hierarchy Versions: ${output.hierarchyVersions.length}`);
    console.log(`  Hierarchy Participants: ${output.hierarchyParticipants.length}`);
    console.log(`  PHA Assignments: ${output.policyHierarchyAssignments.length}`);
    console.log(`  PHA Participants: ${output.policyHierarchyParticipants.length}`);
    return;
  }

  const pool = await sql.connect(config);
  const now = new Date();

  try {
    if (options.verbose) {
      console.log('Writing staging output to database (bulk inserts)...');
    }
    const startTime = Date.now();

    // Clear existing data
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_proposals]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_proposal_key_mapping]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_premium_split_versions]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_premium_split_participants]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_hierarchies]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_hierarchy_versions]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_hierarchy_participants]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_policy_hierarchy_assignments]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_policy_hierarchy_participants]`);

    // 1. Bulk insert proposals
    if (output.proposals.length > 0) {
      const table = new sql.Table(`[${schema}].[stg_proposals]`);
      table.columns.add('Id', sql.NVarChar(100), { nullable: false });
      table.columns.add('ProposalNumber', sql.NVarChar(100), { nullable: true });
      table.columns.add('Status', sql.Int, { nullable: false });
      table.columns.add('SubmittedDate', sql.DateTime2, { nullable: true });
      table.columns.add('ProposedEffectiveDate', sql.DateTime2, { nullable: true });
      table.columns.add('SitusState', sql.NVarChar(10), { nullable: true });
      table.columns.add('GroupId', sql.NVarChar(100), { nullable: true });
      table.columns.add('GroupName', sql.NVarChar(500), { nullable: true });
      table.columns.add('ProductCodes', sql.NVarChar(sql.MAX), { nullable: true });
      table.columns.add('PlanCodes', sql.NVarChar(sql.MAX), { nullable: true });
      table.columns.add('SplitConfigHash', sql.NVarChar(64), { nullable: false });
      table.columns.add('DateRangeFrom', sql.Int, { nullable: true });
      table.columns.add('DateRangeTo', sql.Int, { nullable: true });
      table.columns.add('EffectiveDateFrom', sql.DateTime2, { nullable: true });
      table.columns.add('EffectiveDateTo', sql.DateTime2, { nullable: true });
      table.columns.add('Notes', sql.NVarChar(sql.MAX), { nullable: true });
      table.columns.add('CreationTime', sql.DateTime2, { nullable: false });
      table.columns.add('IsDeleted', sql.Bit, { nullable: false });
      
      for (const p of output.proposals) {
        table.rows.add(
          p.Id, p.ProposalNumber, p.Status, p.SubmittedDate, p.ProposedEffectiveDate,
          p.SitusState, p.GroupId, p.GroupName, p.ProductCodes, p.PlanCodes,
          p.SplitConfigHash, p.DateRangeFrom, p.DateRangeTo,
          p.EffectiveDateFrom, p.EffectiveDateTo, p.Notes,
          now, false
        );
      }
      
      await pool.request().bulk(table);
      
      if (options.verbose) {
        console.log(`  ✓ Bulk inserted ${output.proposals.length} proposals`);
      }
    }

    // 2. Bulk insert key mappings
    if (output.proposalKeyMappings.length > 0) {
      const table = new sql.Table(`[${schema}].[stg_proposal_key_mapping]`);
      table.columns.add('GroupId', sql.NVarChar(100), { nullable: false });
      table.columns.add('EffectiveYear', sql.Int, { nullable: false });
      table.columns.add('ProductCode', sql.NVarChar(100), { nullable: false });
      table.columns.add('PlanCode', sql.NVarChar(100), { nullable: false });
      table.columns.add('ProposalId', sql.NVarChar(100), { nullable: false });
      table.columns.add('SplitConfigHash', sql.NVarChar(64), { nullable: false });
      table.columns.add('CreationTime', sql.DateTime2, { nullable: false });
      
      for (const m of output.proposalKeyMappings) {
        table.rows.add(
          m.GroupId, m.EffectiveYear, m.ProductCode, m.PlanCode,
          m.ProposalId, m.SplitConfigHash, now
        );
      }
      
      await pool.request().bulk(table);
      
      if (options.verbose) {
        console.log(`  ✓ Bulk inserted ${output.proposalKeyMappings.length} key mappings`);
      }
    }

    // 3. Bulk insert split versions
    if (output.premiumSplitVersions.length > 0) {
      const table = new sql.Table(`[${schema}].[stg_premium_split_versions]`);
      table.columns.add('Id', sql.NVarChar(100), { nullable: false });
      table.columns.add('GroupId', sql.NVarChar(100), { nullable: true });
      table.columns.add('GroupName', sql.NVarChar(500), { nullable: true });
      table.columns.add('ProposalId', sql.NVarChar(100), { nullable: false });
      table.columns.add('ProposalNumber', sql.NVarChar(100), { nullable: true });
      table.columns.add('VersionNumber', sql.NVarChar(50), { nullable: false });
      table.columns.add('EffectiveFrom', sql.DateTime2, { nullable: true });
      table.columns.add('EffectiveTo', sql.DateTime2, { nullable: true });
      table.columns.add('TotalSplitPercent', sql.Decimal(18, 4), { nullable: false });
      table.columns.add('Status', sql.Int, { nullable: false });
      table.columns.add('CreationTime', sql.DateTime2, { nullable: false });
      table.columns.add('IsDeleted', sql.Bit, { nullable: false });
      
      for (const v of output.premiumSplitVersions) {
        table.rows.add(
          v.Id, v.GroupId, v.GroupName, v.ProposalId, v.ProposalNumber,
          v.VersionNumber, v.EffectiveFrom, v.EffectiveTo, v.TotalSplitPercent, v.Status,
          now, false
        );
      }
      
      await pool.request().bulk(table);
      
      if (options.verbose) {
        console.log(`  ✓ Bulk inserted ${output.premiumSplitVersions.length} split versions`);
      }
    }

    // 4. Bulk insert split participants
    if (output.premiumSplitParticipants.length > 0) {
      const table = new sql.Table(`[${schema}].[stg_premium_split_participants]`);
      table.columns.add('Id', sql.NVarChar(100), { nullable: false });
      table.columns.add('VersionId', sql.NVarChar(100), { nullable: false });
      table.columns.add('BrokerId', sql.BigInt, { nullable: false });
      table.columns.add('BrokerName', sql.NVarChar(500), { nullable: true });
      table.columns.add('SplitPercent', sql.Decimal(18, 4), { nullable: false });
      table.columns.add('IsWritingAgent', sql.Bit, { nullable: false });
      table.columns.add('HierarchyId', sql.NVarChar(100), { nullable: true });
      table.columns.add('Sequence', sql.Int, { nullable: false });
      table.columns.add('WritingBrokerId', sql.BigInt, { nullable: true });
      table.columns.add('GroupId', sql.NVarChar(100), { nullable: true });
      table.columns.add('EffectiveFrom', sql.DateTime2, { nullable: true });
      table.columns.add('EffectiveTo', sql.DateTime2, { nullable: true });
      table.columns.add('CreationTime', sql.DateTime2, { nullable: false });
      table.columns.add('IsDeleted', sql.Bit, { nullable: false });
      
      for (const p of output.premiumSplitParticipants) {
        table.rows.add(
          p.Id, p.VersionId, p.BrokerId, p.BrokerName, p.SplitPercent,
          p.IsWritingAgent, p.HierarchyId, p.Sequence, p.WritingBrokerId, p.GroupId,
          p.EffectiveFrom, p.EffectiveTo, now, false
        );
      }
      
      await pool.request().bulk(table);
      
      if (options.verbose) {
        console.log(`  ✓ Bulk inserted ${output.premiumSplitParticipants.length} split participants`);
      }
    }

    // 5. Bulk insert hierarchies
    if (output.hierarchies.length > 0) {
      const table = new sql.Table(`[${schema}].[stg_hierarchies]`);
      table.columns.add('Id', sql.NVarChar(100), { nullable: false });
      table.columns.add('Name', sql.NVarChar(500), { nullable: false });
      table.columns.add('GroupId', sql.NVarChar(100), { nullable: true });
      table.columns.add('GroupName', sql.NVarChar(500), { nullable: true });
      table.columns.add('BrokerId', sql.BigInt, { nullable: false });
      table.columns.add('BrokerName', sql.NVarChar(500), { nullable: true });
      table.columns.add('ProposalId', sql.NVarChar(100), { nullable: true });
      table.columns.add('CurrentVersionId', sql.NVarChar(100), { nullable: true });
      table.columns.add('EffectiveDate', sql.Date, { nullable: false });
      table.columns.add('SitusState', sql.NVarChar(10), { nullable: true });
      table.columns.add('Status', sql.Int, { nullable: false });
      table.columns.add('CreationTime', sql.DateTime2, { nullable: false });
      table.columns.add('IsDeleted', sql.Bit, { nullable: false });
      
      for (const h of output.hierarchies) {
        table.rows.add(
          h.Id, h.Name, h.GroupId, h.GroupName, h.BrokerId, h.BrokerName,
          h.ProposalId, h.CurrentVersionId, h.EffectiveDate, h.SitusState, 1,
          now, false
        );
      }
      
      await pool.request().bulk(table);
      
      if (options.verbose) {
        console.log(`  ✓ Bulk inserted ${output.hierarchies.length} hierarchies`);
      }
    }

    // 6. Bulk insert hierarchy versions
    if (output.hierarchyVersions.length > 0) {
      const table = new sql.Table(`[${schema}].[stg_hierarchy_versions]`);
      table.columns.add('Id', sql.NVarChar(100), { nullable: false });
      table.columns.add('HierarchyId', sql.NVarChar(100), { nullable: false });
      table.columns.add('Version', sql.Int, { nullable: false });
      table.columns.add('EffectiveFrom', sql.DateTime2, { nullable: true });
      table.columns.add('EffectiveTo', sql.DateTime2, { nullable: true });
      table.columns.add('Status', sql.Int, { nullable: false });
      table.columns.add('CreationTime', sql.DateTime2, { nullable: false });
      table.columns.add('IsDeleted', sql.Bit, { nullable: false });
      
      for (const hv of output.hierarchyVersions) {
        table.rows.add(
          hv.Id, hv.HierarchyId, 1, hv.EffectiveFrom, hv.EffectiveTo, hv.Status,
          now, false
        );
      }
      
      await pool.request().bulk(table);
      
      if (options.verbose) {
        console.log(`  ✓ Bulk inserted ${output.hierarchyVersions.length} hierarchy versions`);
      }
    }

    // 7. Bulk insert hierarchy participants
    if (output.hierarchyParticipants.length > 0) {
      const table = new sql.Table(`[${schema}].[stg_hierarchy_participants]`);
      table.columns.add('Id', sql.NVarChar(100), { nullable: false });
      table.columns.add('HierarchyVersionId', sql.NVarChar(100), { nullable: false });
      table.columns.add('EntityId', sql.BigInt, { nullable: false });
      table.columns.add('EntityName', sql.NVarChar(500), { nullable: true });
      table.columns.add('Level', sql.Int, { nullable: false });
      table.columns.add('SortOrder', sql.Int, { nullable: false });
      table.columns.add('SplitPercent', sql.Decimal(18, 4), { nullable: false });
      table.columns.add('CommissionRate', sql.Decimal(18, 4), { nullable: true });
      table.columns.add('ScheduleCode', sql.NVarChar(100), { nullable: true });
      table.columns.add('ScheduleId', sql.BigInt, { nullable: true });
      table.columns.add('CreationTime', sql.DateTime2, { nullable: false });
      table.columns.add('IsDeleted', sql.Bit, { nullable: false });
      
      for (const hp of output.hierarchyParticipants) {
        table.rows.add(
          hp.Id, hp.HierarchyVersionId, brokerExternalToInternal(hp.EntityId), hp.EntityName,
          hp.Level, hp.Level, 0, hp.CommissionRate, hp.ScheduleCode, hp.ScheduleId,
          now, false
        );
      }
      
      await pool.request().bulk(table);
      
      if (options.verbose) {
        console.log(`  ✓ Bulk inserted ${output.hierarchyParticipants.length} hierarchy participants`);
      }
    }

    // 8. Bulk insert PHA assignments
    if (output.policyHierarchyAssignments.length > 0) {
      const table = new sql.Table(`[${schema}].[stg_policy_hierarchy_assignments]`);
      table.columns.add('Id', sql.NVarChar(100), { nullable: false });
      table.columns.add('PolicyId', sql.NVarChar(100), { nullable: false });
      table.columns.add('CertificateId', sql.BigInt, { nullable: true });
      table.columns.add('HierarchyId', sql.NVarChar(100), { nullable: true });
      table.columns.add('SplitPercent', sql.Decimal(18, 4), { nullable: false });
      table.columns.add('WritingBrokerId', sql.BigInt, { nullable: false });
      table.columns.add('SplitSequence', sql.Int, { nullable: false });
      table.columns.add('IsNonConforming', sql.Bit, { nullable: true });
      table.columns.add('NonConformantReason', sql.NVarChar(500), { nullable: true });
      table.columns.add('SourceTraceabilityReportId', sql.BigInt, { nullable: true });
      table.columns.add('CreationTime', sql.DateTime2, { nullable: true });
      table.columns.add('IsDeleted', sql.Bit, { nullable: true });
      
      let phaIdCounter = 0;
      for (const pha of output.policyHierarchyAssignments) {
        phaIdCounter++;
        table.rows.add(
          `PHA-${phaIdCounter}`, pha.PolicyId, null, null,
          pha.SplitPercent, pha.WritingBrokerId, pha.SplitSequence,
          null, pha.NonConformantReason, null, now, false
        );
      }
      
      await pool.request().bulk(table);
      
      if (options.verbose) {
        console.log(`  ✓ Bulk inserted ${output.policyHierarchyAssignments.length} PHA assignments`);
      }
    }

    // 9. Bulk insert PHA participants
    if (output.policyHierarchyParticipants.length > 0) {
      const table = new sql.Table(`[${schema}].[stg_policy_hierarchy_participants]`);
      table.columns.add('Id', sql.NVarChar(100), { nullable: false });
      table.columns.add('PolicyHierarchyAssignmentId', sql.NVarChar(100), { nullable: false });
      table.columns.add('BrokerId', sql.BigInt, { nullable: false });
      table.columns.add('BrokerName', sql.NVarChar(500), { nullable: true });
      table.columns.add('Level', sql.Int, { nullable: false });
      table.columns.add('ScheduleCode', sql.NVarChar(100), { nullable: true });
      table.columns.add('CreationTime', sql.DateTime2, { nullable: false });
      table.columns.add('IsDeleted', sql.Bit, { nullable: false });
      
      for (const php of output.policyHierarchyParticipants) {
        table.rows.add(
          php.Id, php.PolicyHierarchyAssignmentId, brokerExternalToInternal(php.BrokerId),
          php.BrokerName, php.Level, php.ScheduleCode, now, false
        );
      }
      
      await pool.request().bulk(table);
      
      if (options.verbose) {
        console.log(`  ✓ Bulk inserted ${output.policyHierarchyParticipants.length} PHA participants`);
      }
    }

    if (options.verbose) {
      console.log(`  ✅ Completed in ${Date.now() - startTime}ms`);
    }
  } finally {
    await pool.close();
  }
}
