/**
 * Safe Export: Proposal Chain Tables
 * 
 * Exports proposals, hierarchies, splits, PHAs, and policies to production.
 * SAFE: Only deletes/replaces data for groups that exist in staging.
 * Other groups' data is preserved.
 * 
 * Usage:
 *   npx tsx scripts/export-proposals-safe.ts [options]
 * 
 * Options:
 *   --groups G1234,G5678   Only export specific groups
 *   --dry-run              Show what would be done without making changes
 *   --target-schema dbo    Target schema (default: dbo)
 *   --source-schema etl    Source schema (default: etl)
 */

import * as sql from 'mssql';

interface ExportOptions {
  groups?: string[];
  dryRun: boolean;
  targetSchema: string;
  sourceSchema: string;
}

function parseConnectionString(connStr: string) {
  const parts: Record<string, string> = {};
  connStr.split(';').forEach(part => {
    const [key, ...valueParts] = part.split('=');
    if (key && valueParts.length > 0) {
      parts[key.trim().toLowerCase()] = valueParts.join('=').trim();
    }
  });
  return {
    server: parts['server'] || parts['data source'],
    database: parts['database'] || parts['initial catalog'],
    user: parts['user id'] || parts['uid'],
    password: parts['password'] || parts['pwd'],
    options: {
      encrypt: true,
      trustServerCertificate: true,
      enableArithAbort: true,
    },
    pool: { max: 10, min: 0, idleTimeoutMillis: 30000 },
    requestTimeout: 600000,
  };
}

async function getGroupsToExport(pool: sql.ConnectionPool, options: ExportOptions): Promise<string[]> {
  if (options.groups && options.groups.length > 0) {
    return options.groups;
  }
  
  // Get all distinct groups from staging proposals
  const result = await pool.request().query(`
    SELECT DISTINCT GroupId FROM [${options.sourceSchema}].[stg_proposals]
    WHERE GroupId IS NOT NULL
    ORDER BY GroupId
  `);
  return result.recordset.map(r => r.GroupId);
}

async function safeDelete(
  pool: sql.ConnectionPool, 
  table: string, 
  groupColumn: string,
  groups: string[],
  options: ExportOptions
): Promise<number> {
  const target = `[${options.targetSchema}].[${table}]`;
  const groupList = groups.map(g => `'${g}'`).join(',');
  
  if (options.dryRun) {
    const countResult = await pool.request().query(`
      SELECT COUNT(*) AS cnt FROM ${target} WHERE ${groupColumn} IN (${groupList})
    `);
    return countResult.recordset[0].cnt;
  }
  
  const result = await pool.request().query(`
    DELETE FROM ${target} WHERE ${groupColumn} IN (${groupList})
  `);
  return result.rowsAffected[0] || 0;
}

async function safeDeleteByFK(
  pool: sql.ConnectionPool,
  table: string,
  fkColumn: string,
  parentTable: string,
  parentGroupColumn: string,
  groups: string[],
  options: ExportOptions
): Promise<number> {
  const target = `[${options.targetSchema}].[${table}]`;
  const parent = `[${options.targetSchema}].[${parentTable}]`;
  const groupList = groups.map(g => `'${g}'`).join(',');
  
  if (options.dryRun) {
    const countResult = await pool.request().query(`
      SELECT COUNT(*) AS cnt FROM ${target} t
      WHERE EXISTS (SELECT 1 FROM ${parent} p WHERE p.Id = t.${fkColumn} AND p.${parentGroupColumn} IN (${groupList}))
    `);
    return countResult.recordset[0].cnt;
  }
  
  const result = await pool.request().query(`
    DELETE t FROM ${target} t
    WHERE EXISTS (SELECT 1 FROM ${parent} p WHERE p.Id = t.${fkColumn} AND p.${parentGroupColumn} IN (${groupList}))
  `);
  return result.rowsAffected[0] || 0;
}

async function exportTable(
  pool: sql.ConnectionPool,
  tableName: string,
  columns: string[],
  whereClause: string,
  options: ExportOptions
): Promise<number> {
  const target = `[${options.targetSchema}].[${tableName}]`;
  const source = `[${options.sourceSchema}].[stg_${tableName.toLowerCase().replace(/([a-z])([A-Z])/g, '$1_$2').toLowerCase()}]`;
  
  // Build column list - add CreationTime and IsDeleted if not in source
  const selectCols = columns.join(', ');
  const insertCols = columns.includes('CreationTime') ? selectCols : `${selectCols}, CreationTime, IsDeleted`;
  const selectExpr = columns.includes('CreationTime') ? selectCols : `${selectCols}, GETUTCDATE(), 0`;
  
  const query = `
    INSERT INTO ${target} (${insertCols})
    SELECT ${selectExpr}
    FROM ${source}
    ${whereClause}
  `;
  
  if (options.dryRun) {
    const countQuery = `SELECT COUNT(*) AS cnt FROM ${source} ${whereClause}`;
    const result = await pool.request().query(countQuery);
    return result.recordset[0].cnt;
  }
  
  const result = await pool.request().query(query);
  return result.rowsAffected[0] || 0;
}

async function main() {
  const args = process.argv.slice(2);
  
  const options: ExportOptions = {
    dryRun: args.includes('--dry-run'),
    targetSchema: args.includes('--target-schema') 
      ? args[args.indexOf('--target-schema') + 1] 
      : 'dbo',
    sourceSchema: args.includes('--source-schema')
      ? args[args.indexOf('--source-schema') + 1]
      : 'etl',
    groups: undefined
  };
  
  if (args.includes('--groups')) {
    const groupsArg = args[args.indexOf('--groups') + 1];
    if (groupsArg && !groupsArg.startsWith('--')) {
      options.groups = groupsArg.split(',').map(g => g.trim());
    }
  }
  
  const connStr = process.env.SQLSERVER;
  if (!connStr) {
    throw new Error('SQLSERVER environment variable not set');
  }
  
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║  SAFE EXPORT: Proposal Chain Tables                            ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');
  console.log('');
  console.log(`Source Schema: ${options.sourceSchema}`);
  console.log(`Target Schema: ${options.targetSchema}`);
  if (options.dryRun) console.log('MODE: DRY RUN (no changes will be made)');
  console.log('');
  
  const pool = await sql.connect(parseConnectionString(connStr));
  
  try {
    // Get groups to export
    const groups = await getGroupsToExport(pool, options);
    
    if (groups.length === 0) {
      console.log('No groups found in staging to export.');
      return;
    }
    
    console.log(`Groups to export: ${groups.length}`);
    if (groups.length <= 20) {
      console.log(`  ${groups.join(', ')}`);
    } else {
      console.log(`  ${groups.slice(0, 20).join(', ')}... and ${groups.length - 20} more`);
    }
    console.log('');
    
    const groupList = groups.map(g => `'${g}'`).join(',');
    
    // ========================================================================
    // STEP 1: DELETE existing data for these groups (in FK order - children first)
    // ========================================================================
    console.log('═'.repeat(60));
    console.log('STEP 1: Deleting existing data for specified groups');
    console.log('═'.repeat(60));
    
    // PolicyHierarchyAssignments - delete by hierarchy's GroupId
    let count = await safeDeleteByFK(pool, 'PolicyHierarchyAssignments', 'HierarchyId', 'Hierarchies', 'GroupId', groups, options);
    console.log(`  ${options.dryRun ? 'Would delete' : 'Deleted'} ${count} from PolicyHierarchyAssignments`);
    
    // HierarchyParticipants - delete by hierarchy's GroupId  
    count = await safeDeleteByFK(pool, 'HierarchyParticipants', 'HierarchyId', 'Hierarchies', 'GroupId', groups, options);
    console.log(`  ${options.dryRun ? 'Would delete' : 'Deleted'} ${count} from HierarchyParticipants`);
    
    // HierarchyVersions - delete by hierarchy's GroupId
    count = await safeDeleteByFK(pool, 'HierarchyVersions', 'HierarchyId', 'Hierarchies', 'GroupId', groups, options);
    console.log(`  ${options.dryRun ? 'Would delete' : 'Deleted'} ${count} from HierarchyVersions`);
    
    // Hierarchies
    count = await safeDelete(pool, 'Hierarchies', 'GroupId', groups, options);
    console.log(`  ${options.dryRun ? 'Would delete' : 'Deleted'} ${count} from Hierarchies`);
    
    // PremiumSplitParticipants
    count = await safeDelete(pool, 'PremiumSplitParticipants', 'GroupId', groups, options);
    console.log(`  ${options.dryRun ? 'Would delete' : 'Deleted'} ${count} from PremiumSplitParticipants`);
    
    // PremiumSplitVersions
    count = await safeDelete(pool, 'PremiumSplitVersions', 'GroupId', groups, options);
    console.log(`  ${options.dryRun ? 'Would delete' : 'Deleted'} ${count} from PremiumSplitVersions`);
    
    // Policies
    count = await safeDelete(pool, 'Policies', 'GroupId', groups, options);
    console.log(`  ${options.dryRun ? 'Would delete' : 'Deleted'} ${count} from Policies`);
    
    // ProposalProducts - delete by proposal's GroupId
    count = await safeDeleteByFK(pool, 'ProposalProducts', 'ProposalId', 'Proposals', 'GroupId', groups, options);
    console.log(`  ${options.dryRun ? 'Would delete' : 'Deleted'} ${count} from ProposalProducts`);
    
    // Proposals
    count = await safeDelete(pool, 'Proposals', 'GroupId', groups, options);
    console.log(`  ${options.dryRun ? 'Would delete' : 'Deleted'} ${count} from Proposals`);
    
    if (options.dryRun) {
      console.log('\n[DRY RUN] Skipping inserts...\n');
      
      // Show what would be inserted
      console.log('═'.repeat(60));
      console.log('STEP 2: Would insert from staging');
      console.log('═'.repeat(60));
      
      // Count what would be inserted
      let result = await pool.request().query(`SELECT COUNT(*) AS cnt FROM [${options.sourceSchema}].[stg_proposals] WHERE GroupId IN (${groupList})`);
      console.log(`  Would insert ${result.recordset[0].cnt} into Proposals`);
      
      result = await pool.request().query(`SELECT COUNT(*) AS cnt FROM [${options.sourceSchema}].[stg_proposal_products] pp INNER JOIN [${options.sourceSchema}].[stg_proposals] p ON p.Id = pp.ProposalId WHERE p.GroupId IN (${groupList})`);
      console.log(`  Would insert ${result.recordset[0].cnt} into ProposalProducts`);
      
      result = await pool.request().query(`SELECT COUNT(*) AS cnt FROM [${options.sourceSchema}].[stg_premium_split_versions] WHERE GroupId IN (${groupList})`);
      console.log(`  Would insert ${result.recordset[0].cnt} into PremiumSplitVersions`);
      
      result = await pool.request().query(`SELECT COUNT(*) AS cnt FROM [${options.sourceSchema}].[stg_premium_split_participants] WHERE GroupId IN (${groupList})`);
      console.log(`  Would insert ${result.recordset[0].cnt} into PremiumSplitParticipants`);
      
      result = await pool.request().query(`SELECT COUNT(*) AS cnt FROM [${options.sourceSchema}].[stg_hierarchies] WHERE GroupId IN (${groupList})`);
      console.log(`  Would insert ${result.recordset[0].cnt} into Hierarchies`);
      
      result = await pool.request().query(`SELECT COUNT(*) AS cnt FROM [${options.sourceSchema}].[stg_hierarchy_versions] hv INNER JOIN [${options.sourceSchema}].[stg_hierarchies] h ON h.Id = hv.HierarchyId WHERE h.GroupId IN (${groupList})`);
      console.log(`  Would insert ${result.recordset[0].cnt} into HierarchyVersions`);
      
      result = await pool.request().query(`SELECT COUNT(*) AS cnt FROM [${options.sourceSchema}].[stg_hierarchy_participants] hp INNER JOIN [${options.sourceSchema}].[stg_hierarchy_versions] hv ON hv.Id = hp.HierarchyVersionId INNER JOIN [${options.sourceSchema}].[stg_hierarchies] h ON h.Id = hv.HierarchyId WHERE h.GroupId IN (${groupList})`);
      console.log(`  Would insert ${result.recordset[0].cnt} into HierarchyParticipants`);
      
      result = await pool.request().query(`SELECT COUNT(*) AS cnt FROM [${options.sourceSchema}].[stg_policies] WHERE GroupId IN (${groupList})`);
      console.log(`  Would insert ${result.recordset[0].cnt} into Policies`);
      
      result = await pool.request().query(`SELECT COUNT(*) AS cnt FROM [${options.sourceSchema}].[stg_policy_hierarchy_assignments] pha INNER JOIN [${options.sourceSchema}].[stg_hierarchies] h ON h.Id = pha.HierarchyId WHERE h.GroupId IN (${groupList})`);
      console.log(`  Would insert ${result.recordset[0].cnt} into PolicyHierarchyAssignments`);
      
      console.log('\n✅ DRY RUN complete - no changes made');
      return;
    }
    
    // ========================================================================
    // STEP 2: INSERT from staging
    // ========================================================================
    console.log('\n' + '═'.repeat(60));
    console.log('STEP 2: Inserting from staging');
    console.log('═'.repeat(60));
    
    // Proposals
    let insertCount = await pool.request().query(`
      INSERT INTO [${options.targetSchema}].[Proposals] (
        Id, ProposalNumber, Status, SubmittedDate, ProposedEffectiveDate,
        SitusState, GroupId, GroupName, ProductCodes, PlanCodes,
        SplitConfigHash, DateRangeFrom, DateRangeTo,
        EffectiveDateFrom, EffectiveDateTo, Notes,
        CreationTime, IsDeleted
      )
      SELECT 
        Id, ProposalNumber, Status, SubmittedDate, ProposedEffectiveDate,
        SitusState, GroupId, GroupName, ProductCodes, PlanCodes,
        SplitConfigHash, DateRangeFrom, DateRangeTo,
        EffectiveDateFrom, EffectiveDateTo, Notes,
        GETUTCDATE(), 0
      FROM [${options.sourceSchema}].[stg_proposals]
      WHERE GroupId IN (${groupList})
    `);
    console.log(`  Inserted ${insertCount.rowsAffected[0]} Proposals`);
    
    // ProposalProducts
    insertCount = await pool.request().query(`
      INSERT INTO [${options.targetSchema}].[ProposalProducts] (
        Id, ProposalId, ProductId, ProductCode, ProductName,
        PlanCode, PlanName, EffectiveDate,
        CreationTime, IsDeleted
      )
      SELECT 
        pp.Id, pp.ProposalId, pp.ProductId, pp.ProductCode, pp.ProductName,
        pp.PlanCode, pp.PlanName, pp.EffectiveDate,
        GETUTCDATE(), 0
      FROM [${options.sourceSchema}].[stg_proposal_products] pp
      INNER JOIN [${options.sourceSchema}].[stg_proposals] p ON p.Id = pp.ProposalId
      WHERE p.GroupId IN (${groupList})
    `);
    console.log(`  Inserted ${insertCount.rowsAffected[0]} ProposalProducts`);
    
    // PremiumSplitVersions
    insertCount = await pool.request().query(`
      INSERT INTO [${options.targetSchema}].[PremiumSplitVersions] (
        Id, GroupId, GroupName, ProposalId, ProposalNumber,
        VersionNumber, EffectiveFrom, EffectiveTo,
        TotalSplitPercent, Status,
        CreationTime, IsDeleted
      )
      SELECT 
        Id, GroupId, GroupName, ProposalId, ProposalNumber,
        VersionNumber, EffectiveFrom, EffectiveTo,
        TotalSplitPercent, Status,
        GETUTCDATE(), 0
      FROM [${options.sourceSchema}].[stg_premium_split_versions]
      WHERE GroupId IN (${groupList})
    `);
    console.log(`  Inserted ${insertCount.rowsAffected[0]} PremiumSplitVersions`);
    
    // PremiumSplitParticipants
    insertCount = await pool.request().query(`
      INSERT INTO [${options.targetSchema}].[PremiumSplitParticipants] (
        Id, VersionId, BrokerId, BrokerName, SplitPercent,
        IsWritingAgent, HierarchyId, Sequence, WritingBrokerId,
        GroupId, EffectiveFrom, EffectiveTo,
        CreationTime, IsDeleted
      )
      SELECT 
        Id, VersionId, BrokerId, BrokerName, SplitPercent,
        IsWritingAgent, HierarchyId, Sequence, WritingBrokerId,
        GroupId, EffectiveFrom, EffectiveTo,
        GETUTCDATE(), 0
      FROM [${options.sourceSchema}].[stg_premium_split_participants]
      WHERE GroupId IN (${groupList})
    `);
    console.log(`  Inserted ${insertCount.rowsAffected[0]} PremiumSplitParticipants`);
    
    // Hierarchies
    insertCount = await pool.request().query(`
      INSERT INTO [${options.targetSchema}].[Hierarchies] (
        Id, Name, GroupId, GroupName, BrokerId, BrokerName,
        ProposalId, SitusState, CurrentVersionId, Status,
        CreationTime, IsDeleted
      )
      SELECT 
        Id, Name, GroupId, GroupName, BrokerId, BrokerName,
        ProposalId, SitusState, CurrentVersionId, Status,
        GETUTCDATE(), 0
      FROM [${options.sourceSchema}].[stg_hierarchies]
      WHERE GroupId IN (${groupList})
    `);
    console.log(`  Inserted ${insertCount.rowsAffected[0]} Hierarchies`);
    
    // HierarchyVersions
    insertCount = await pool.request().query(`
      INSERT INTO [${options.targetSchema}].[HierarchyVersions] (
        Id, HierarchyId, VersionNumber, EffectiveFrom, EffectiveTo, Status,
        CreationTime, IsDeleted
      )
      SELECT 
        hv.Id, hv.HierarchyId, hv.VersionNumber, hv.EffectiveFrom, hv.EffectiveTo, hv.Status,
        GETUTCDATE(), 0
      FROM [${options.sourceSchema}].[stg_hierarchy_versions] hv
      INNER JOIN [${options.sourceSchema}].[stg_hierarchies] h ON h.Id = hv.HierarchyId
      WHERE h.GroupId IN (${groupList})
    `);
    console.log(`  Inserted ${insertCount.rowsAffected[0]} HierarchyVersions`);
    
    // HierarchyParticipants
    insertCount = await pool.request().query(`
      INSERT INTO [${options.targetSchema}].[HierarchyParticipants] (
        Id, HierarchyId, HierarchyVersionId, BrokerId, BrokerName,
        Level, SplitPercent, ScheduleId, ScheduleCode,
        CreationTime, IsDeleted
      )
      SELECT 
        hp.Id, hv.HierarchyId, hp.HierarchyVersionId, hp.EntityId, hp.EntityName,
        hp.Level, hp.SplitPercent, hp.ScheduleId, hp.ScheduleCode,
        GETUTCDATE(), 0
      FROM [${options.sourceSchema}].[stg_hierarchy_participants] hp
      INNER JOIN [${options.sourceSchema}].[stg_hierarchy_versions] hv ON hv.Id = hp.HierarchyVersionId
      INNER JOIN [${options.sourceSchema}].[stg_hierarchies] h ON h.Id = hv.HierarchyId
      WHERE h.GroupId IN (${groupList})
    `);
    console.log(`  Inserted ${insertCount.rowsAffected[0]} HierarchyParticipants`);
    
    // Policies
    insertCount = await pool.request().query(`
      INSERT INTO [${options.targetSchema}].[Policies] (
        Id, PolicyNumber, GroupId, GroupName, ProductCode, PlanCode,
        EffectiveDate, TerminationDate, Status, BrokerId, ProposalId,
        SitusState, ProposalAssignmentSource,
        CreationTime, IsDeleted
      )
      SELECT 
        Id, PolicyNumber, GroupId, GroupName, ProductCode, PlanCode,
        EffectiveDate, TerminationDate, Status, BrokerId, ProposalId,
        [State], ProposalAssignmentSource,
        GETUTCDATE(), 0
      FROM [${options.sourceSchema}].[stg_policies]
      WHERE GroupId IN (${groupList})
    `);
    console.log(`  Inserted ${insertCount.rowsAffected[0]} Policies`);
    
    // PolicyHierarchyAssignments
    insertCount = await pool.request().query(`
      INSERT INTO [${options.targetSchema}].[PolicyHierarchyAssignments] (
        Id, PolicyId, HierarchyId, SplitSequence, SplitPercent,
        WritingBrokerId, IsNonConforming, NonConformantReason,
        CreationTime, IsDeleted
      )
      SELECT 
        pha.Id, pha.PolicyId, pha.HierarchyId, pha.SplitSequence, pha.SplitPercent,
        pha.WritingBrokerId, pha.IsNonConforming, pha.NonConformantReason,
        GETUTCDATE(), 0
      FROM [${options.sourceSchema}].[stg_policy_hierarchy_assignments] pha
      INNER JOIN [${options.sourceSchema}].[stg_hierarchies] h ON h.Id = pha.HierarchyId
      WHERE h.GroupId IN (${groupList})
    `);
    console.log(`  Inserted ${insertCount.rowsAffected[0]} PolicyHierarchyAssignments`);
    
    console.log('\n' + '═'.repeat(60));
    console.log('✅ EXPORT COMPLETE');
    console.log('═'.repeat(60));
    
  } finally {
    await pool.close();
  }
}

main()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ Error:', err.message || err);
    process.exit(1);
  });
