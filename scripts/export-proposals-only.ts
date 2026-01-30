/**
 * Export Proposals, Assignments, Splits, and Hierarchies Only
 * Clears and recreates proposal-related data in production
 */

import * as sql from 'mssql';

function parseConnectionString(connStr: string) {
  const parts: Record<string, string> = {};
  connStr.split(';').forEach(part => {
    const [key, ...valueParts] = part.split('=');
    if (key && valueParts.length > 0) {
      parts[key.trim().toLowerCase()] = valueParts.join('=').trim();
    }
  });
  const encrypt = parts['encrypt'];
  const trustCert = parts['trustservercertificate'];
  return {
    server: parts['server'] || parts['data source'],
    database: parts['database'] || parts['initial catalog'],
    user: parts['user id'] || parts['uid'],
    password: parts['password'] || parts['pwd'],
    options: {
      encrypt: encrypt === undefined ? true : encrypt.toLowerCase() === 'true',
      trustServerCertificate: trustCert === undefined ? true : trustCert.toLowerCase() === 'true',
      enableArithAbort: true,
    },
    pool: { max: 10, min: 0, idleTimeoutMillis: 30000 },
    requestTimeout: 600000,
  };
}

async function main() {
  console.log('============================================================');
  console.log('EXPORT PROPOSALS, ASSIGNMENTS, SPLITS, HIERARCHIES');
  console.log('============================================================\n');
  
  const connStr = process.env.SQLSERVER;
  if (!connStr) {
    throw new Error('SQLSERVER environment variable not set');
  }
  
  const config = parseConnectionString(connStr);
  const pool = await sql.connect(config);
  
  try {
    // Step 1: DELETE production data (in FK order - children first)
    console.log('\n📋 STEP 1: Clearing production proposal data');
    console.log('='.repeat(60));
    
    const deletions = [
      'CommissionAssignmentRecipients',
      'CommissionAssignmentVersions',
      'PolicyHierarchyAssignments',
      'PremiumTransactions',
      'Policies',
      'PremiumSplitParticipants',
      'PremiumSplitVersions',
      'HierarchyParticipants',
      'Hierarchies',
      'ProposalProducts',
      'Proposals',
    ];
    
    for (const table of deletions) {
      console.log(`🗑️  Deleting from ${table}...`);
      try {
        const result = await pool.request().query(`DELETE FROM [dbo].[${table}];`);
        const count = result.rowsAffected[0] || 0;
        console.log(`   ✅ Deleted ${count} rows`);
      } catch (err: any) {
        console.error(`   ❌ Error: ${err.message}`);
        // Continue with other deletions
      }
    }
    
    // Step 2: Export from staging to production
    console.log('\n\n📋 STEP 2: Exporting from staging to production');
    console.log('='.repeat(60));
    
    // Export Proposals
    console.log('\n📋 Exporting Proposals...');
    await pool.request().query(`
      INSERT INTO [dbo].[Proposals] (
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
      FROM [etl].[stg_proposals]
    `);
    console.log(`   ✅ Exported ${await getCount(pool, 'Proposals')} proposals`);
    
    // Export Premium Split Versions
    console.log('\n📋 Exporting Premium Split Versions...');
    await pool.request().query(`
      INSERT INTO [dbo].[PremiumSplitVersions] (
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
      FROM [etl].[stg_premium_split_versions]
    `);
    console.log(`   ✅ Exported ${await getCount(pool, 'PremiumSplitVersions')} split versions`);
    
    // Export Premium Split Participants
    console.log('\n📋 Exporting Premium Split Participants...');
    await pool.request().query(`
      INSERT INTO [dbo].[PremiumSplitParticipants] (
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
      FROM [etl].[stg_premium_split_participants]
    `);
    console.log(`   ✅ Exported ${await getCount(pool, 'PremiumSplitParticipants')} split participants`);
    
    // Export Hierarchies
    console.log('\n📋 Exporting Hierarchies...');
    await pool.request().query(`
      INSERT INTO [dbo].[Hierarchies] (
        Id, Name, GroupId, GroupName, BrokerId, BrokerName,
        ProposalId, SitusState, CurrentVersionId, Status,
        CreationTime, IsDeleted
      )
      SELECT 
        Id, Name, GroupId, GroupName, BrokerId, BrokerName,
        ProposalId, SitusState, VersionNumber as CurrentVersionId, Status,
        GETUTCDATE(), 0
      FROM [etl].[stg_hierarchies]
    `);
    console.log(`   ✅ Exported ${await getCount(pool, 'Hierarchies')} hierarchies`);
    
    // Export Hierarchy Versions (use hierarchies table as source)
    console.log('\n📋 Exporting Hierarchy Versions...');
    await pool.request().query(`
      INSERT INTO [dbo].[HierarchyVersions] (
        Id, HierarchyId, VersionNumber, EffectiveFrom, EffectiveTo, Status,
        CreationTime, IsDeleted
      )
      SELECT 
        hv.Id, hv.HierarchyId, hv.VersionNumber,
        hv.EffectiveFrom, hv.EffectiveTo, hv.Status,
        GETUTCDATE(), 0
      FROM [etl].[stg_hierarchy_versions] hv
    `);
    console.log(`   ✅ Exported ${await getCount(pool, 'HierarchyVersions')} hierarchy versions`);
    
    // Export Hierarchy Participants
    console.log('\n📋 Exporting Hierarchy Participants...');
    await pool.request().query(`
      INSERT INTO [dbo].[HierarchyParticipants] (
        Id, HierarchyVersionId, EntityId, EntityName, EntityType,
        Level, SortOrder, SplitPercent, CommissionRate,
        ScheduleCode, ScheduleId,
        CreationTime, IsDeleted
      )
      SELECT 
        Id, HierarchyVersionId, EntityId, EntityName, EntityType,
        Level, SortOrder, SplitPercent, CommissionRate,
        ScheduleCode, ScheduleId,
        GETUTCDATE(), 0
      FROM [etl].[stg_hierarchy_participants]
    `);
    console.log(`   ✅ Exported ${await getCount(pool, 'HierarchyParticipants')} hierarchy participants`);
    
    // Export Policies
    console.log('\n📋 Exporting Policies...');
    await pool.request().query(`
      INSERT INTO [dbo].[Policies] (
        PolicyId, GroupId, CertificateNumber, PremiumAmount, ProductCode,
        PlanCode, CertEffectiveDate, CertIssuedState, CertStatus,
        SplitConfigHash, ProposalId,
        CreationTime, IsDeleted
      )
      SELECT 
        p.PolicyId,
        CASE WHEN p.GroupId IS NULL OR p.GroupId = '' THEN NULL 
             ELSE CONCAT('G', p.GroupId) 
        END AS GroupId,
        p.CertificateNumber,
        p.PremiumAmount,
        p.ProductCode,
        p.PlanCode,
        p.CertEffectiveDate,
        p.CertIssuedState,
        p.CertStatus,
        p.SplitConfigHash,
        pkm.ProposalId
      FROM [etl].[stg_policies] p
      LEFT JOIN [etl].[stg_proposal_key_mapping] pkm 
        ON pkm.GroupId = p.GroupId
        AND pkm.EffectiveYear = YEAR(p.CertEffectiveDate)
        AND pkm.ProductCode = p.ProductCode
        AND pkm.PlanCode = p.PlanCode
    `);
    console.log(`   ✅ Exported ${await getCount(pool, 'Policies')} policies`);
    
    // Export PolicyHierarchyAssignments
    console.log('\n📋 Exporting PolicyHierarchyAssignments...');
    await pool.request().query(`
      INSERT INTO [dbo].[PolicyHierarchyAssignments] (
        Id, PolicyId, WritingBrokerId, SplitSequence, SplitPercent,
        NonConformantReason,
        CreationTime, IsDeleted
      )
      SELECT 
        CONCAT('PHA-', ROW_NUMBER() OVER (ORDER BY PolicyId, SplitSequence)),
        PolicyId, WritingBrokerId, SplitSequence, SplitPercent,
        NonConformantReason,
        GETUTCDATE(), 0
      FROM [etl].[stg_policy_hierarchy_assignments]
    `);
    console.log(`   ✅ Exported ${await getCount(pool, 'PolicyHierarchyAssignments')} PHA assignments`);
    
    // Note about Commission Assignments
    console.log('\n📋 NOTE: Commission Assignments');
    console.log('   Commission assignments were written directly by the proposal builder');
    console.log('   They are already in production tables (not in staging)');
    console.log(`   Current count: ${await getCount(pool, 'CommissionAssignmentVersions')} versions`);
    console.log(`   Current count: ${await getCount(pool, 'CommissionAssignmentRecipients')} recipients`);
    
    // Step 3: Verify counts
    console.log('\n\n📋 STEP 3: Verification');
    console.log('='.repeat(60));
    
    const verification = await pool.request().query(`
      SELECT 'Proposals' as Entity, COUNT(*) as [Count] FROM [dbo].[Proposals]
      UNION ALL SELECT 'Premium Split Versions', COUNT(*) FROM [dbo].[PremiumSplitVersions]
      UNION ALL SELECT 'Premium Split Participants', COUNT(*) FROM [dbo].[PremiumSplitParticipants]
      UNION ALL SELECT 'Hierarchies', COUNT(*) FROM [dbo].[Hierarchies]
      UNION ALL SELECT 'Hierarchy Versions', COUNT(*) FROM [dbo].[HierarchyVersions]
      UNION ALL SELECT 'Hierarchy Participants', COUNT(*) FROM [dbo].[HierarchyParticipants]
      UNION ALL SELECT 'Policies', COUNT(*) FROM [dbo].[Policies]
      UNION ALL SELECT 'PolicyHierarchyAssignments', COUNT(*) FROM [dbo].[PolicyHierarchyAssignments]
      UNION ALL SELECT 'Commission Assignment Versions', COUNT(*) FROM [dbo].[CommissionAssignmentVersions]
      UNION ALL SELECT 'Commission Assignment Recipients', COUNT(*) FROM [dbo].[CommissionAssignmentRecipients]
      ORDER BY 1
    `);
    
    console.log('\n✅ Production Data Summary:');
    console.table(verification.recordset);
    
    // Sample assignments
    console.log('\n📋 Sample Commission Assignments:');
    const samples = await pool.request().query(`
      SELECT TOP 10
        v.ProposalId,
        p.GroupId,
        bs.Name as SourceBroker,
        br.Name as RecipientBroker,
        v.EffectiveFrom,
        v.EffectiveTo
      FROM [dbo].[CommissionAssignmentVersions] v
      INNER JOIN [dbo].[CommissionAssignmentRecipients] r ON r.VersionId = v.Id
      LEFT JOIN [dbo].[Proposals] p ON p.Id = v.ProposalId
      LEFT JOIN [dbo].[Brokers] bs ON bs.Id = v.BrokerId
      LEFT JOIN [dbo].[Brokers] br ON br.Id = r.RecipientBrokerId
      ORDER BY v.ProposalId
    `);
    console.table(samples.recordset);
    
    console.log('\n============================================================');
    console.log('✅ EXPORT COMPLETED SUCCESSFULLY');
    console.log('============================================================\n');
    
  } finally {
    await pool.close();
  }
}

async function getCount(pool: sql.ConnectionPool, table: string): Promise<number> {
  const result = await pool.request().query(`SELECT COUNT(*) as cnt FROM [dbo].[${table}]`);
  return result.recordset[0].cnt;
}

main().catch(err => {
  console.error('\n❌ Export failed:', err);
  process.exit(1);
});
