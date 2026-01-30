/**
 * Fix Policy Hierarchy Assignments
 * 
 * This script addresses the issue where PHAs (PolicyHierarchyAssignments) have
 * no HierarchyId - they should have fully compliant, commission-ready hierarchies.
 * 
 * The Problem:
 * - proposal-builder.ts creates PHAs for "invalid groups" (null/empty/zeros)
 * - But it doesn't create hierarchies for them - HierarchyId is NULL
 * - This breaks commission calculations for DTC (Direct-to-Consumer) policies
 * 
 * The Solution:
 * - Read PHA data from STAGING tables (etl.stg_policy_hierarchy_assignments)
 * - Build hierarchies from the embedded participants (etl.stg_policy_hierarchy_participants)
 * - Create hierarchy versions, participants, state rules, splits, distributions
 * - Update production PHA records with the correct HierarchyId
 * 
 * Usage:
 *   npx tsx scripts/fix-policy-hierarchy-assignments.ts --verbose
 *   npx tsx scripts/fix-policy-hierarchy-assignments.ts --dry-run
 *   npx tsx scripts/fix-policy-hierarchy-assignments.ts --limit 100
 */

import * as crypto from 'crypto';
import * as sql from 'mssql';

// =============================================================================
// Configuration
// =============================================================================

interface DatabaseConfig {
  server: string;
  database: string;
  user: string;
  password: string;
  options?: {
    encrypt?: boolean;
    trustServerCertificate?: boolean;
  };
}

interface FixOptions {
  verbose?: boolean;
  dryRun?: boolean;
  limit?: number;
  schema?: string;
}

// =============================================================================
// Type Definitions
// =============================================================================

/** PHA record from staging table */
interface StagingPHARecord {
  id: string;
  policyId: string;
  hierarchyId: string | null;
  writingBrokerId: string;
  splitSequence: number;
  splitPercent: number;
  nonConformantReason: string | null;
}

/** PHA participant from staging table */
interface PHAParticipant {
  id: string;
  phaId: string;
  brokerId: string;
  brokerName: string | null;
  level: number;
  scheduleCode: string | null;
}

/** Policy info from production */
interface PolicyInfo {
  policyId: string;
  groupId: string;
  productCode: string;
  situsState: string | null;
  effectiveDate: Date;
  premium: number;
}

/** Generated hierarchy structure */
interface GeneratedHierarchy {
  hierarchyId: string;
  hierarchyVersionId: string;
  stateRuleId: string;
  participants: GeneratedParticipant[];
  isNew: boolean; // false if reusing existing hierarchy
}

/** Generated participant */
interface GeneratedParticipant {
  id: string;
  entityId: number;
  entityName: string | null;
  level: number;
  scheduleCode: string | null;
  scheduleId: number | null;
}

// =============================================================================
// Utility Functions
// =============================================================================

function brokerExternalToInternal(externalId: string): number {
  const numStr = externalId.replace(/^P/, '');
  const num = parseInt(numStr, 10);
  return isNaN(num) ? 0 : num;
}

function getStateName(stateCode: string): string {
  const stateNames: Record<string, string> = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia'
  };
  return stateNames[stateCode] || stateCode;
}

function computeHash(input: string): string {
  return crypto.createHash('sha256').update(input).digest('hex').toUpperCase().substring(0, 16);
}

// =============================================================================
// Main Fix Class
// =============================================================================

class PHAFixer {
  private pool: sql.ConnectionPool;
  private options: FixOptions;
  private scheduleIdByExternalId = new Map<string, number>();
  
  // Counters
  private hierarchyCounter = 0;
  private hvCounter = 0;
  private hpCounter = 0;
  private stateRuleCounter = 0;
  private hierarchySplitCounter = 0;
  private splitDistributionCounter = 0;
  
  // Deduplication
  private hierarchyByHash = new Map<string, string>();
  
  // Stats
  private stats = {
    phasProcessed: 0,
    hierarchiesCreated: 0,
    hierarchiesReused: 0,
    phasUpdated: 0,
    errors: 0
  };

  constructor(pool: sql.ConnectionPool, options: FixOptions) {
    this.pool = pool;
    this.options = options;
  }

  // ==========================================================================
  // Step 1: Load Schedules for ID Resolution
  // ==========================================================================

  async loadSchedules(): Promise<void> {
    if (this.options.verbose) console.log('Loading schedules for ID resolution...');
    
    const result = await this.pool.request().query(`
      SELECT Id, ExternalId
      FROM dbo.Schedules
      WHERE ExternalId IS NOT NULL
    `);
    
    for (const row of result.recordset) {
      this.scheduleIdByExternalId.set(row.ExternalId, row.Id);
    }
    
    if (this.options.verbose) {
      console.log(`  ✓ Loaded ${this.scheduleIdByExternalId.size} schedule mappings`);
    }
  }

  // ==========================================================================
  // Step 2: Load PHAs from Staging Table
  // ==========================================================================

  async loadPHAsFromStaging(): Promise<StagingPHARecord[]> {
    if (this.options.verbose) console.log('Loading PHAs from staging table...');
    
    const schema = this.options.schema || 'etl';
    const limitClause = this.options.limit ? `TOP ${this.options.limit}` : '';
    
    // Load from staging table which has full PHA data
    const result = await this.pool.request().query(`
      SELECT ${limitClause}
        pha.Id as id,
        pha.PolicyId as policyId,
        pha.HierarchyId as hierarchyId,
        CAST(pha.WritingBrokerId AS NVARCHAR(50)) as writingBrokerId,
        pha.SplitSequence as splitSequence,
        pha.SplitPercent as splitPercent,
        pha.NonConformantReason as nonConformantReason
      FROM [${schema}].[stg_policy_hierarchy_assignments] pha
      WHERE pha.HierarchyId IS NULL
      ORDER BY pha.Id
    `);
    
    if (this.options.verbose) {
      console.log(`  ✓ Found ${result.recordset.length} PHAs without hierarchies in staging`);
    }
    
    return result.recordset;
  }

  // ==========================================================================
  // Step 3: Load PHA Participants (embedded hierarchy data)
  // ==========================================================================

  async loadPHAParticipants(): Promise<Map<string, PHAParticipant[]>> {
    if (this.options.verbose) console.log('Loading PHA participants from staging...');
    
    const schema = this.options.schema || 'etl';
    
    // Load from staging table (etl.stg_policy_hierarchy_participants)
    const result = await this.pool.request().query(`
      SELECT 
        Id as id,
        PolicyHierarchyAssignmentId as phaId,
        CAST(BrokerId AS NVARCHAR(50)) as brokerId,
        BrokerName as brokerName,
        Level as level,
        LTRIM(RTRIM(ScheduleCode)) as scheduleCode
      FROM [${schema}].[stg_policy_hierarchy_participants]
      ORDER BY PolicyHierarchyAssignmentId, Level
    `);
    
    // Group by PHA ID
    const participantsByPHA = new Map<string, PHAParticipant[]>();
    for (const row of result.recordset) {
      const phaId = row.phaId;
      if (!participantsByPHA.has(phaId)) {
        participantsByPHA.set(phaId, []);
      }
      participantsByPHA.get(phaId)!.push({
        id: row.id,
        phaId: row.phaId,
        brokerId: row.brokerId,
        brokerName: row.brokerName,
        level: row.level,
        scheduleCode: row.scheduleCode
      });
    }
    
    if (this.options.verbose) {
      console.log(`  ✓ Loaded ${result.recordset.length} PHA participants for ${participantsByPHA.size} PHAs`);
    }
    
    return participantsByPHA;
  }

  // ==========================================================================
  // Step 4: Load Policy Info for State/Product Context
  // ==========================================================================

  async loadPolicyInfo(policyIds: string[]): Promise<Map<string, PolicyInfo>> {
    if (this.options.verbose) console.log('Loading policy info...');
    
    if (policyIds.length === 0) {
      return new Map();
    }
    
    // Load from production Policies table
    // Note: Use "State" column, not "SitusState"
    const result = await this.pool.request().query(`
      SELECT 
        p.Id as policyId,
        p.GroupId as groupId,
        p.ProductCode as productCode,
        p.[State] as situsState,
        p.EffectiveDate as effectiveDate,
        p.Premium as premium
      FROM dbo.Policies p
      WHERE p.Id IN (SELECT value FROM STRING_SPLIT('${policyIds.join(',')}', ','))
    `);
    
    const policyMap = new Map<string, PolicyInfo>();
    for (const row of result.recordset) {
      policyMap.set(String(row.policyId), {
        policyId: String(row.policyId),
        groupId: row.groupId || 'G00000',
        productCode: row.productCode || 'UNKNOWN',
        situsState: row.situsState || null,
        effectiveDate: row.effectiveDate || new Date(),
        premium: row.premium || 0
      });
    }
    
    if (this.options.verbose) {
      console.log(`  ✓ Loaded info for ${policyMap.size} policies`);
    }
    
    return policyMap;
  }

  // ==========================================================================
  // Step 5: Create Hierarchy for PHA
  // ==========================================================================

  createHierarchyForPHA(
    pha: StagingPHARecord,
    participants: PHAParticipant[],
    policyInfo: PolicyInfo | undefined
  ): GeneratedHierarchy | null {
    if (participants.length === 0) {
      if (this.options.verbose) {
        console.log(`  ⚠️ PHA ${pha.id} has no participants, skipping`);
      }
      return null;
    }
    
    // NOTE: For PHAs, we do NOT deduplicate hierarchies because:
    // 1. There's a unique index on (PolicyId, HierarchyId, WritingBrokerId)
    // 2. Multiple PHAs for the same policy with same writing broker would violate this
    // 3. Each PHA needs its own hierarchy to avoid the constraint violation
    
    // Create new hierarchy (unique per PHA)
    this.hierarchyCounter++;
    const hierarchyId = `H-PHA-${this.hierarchyCounter}`;
    const hierarchyVersionId = `HV-PHA-${this.hierarchyCounter}`;
    const stateRuleId = `SR-${hierarchyVersionId}-DEFAULT`;
    
    // Create participants
    const generatedParticipants: GeneratedParticipant[] = [];
    for (const p of participants) {
      this.hpCounter++;
      const scheduleId = p.scheduleCode 
        ? this.scheduleIdByExternalId.get(p.scheduleCode) || null 
        : null;
      
      generatedParticipants.push({
        id: `HP-PHA-${this.hpCounter}`,
        entityId: brokerExternalToInternal(p.brokerId),
        entityName: p.brokerName,
        level: p.level,
        scheduleCode: p.scheduleCode,
        scheduleId
      });
    }
    
    this.stats.hierarchiesCreated++;
    
    return {
      hierarchyId,
      hierarchyVersionId,
      stateRuleId,
      participants: generatedParticipants,
      isNew: true
    };
  }

  // ==========================================================================
  // Step 6: Write Hierarchies to Database (Bulk Insert Optimized)
  // ==========================================================================

  async writeHierarchies(
    hierarchies: Array<{
      pha: StagingPHARecord;
      hierarchy: GeneratedHierarchy;
      policyInfo: PolicyInfo | undefined;
    }>
  ): Promise<void> {
    if (this.options.dryRun) {
      console.log(`[DRY RUN] Would create ${hierarchies.length} hierarchies (one per PHA)`);
      return;
    }

    if (this.options.verbose) console.log('Writing hierarchies to database...');

    // All hierarchies are new (no deduplication for PHAs)
    const newHierarchies = hierarchies;

    if (newHierarchies.length === 0) {
      if (this.options.verbose) console.log('  No new hierarchies to create');
      return;
    }


    // Process one hierarchy at a time with individual transactions
    for (let i = 0; i < newHierarchies.length; i++) {
      const { pha, hierarchy, policyInfo } = newHierarchies[i];

      // Use transaction for each hierarchy
      const transaction = new sql.Transaction(this.pool);
      await transaction.begin();

      try {
          const writingBrokerName = hierarchy.participants[0]?.entityName || null;
          const writingBrokerId = brokerExternalToInternal(pha.writingBrokerId);
          const effectiveDate = policyInfo?.effectiveDate || new Date();
          const productCode = policyInfo?.productCode || 'UNKNOWN';

          // Insert Hierarchy
          await new sql.Request(transaction)
            .input('Id', sql.NVarChar(100), hierarchy.hierarchyId)
            .input('Name', sql.NVarChar(500), `PHA Hierarchy for Policy ${pha.policyId}`)
            .input('Description', sql.NVarChar(1000), `Policy Hierarchy Assignment for policy ${pha.policyId}`)
            .input('Type', sql.Int, 0)
            .input('Status', sql.Int, 1)
            .input('ProposalId', sql.NVarChar(100), null)
            .input('ProposalNumber', sql.NVarChar(100), null)
            .input('GroupId', sql.NVarChar(100), policyInfo?.groupId || 'G00000')
            .input('GroupName', sql.NVarChar(500), 'Direct-to-Consumer')
            .input('GroupNumber', sql.NVarChar(100), null)
            .input('BrokerId', sql.BigInt, writingBrokerId)
            .input('BrokerName', sql.NVarChar(500), writingBrokerName)
            .input('BrokerLevel', sql.Int, null)
            .input('SourceType', sql.Int, 0)
            .input('HasOverrides', sql.Bit, 0)
            .input('DeviationCount', sql.Int, 0)
            .input('SitusState', sql.NVarChar(10), policyInfo?.situsState || null)
            .input('EffectiveDate', sql.Date, effectiveDate)
            .input('CurrentVersionId', sql.NVarChar(100), hierarchy.hierarchyVersionId)
            .input('CurrentVersionNumber', sql.Int, 1)
            .input('CreationTime', sql.DateTime2, new Date())
            .input('IsDeleted', sql.Bit, 0)
            .query(`
              INSERT INTO dbo.Hierarchies (
                Id, Name, [Description], [Type], [Status], ProposalId, ProposalNumber,
                GroupId, GroupName, GroupNumber, BrokerId, BrokerName, BrokerLevel,
                SourceType, HasOverrides, DeviationCount, SitusState, EffectiveDate,
                CurrentVersionId, CurrentVersionNumber, CreationTime, IsDeleted
              ) VALUES (
                @Id, @Name, @Description, @Type, @Status, @ProposalId, @ProposalNumber,
                @GroupId, @GroupName, @GroupNumber, @BrokerId, @BrokerName, @BrokerLevel,
                @SourceType, @HasOverrides, @DeviationCount, @SitusState, @EffectiveDate,
                @CurrentVersionId, @CurrentVersionNumber, @CreationTime, @IsDeleted
              )
            `);

          // Insert Hierarchy Version
          await new sql.Request(transaction)
            .input('Id', sql.NVarChar(100), hierarchy.hierarchyVersionId)
            .input('HierarchyId', sql.NVarChar(100), hierarchy.hierarchyId)
            .input('Version', sql.Int, 1)
            .input('Status', sql.Int, 1)
            .input('EffectiveFrom', sql.DateTime2, effectiveDate)
            .input('EffectiveTo', sql.DateTime2, new Date('2099-01-01'))
            .input('ChangeReason', sql.NVarChar(500), 'PHA Creation')
            .input('CreationTime', sql.DateTime2, new Date())
            .input('IsDeleted', sql.Bit, 0)
            .query(`
              INSERT INTO dbo.HierarchyVersions (
                Id, HierarchyId, [Version], [Status], EffectiveFrom, EffectiveTo,
                ChangeReason, CreationTime, IsDeleted
              ) VALUES (
                @Id, @HierarchyId, @Version, @Status, @EffectiveFrom, @EffectiveTo,
                @ChangeReason, @CreationTime, @IsDeleted
              )
            `);

          // Insert Hierarchy Participants
          for (const participant of hierarchy.participants) {
            await new sql.Request(transaction)
              .input('Id', sql.NVarChar(100), participant.id)
              .input('HierarchyVersionId', sql.NVarChar(100), hierarchy.hierarchyVersionId)
              .input('EntityId', sql.BigInt, participant.entityId)
              .input('EntityName', sql.NVarChar(500), participant.entityName)
              .input('Level', sql.Int, participant.level)
              .input('SortOrder', sql.Int, participant.level)
              .input('ScheduleCode', sql.NVarChar(100), participant.scheduleCode)
              .input('ScheduleId', sql.BigInt, participant.scheduleId)
              .input('CommissionRate', sql.Decimal(18, 4), null)
              .input('CreationTime', sql.DateTime2, new Date())
              .input('IsDeleted', sql.Bit, 0)
              .query(`
                INSERT INTO dbo.HierarchyParticipants (
                  Id, HierarchyVersionId, EntityId, EntityName,
                  Level, SortOrder, ScheduleCode, ScheduleId, CommissionRate,
                  CreationTime, IsDeleted
                ) VALUES (
                  @Id, @HierarchyVersionId, @EntityId, @EntityName,
                  @Level, @SortOrder, @ScheduleCode, @ScheduleId, @CommissionRate,
                  @CreationTime, @IsDeleted
                )
              `);
          }

          // Insert State Rule (DEFAULT rule)
          await new sql.Request(transaction)
            .input('Id', sql.NVarChar(100), hierarchy.stateRuleId)
            .input('HierarchyVersionId', sql.NVarChar(100), hierarchy.hierarchyVersionId)
            .input('ShortName', sql.NVarChar(100), 'DEFAULT')
            .input('Name', sql.NVarChar(500), 'Default Rule')
            .input('Description', sql.NVarChar(1000), 'Default state rule for PHA hierarchy')
            .input('Type', sql.Int, 0)
            .input('SortOrder', sql.Int, 1)
            .input('CreationTime', sql.DateTime2, new Date())
            .input('IsDeleted', sql.Bit, 0)
            .query(`
              INSERT INTO dbo.StateRules (
                Id, HierarchyVersionId, ShortName, Name, [Description], [Type], SortOrder,
                CreationTime, IsDeleted
              ) VALUES (
                @Id, @HierarchyVersionId, @ShortName, @Name, @Description,
                @Type, @SortOrder, @CreationTime, @IsDeleted
              )
            `);

          // Insert Hierarchy Split (for the policy's product)
          this.hierarchySplitCounter++;
          const hierarchySplitId = `HS-PHA-${this.hierarchySplitCounter}`;

          await new sql.Request(transaction)
            .input('Id', sql.NVarChar(200), hierarchySplitId)
            .input('StateRuleId', sql.NVarChar(200), hierarchy.stateRuleId)
            .input('ProductId', sql.NVarChar(100), productCode)
            .input('ProductCode', sql.NVarChar(100), productCode)
            .input('ProductName', sql.NVarChar(500), `${productCode} Product`)
            .input('SortOrder', sql.Int, 1)
            .input('CreationTime', sql.DateTime2, new Date())
            .input('IsDeleted', sql.Bit, 0)
            .query(`
              INSERT INTO dbo.HierarchySplits (
                Id, StateRuleId, ProductId, ProductCode, ProductName, SortOrder,
                CreationTime, IsDeleted
              ) VALUES (
                @Id, @StateRuleId, @ProductId, @ProductCode, @ProductName, @SortOrder,
                @CreationTime, @IsDeleted
              )
            `);

          // Insert Split Distributions for each participant
          for (const participant of hierarchy.participants) {
            this.splitDistributionCounter++;
            const distributionId = `SD-PHA-${this.splitDistributionCounter}`;

            await new sql.Request(transaction)
              .input('Id', sql.NVarChar(200), distributionId)
              .input('HierarchySplitId', sql.NVarChar(200), hierarchySplitId)
              .input('HierarchyParticipantId', sql.NVarChar(200), participant.id)
              .input('ParticipantEntityId', sql.BigInt, participant.entityId)
              .input('Percentage', sql.Decimal(18, 4), 100 / hierarchy.participants.length)
              .input('ScheduleId', sql.NVarChar(100), participant.scheduleId ? String(participant.scheduleId) : null)
              .input('ScheduleName', sql.NVarChar(500), participant.scheduleCode ? `Schedule ${participant.scheduleCode}` : null)
              .input('CreationTime', sql.DateTime2, new Date())
              .input('IsDeleted', sql.Bit, 0)
              .query(`
                INSERT INTO dbo.SplitDistributions (
                  Id, HierarchySplitId, HierarchyParticipantId, ParticipantEntityId,
                  Percentage, ScheduleId, ScheduleName, CreationTime, IsDeleted
                ) VALUES (
                  @Id, @HierarchySplitId, @HierarchyParticipantId, @ParticipantEntityId,
                  @Percentage, @ScheduleId, @ScheduleName, @CreationTime, @IsDeleted
                )
              `);
          }
        }

          await transaction.commit();

          if (this.options.verbose && (i + 1) % 10 === 0) {
            console.log(`  Progress: ${i + 1}/${newHierarchies.length}`);
          }

      } catch (error) {
        console.error(`❌ Error processing hierarchy ${i + 1} (${hierarchy.hierarchyId}):`, error);
        await transaction.rollback();
        throw error;
      }
    }

    if (this.options.verbose) {
      console.log(`  ✓ Created ${newHierarchies.length} hierarchies with all related entities`);
    }
  }

  // ==========================================================================
  // Step 7: Update PHAs with Hierarchy IDs
  // ==========================================================================

  async updatePHAHierarchyIds(
    updates: Array<{ phaId: string; hierarchyId: string }>
  ): Promise<void> {
    if (this.options.dryRun) {
      console.log(`[DRY RUN] Would update ${updates.length} PHA records with HierarchyIds`);
      return;
    }
    
    if (this.options.verbose) console.log('Updating PHA records with hierarchy IDs...');
    
    const schema = this.options.schema || 'etl';
    let updated = 0;
    
    for (const { phaId, hierarchyId } of updates) {
      // Update staging table
      await this.pool.request()
        .input('HierarchyId', sql.NVarChar(100), hierarchyId)
        .input('Id', sql.NVarChar(100), phaId)
        .query(`
          UPDATE [${schema}].[stg_policy_hierarchy_assignments]
          SET HierarchyId = @HierarchyId
          WHERE Id = @Id
        `);
      
      // Update production table
      await this.pool.request()
        .input('HierarchyId', sql.NVarChar(100), hierarchyId)
        .input('Id', sql.NVarChar(100), phaId)
        .query(`
          UPDATE dbo.PolicyHierarchyAssignments
          SET HierarchyId = @HierarchyId
          WHERE Id = @Id
        `);
      
      this.stats.phasUpdated++;
      updated++;
      
      if (this.options.verbose && updated % 500 === 0) {
        console.log(`  Progress: ${updated}/${updates.length} PHAs updated`);
      }
    }
    
    if (this.options.verbose) {
      console.log(`  ✓ Updated ${updates.length} PHA records in both staging and production`);
    }
  }

  // ==========================================================================
  // Step 0: Clean up existing PHA hierarchies
  // ==========================================================================

  async cleanupExistingPHAHierarchies(): Promise<void> {
    if (this.options.verbose) console.log('Cleaning up existing PHA hierarchies...');
    
    if (this.options.dryRun) {
      console.log('[DRY RUN] Would delete existing PHA hierarchies');
      return;
    }
    
    // Delete in reverse order of dependencies
    await this.pool.request().query(`DELETE FROM dbo.SplitDistributions WHERE Id LIKE 'SD-PHA-%'`);
    await this.pool.request().query(`DELETE FROM dbo.HierarchySplits WHERE Id LIKE 'HS-PHA-%'`);
    await this.pool.request().query(`DELETE FROM dbo.StateRules WHERE Id LIKE 'SR-HV-PHA-%'`);
    await this.pool.request().query(`DELETE FROM dbo.HierarchyParticipants WHERE Id LIKE 'HP-PHA-%'`);
    await this.pool.request().query(`DELETE FROM dbo.HierarchyVersions WHERE Id LIKE 'HV-PHA-%'`);
    await this.pool.request().query(`DELETE FROM dbo.Hierarchies WHERE Id LIKE 'H-PHA-%'`);
    
    // Reset HierarchyId on PHAs
    const schema = this.options.schema || 'etl';
    await this.pool.request().query(`
      UPDATE [${schema}].[stg_policy_hierarchy_assignments]
      SET HierarchyId = NULL
      WHERE HierarchyId LIKE 'H-PHA-%'
    `);
    await this.pool.request().query(`
      UPDATE dbo.PolicyHierarchyAssignments
      SET HierarchyId = NULL
      WHERE HierarchyId LIKE 'H-PHA-%'
    `);
    
    if (this.options.verbose) console.log('  ✓ Cleaned up existing PHA hierarchies');
  }

  // ==========================================================================
  // Main Execution
  // ==========================================================================

  async run(): Promise<void> {
    console.log('='.repeat(70));
    console.log('FIX POLICY HIERARCHY ASSIGNMENTS');
    console.log('='.repeat(70));
    console.log('');
    
    // Step 0: Clean up existing PHA hierarchies
    await this.cleanupExistingPHAHierarchies();
    
    // Step 1: Load schedules for ID resolution
    await this.loadSchedules();
    
    // Step 2: Load PHAs from staging table (which has full data)
    const phas = await this.loadPHAsFromStaging();
    
    if (phas.length === 0) {
      console.log('✅ No PHAs without hierarchies found in staging. Nothing to fix!');
      return;
    }
    
    // Step 3: Load PHA participants from staging
    const participantsByPHA = await this.loadPHAParticipants();
    
    // Step 4: Load policy info from production
    const policyIds = phas.map(p => p.policyId);
    const policyInfoMap = await this.loadPolicyInfo(policyIds);
    
    // Step 5: Create hierarchies for each PHA
    console.log('');
    console.log('Creating hierarchies for PHAs...');
    
    const hierarchyUpdates: Array<{
      pha: StagingPHARecord;
      hierarchy: GeneratedHierarchy;
      policyInfo: PolicyInfo | undefined;
    }> = [];
    
    let processed = 0;
    for (const pha of phas) {
      processed++;
      this.stats.phasProcessed++;
      
      const participants = participantsByPHA.get(pha.id) || [];
      const policyInfo = policyInfoMap.get(pha.policyId);
      
      const hierarchy = this.createHierarchyForPHA(pha, participants, policyInfo);
      
      if (hierarchy) {
        hierarchyUpdates.push({ pha, hierarchy, policyInfo });
      }
      
      if (this.options.verbose && processed % 500 === 0) {
        console.log(`  Progress: ${processed}/${phas.length} PHAs processed`);
      }
    }
    
    console.log(`  ✓ Generated hierarchies for ${hierarchyUpdates.length} PHAs`);
    
    // Step 6: Write hierarchies to database
    await this.writeHierarchies(hierarchyUpdates);
    
    // Step 7: Update PHAs with hierarchy IDs (both staging and production)
    const phaUpdates = hierarchyUpdates.map(h => ({
      phaId: h.pha.id,
      hierarchyId: h.hierarchy.hierarchyId
    }));
    await this.updatePHAHierarchyIds(phaUpdates);
    
    // Print summary
    console.log('');
    console.log('='.repeat(70));
    console.log('SUMMARY');
    console.log('='.repeat(70));
    console.log(`PHAs processed: ${this.stats.phasProcessed}`);
    console.log(`Hierarchies created: ${this.stats.hierarchiesCreated}`);
    console.log(`PHAs updated: ${this.stats.phasUpdated}`);
    console.log(`Errors: ${this.stats.errors}`);
    console.log('');
    
    if (this.options.dryRun) {
      console.log('⚠️  DRY RUN - No changes were made to the database');
    } else {
      console.log('✅ Done!');
    }
  }
}

// =============================================================================
// Verification Queries
// =============================================================================

async function runVerification(pool: sql.ConnectionPool, verbose: boolean): Promise<void> {
  console.log('');
  console.log('='.repeat(70));
  console.log('VERIFICATION');
  console.log('='.repeat(70));
  
  // Check PHAs
  const phaResult = await pool.request().query(`
    SELECT 
      COUNT(*) as total,
      COUNT(HierarchyId) as withHierarchy,
      COUNT(*) - COUNT(HierarchyId) as withoutHierarchy
    FROM dbo.PolicyHierarchyAssignments
  `);
  
  const phaStats = phaResult.recordset[0];
  console.log(`PolicyHierarchyAssignments:`);
  console.log(`  Total: ${phaStats.total}`);
  console.log(`  With HierarchyId: ${phaStats.withHierarchy} (${((phaStats.withHierarchy / phaStats.total) * 100).toFixed(1)}%)`);
  console.log(`  Without HierarchyId: ${phaStats.withoutHierarchy} (${((phaStats.withoutHierarchy / phaStats.total) * 100).toFixed(1)}%)`);
  
  // Check hierarchies created for PHAs
  const hierarchyResult = await pool.request().query(`
    SELECT COUNT(*) as total
    FROM dbo.Hierarchies
    WHERE Id LIKE 'H-PHA-%'
  `);
  console.log(`PHA Hierarchies created: ${hierarchyResult.recordset[0].total}`);
  
  // Sample PHA with full hierarchy
  if (verbose) {
    const sampleResult = await pool.request().query(`
      SELECT TOP 3
        pha.Id as PHAId,
        pha.PolicyId,
        pha.HierarchyId,
        h.Name as HierarchyName,
        hv.Id as HierarchyVersionId,
        (SELECT COUNT(*) FROM dbo.HierarchyParticipants hp WHERE hp.HierarchyVersionId = hv.Id) as ParticipantCount,
        (SELECT COUNT(*) FROM dbo.StateRules sr WHERE sr.HierarchyVersionId = hv.Id) as StateRuleCount,
        (SELECT COUNT(*) FROM dbo.HierarchySplits hs 
         INNER JOIN dbo.StateRules sr ON sr.Id = hs.StateRuleId 
         WHERE sr.HierarchyVersionId = hv.Id) as SplitCount
      FROM dbo.PolicyHierarchyAssignments pha
      LEFT JOIN dbo.Hierarchies h ON h.Id = pha.HierarchyId
      LEFT JOIN dbo.HierarchyVersions hv ON hv.HierarchyId = h.Id
      WHERE pha.HierarchyId IS NOT NULL
        AND pha.HierarchyId LIKE 'H-PHA-%'
      ORDER BY pha.Id
    `);
    
    console.log('');
    console.log('Sample PHAs with hierarchies:');
    for (const row of sampleResult.recordset) {
      console.log(`  ${row.PHAId}: Policy ${row.PolicyId}`);
      console.log(`    Hierarchy: ${row.HierarchyId} (${row.HierarchyName})`);
      console.log(`    Version: ${row.HierarchyVersionId}`);
      console.log(`    Participants: ${row.ParticipantCount}, StateRules: ${row.StateRuleCount}, Splits: ${row.SplitCount}`);
    }
  }
}

// =============================================================================
// CLI Entry Point
// =============================================================================

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  
  // Parse CLI arguments
  const options: FixOptions = {
    verbose: args.includes('--verbose') || args.includes('-v'),
    dryRun: args.includes('--dry-run'),
    limit: args.includes('--limit')
      ? parseInt(args[args.indexOf('--limit') + 1])
      : undefined,
    schema: args.includes('--schema')
      ? args[args.indexOf('--schema') + 1]
      : 'etl'
  };
  
  // Get connection string from environment
  const connectionString = process.env.SQLSERVER;
  if (!connectionString) {
    console.error('ERROR: SQLSERVER environment variable not set');
    console.error('Format: Server=host;Database=db;User Id=user;Password=pass');
    process.exit(1);
  }
  
  // Parse connection string
  const parts = connectionString.split(';').reduce((acc, part) => {
    const [key, value] = part.split('=');
    if (key && value) acc[key.trim().toLowerCase()] = value.trim();
    return acc;
  }, {} as Record<string, string>);
  
  const config: DatabaseConfig = {
    server: parts['server'] || parts['data source'] || '',
    database: parts['database'] || parts['initial catalog'] || '',
    user: parts['user id'] || parts['uid'] || '',
    password: parts['password'] || parts['pwd'] || '',
    options: {
      encrypt: true,
      trustServerCertificate: true
    }
  };
  
  // Connect to database
  const pool = await sql.connect(config);
  
  try {
    const fixer = new PHAFixer(pool, options);
    await fixer.run();
    
    // Run verification
    await runVerification(pool, options.verbose || false);
    
  } finally {
    await pool.close();
  }
}

// Run
main().catch(err => {
  console.error('❌ Error:', err);
  process.exit(1);
});
