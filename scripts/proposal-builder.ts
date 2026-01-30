/**
 * APL Commission System - Proposal Builder (TypeScript Implementation)
 * 
 * NOTE: This file now uses BULK INSERTS for optimal performance.
 * See proposal-builder-bulk.ts for the optimized write implementation.
 * 
 * Generates all staging table entities from certificate data:
 * 
 * 1. stg_proposals                    - Commission agreements
 * 2. stg_proposal_key_mapping         - Lookup table for proposal resolution
 * 3. stg_premium_split_versions       - Premium split configurations
 * 4. stg_premium_split_participants   - Split participants (links to hierarchies)
 * 5. stg_hierarchies                  - Hierarchy containers
 * 6. stg_hierarchy_versions           - Time-versioned hierarchy structures
 * 7. stg_hierarchy_participants       - Brokers in hierarchy chains
 * 8. stg_policy_hierarchy_assignments - Non-conformant policy assignments
 * 9. stg_policy_hierarchy_participants - Embedded participants for PHA
 * 
 * Algorithm:
 * - Proposal Key = (GroupId, ConfigHash)
 * - Expandable: ProductCodes, PlanCodes, EffectiveDateRange
 * - Routes to PHA: Invalid groups (null/zeros), non-conformant (multiple hierarchies)
 * 
 * MODIFICATIONS FROM ORIGINAL code.md:
 * - Full SHA256 hash (64 chars) instead of truncated 16 chars
 * - Collision detection for hash safety
 * - Batched processing mode for large datasets
 * - Audit logging for operations tracking
 * - CLI flags for debugging and dry-run
 */

import * as crypto from 'crypto';
import * as sql from 'mssql';

// =============================================================================
// Type Definitions
// =============================================================================

/** A commission assignment from source broker to recipient broker */
interface ProposalAssignment {
  sourceBrokerId: string;           // Who earns the commission
  sourceBrokerName: string | null;
  recipientBrokerId: string;        // Who receives the payment
  recipientBrokerName: string | null;
}

/** Raw certificate record from input_certificate_info */
interface CertificateRecord {
  certificateId: string;
  groupId: string;
  groupName: string | null;
  certEffectiveDate: Date;
  productCode: string;
  planCode: string | null;
  certStatus: string;
  situsState: string | null;
  premium: number;
  // Split/hierarchy fields
  certSplitSeq: number;
  certSplitPercent: number;
  splitBrokerSeq: number;
  splitBrokerId: string; // External ID like "P13178"
  splitBrokerName: string | null;
  splitBrokerNPN: string | null;
  commissionSchedule: string | null;
  // Assignment fields (NEW)
  paidBrokerId: string | null;
  paidBrokerName: string | null;
}

/** Helper to convert broker external ID (P13178) to internal ID (13178) */
function brokerExternalToInternal(externalId: string): number {
  // Remove 'P' prefix and convert to number
  const numStr = externalId.replace(/^P/, '');
  const num = parseInt(numStr, 10);
  return isNaN(num) ? 0 : num;
}

/** Helper to convert state code to state name */
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

/** A single tier/level within a hierarchy */
interface HierarchyTier {
  level: number;
  brokerId: string;
  brokerName: string | null;
  brokerNPN: string | null;
  schedule: string | null;
  // Assignment tracking (NEW)
  paidBrokerId: string | null;
  paidBrokerName: string | null;
}

/** A premium split participant with their hierarchy */
interface SplitParticipant {
  splitSeq: number;
  splitPercent: number;
  writingBrokerId: string;
  writingBrokerName: string | null;
  writingBrokerNPN: string | null;
  tiers: HierarchyTier[];
  // Computed hash for this specific hierarchy chain
  hierarchyHash: string;
}

/** Complete split configuration for a proposal */
interface SplitConfiguration {
  splits: SplitParticipant[];
  totalSplitPercent: number;
}

/** Unique selection criteria with computed hierarchy */
interface SelectionCriteria {
  groupId: string;
  groupName: string | null;
  effectiveDate: Date;
  productCode: string;
  planCode: string;
  situsState: string | null;
  splitConfig: SplitConfiguration;
  configJson: string;
  configHash: string;
  certificateIds: string[];
}

/** Final proposal */
interface Proposal {
  id: string;
  proposalNumber: string;
  groupId: string;
  groupName: string | null;
  brokerId: number | null;
  brokerName: string | null;
  brokerUniquePartyId: string | null;
  situsState: string | null;
  planCodes: string[];
  productCodes: string[];
  configHash: string;
  effectiveDateFrom: Date;
  effectiveDateTo: Date;
  splitConfig: SplitConfiguration;
  certificateIds: string[];
  // Commission assignments (NEW)
  assignments: ProposalAssignment[];
}

/** Policy Hierarchy Assignment (non-conformant cases) */
interface PolicyHierarchyAssignment {
  certificateId: string;
  groupId: string;
  effectiveDate: Date;
  splitConfig: SplitConfiguration;
  reason: string;
}

/** Staging table output structures */
interface StagingProposal {
  Id: string;
  ProposalNumber: string;
  Status: number;
  SubmittedDate: Date;
  ProposedEffectiveDate: Date;
  SitusState: string | null;
  GroupId: string;
  GroupName: string | null;
  BrokerId: number | null;
  BrokerName: string | null;
  BrokerUniquePartyId: string | null;
  ProductCodes: string;
  PlanCodes: string;
  SplitConfigHash: string;
  DateRangeFrom: number;
  DateRangeTo: number;
  EffectiveDateFrom: Date;
  EffectiveDateTo: Date;
  Notes: string | null;
}

interface StagingProposalKeyMapping {
  GroupId: string;
  EffectiveYear: number;
  ProductCode: string;
  PlanCode: string;
  ProposalId: string;
  SplitConfigHash: string;
}

interface StagingPremiumSplitVersion {
  Id: string;
  GroupId: string;
  GroupName: string | null;
  ProposalId: string;
  ProposalNumber: string;
  VersionNumber: string;
  EffectiveFrom: Date;
  EffectiveTo: Date;
  TotalSplitPercent: number;
  Status: number;
}

interface StagingPremiumSplitParticipant {
  Id: string;
  VersionId: string;
  BrokerId: number;
  BrokerName: string | null;
  BrokerNPN: string | null;
  BrokerUniquePartyId: string | null;
  SplitPercent: number;
  IsWritingAgent: boolean;
  HierarchyId: string;
  HierarchyName: string | null;
  TemplateId: string | null;
  TemplateName: string | null;
  Sequence: number;
  WritingBrokerId: number;
  GroupId: string;
  EffectiveFrom: Date;
  EffectiveTo: Date;
  Notes: string | null;
}

interface StagingHierarchy {
  Id: string;
  Name: string;
  GroupId: string;
  GroupName: string | null;
  BrokerId: number;
  BrokerName: string | null;
  ProposalId: string;
  CurrentVersionId: string;
  EffectiveDate: Date;
  SitusState: string | null;
  Status: number;
}

interface StagingHierarchyVersion {
  Id: string;
  HierarchyId: string;
  VersionNumber: string;
  EffectiveFrom: Date;
  EffectiveTo: Date;
  Status: number;
}

interface StagingHierarchyParticipant {
  Id: string;
  HierarchyVersionId: string;
  EntityId: string;
  EntityName: string | null;
  EntityType: number;
  Level: number;
  CommissionRate: number | null;
  ScheduleCode: string | null;
  ScheduleId: number | null;
}

interface StagingPolicyHierarchyAssignment {
  PolicyId: string;
  WritingBrokerId: number;
  SplitSequence: number;
  SplitPercent: number;
  NonConformantReason: string | null;
}

interface StagingPolicyHierarchyParticipant {
  Id: string;
  PolicyHierarchyAssignmentId: string;
  BrokerId: string;
  BrokerName: string | null;
  Level: number;
  ScheduleCode: string | null;
}

/** Commission Assignment Version (NEW) */
interface StagingCommissionAssignmentVersion {
  Id: string;
  BrokerId: number;
  BrokerName: string | null;
  ProposalId: string;
  GroupId: string | null;
  HierarchyId: string | null;
  HierarchyVersionId: string | null;
  HierarchyParticipantId: string | null;
  VersionNumber: string;
  EffectiveFrom: Date;
  EffectiveTo: Date | null;
  Status: number;
  Type: number;
  ChangeDescription: string | null;
  TotalAssignedPercent: number;
}

/** Commission Assignment Recipient (NEW) */
interface StagingCommissionAssignmentRecipient {
  Id: string;
  VersionId: string;
  RecipientBrokerId: number;
  RecipientName: string | null;
  RecipientNPN: string | null;
  Percentage: number;
  RecipientHierarchyId: string | null;
  Notes: string | null;
}

/** State Rule for Hierarchy Version */
interface StagingStateRule {
  Id: string;
  HierarchyVersionId: string;
  ShortName: string;
  Name: string;
  Description: string | null;
  Type: number;
  SortOrder: number;
}

/** State Rule State Association */
interface StagingStateRuleState {
  Id: string;
  StateRuleId: string;
  StateCode: string;
  StateName: string;
}

/** Hierarchy Split - Product distribution per state rule */
interface StagingHierarchySplit {
  Id: string;
  StateRuleId: string;
  ProductId: string | null;
  ProductCode: string;
  ProductName: string;
  SortOrder: number;
}

/** Split Distribution - Links participants to splits with schedule references */
interface StagingSplitDistribution {
  Id: string;
  HierarchySplitId: string;
  HierarchyParticipantId: string;
  ParticipantEntityId: number;
  Percentage: number;
  ScheduleId: string | null;
  ScheduleName: string | null;
}

/** Proposal Product - Normalized product records from proposal ProductCodes */
interface StagingProposalProduct {
  Id: number;
  ProposalId: string;
  ProductCode: string;
  ProductName: string | null;
  CommissionStructure: string | null;
  ResolvedScheduleId: string | null;
}

interface StagingOutput {
  proposals: StagingProposal[];
  proposalProducts: StagingProposalProduct[];
  proposalKeyMappings: StagingProposalKeyMapping[];
  premiumSplitVersions: StagingPremiumSplitVersion[];
  premiumSplitParticipants: StagingPremiumSplitParticipant[];
  hierarchies: StagingHierarchy[];
  hierarchyVersions: StagingHierarchyVersion[];
  hierarchyParticipants: StagingHierarchyParticipant[];
  stateRules: StagingStateRule[];
  stateRuleStates: StagingStateRuleState[];
  hierarchySplits: StagingHierarchySplit[];
  splitDistributions: StagingSplitDistribution[];
  policyHierarchyAssignments: StagingPolicyHierarchyAssignment[];
  policyHierarchyParticipants: StagingPolicyHierarchyParticipant[];
  // Commission assignments (NEW)
  commissionAssignmentVersions: StagingCommissionAssignmentVersion[];
  commissionAssignmentRecipients: StagingCommissionAssignmentRecipient[];
}

/** Builder options for configurable operation */
export interface BuilderOptions {
  batchSize?: number;         // Default: null (process all), Set to 1000-5000 for batching
  dryRun?: boolean;           // Default: false
  verbose?: boolean;          // Default: false
  limitCertificates?: number; // For testing
  schema?: string;            // Target schema (default: 'etl')
}

/** Audit log structure */
interface AuditLog {
  runId: string;
  startTime: Date;
  endTime: Date;
  certificatesProcessed: number;
  proposalsGenerated: number;
  hierarchiesGenerated: number;
  phaRecordsGenerated: number;
  batchesProcessed: number;
  errors: string[];
  warnings: string[];
  hashCollisions: number;
}

// =============================================================================
// Proposal Builder Class
// =============================================================================

class ProposalBuilder {
  private certificates: CertificateRecord[] = [];
  private selectionCriteria: SelectionCriteria[] = [];
  private proposals: Proposal[] = [];
  private phaRecords: PolicyHierarchyAssignment[] = [];
  
  // Hash collision detection
  private hashCollisions = new Map<string, string>();
  private collisionCount = 0;
  
  // Deduplicated hierarchies
  private hierarchyByHash = new Map<string, { writingBrokerId: string; tiers: HierarchyTier[] }>();
  
  // Map from hierarchy hash to generated hierarchy ID (for deduplication)
  private hierarchyIdByHash = new Map<string, string>();
  
  // Track states per hierarchy (hierarchyHash -> Set<stateCode>)
  private hierarchyStatesByHash = new Map<string, Set<string>>();
  
  // Counters for entity IDs
  private proposalCounter = 0;
  private splitVersionCounter = 0;
  private splitParticipantCounter = 0;
  private hierarchyCounter = 0;
  private hvCounter = 0;
  private hpCounter = 0;
  private phaCounter = 0;
  private phpCounter = 0;
  private stateRuleCounter = 0;
  private stateRuleStateCounter = 0;
  private hierarchySplitCounter = 0;
  private splitDistributionCounter = 0;
  private proposalProductCounter = 0;

  // Non-conformant detection
  private nonConformantGroups = new Set<string>();
  
  // Track (state, products) per hierarchy for generating hierarchy splits
  private hierarchyStateProducts = new Map<string, Map<string, Set<string>>>();
  
  // Schedule lookup map: ExternalId -> numeric Id
  private scheduleIdByExternalId = new Map<string, number>();

  // ==========================================================================
  // Step 0: Load Schedules (for ID resolution)
  // ==========================================================================

  async loadSchedules(pool: any): Promise<void> {
    console.log('Loading schedules for ID resolution...');
    const result = await pool.request().query(`
      SELECT Id, ExternalId
      FROM dbo.Schedules
      WHERE ExternalId IS NOT NULL
    `);
    
    for (const row of result.recordset) {
      this.scheduleIdByExternalId.set(row.ExternalId, row.Id);
    }
    
    console.log(`  ✓ Loaded ${this.scheduleIdByExternalId.size} schedule mappings`);
  }

  // ==========================================================================
  // Step 1: Load Certificates
  // ==========================================================================

  loadCertificates(certificates: CertificateRecord[]): void {
    this.certificates = certificates;
  }

  // ==========================================================================
  // Step 2: Extract Selection Criteria
  // ==========================================================================

  extractSelectionCriteria(): void {
    console.log('Extracting selection criteria...');
    const startTime = Date.now();
    
    // PERFORMANCE FIX: Pre-group certificates to avoid O(n²) filtering
    // Group by (groupId, certificateId) first - O(n) complexity
    const certGroups = new Map<string, CertificateRecord[]>();
    
    console.log(`  Grouping ${this.certificates.length} rows by certificate...`);
    for (const cert of this.certificates) {
      const groupKey = `${cert.groupId}|${cert.certificateId}`;
      if (!certGroups.has(groupKey)) {
        certGroups.set(groupKey, []);
      }
      certGroups.get(groupKey)!.push(cert);
    }
    
    console.log(`  Processing ${certGroups.size} unique certificates...`);
    const criteriaMap = new Map<string, SelectionCriteria>();
    let processed = 0;
    const totalCerts = certGroups.size;
    
    // Now process each group - O(n) overall
    for (const [groupKey, certsForThisCert] of certGroups) {
      processed++;
      if (processed % 10000 === 0) {
        const pct = ((processed / totalCerts) * 100).toFixed(1);
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`    Progress: ${processed}/${totalCerts} (${pct}%) - ${elapsed}s elapsed`);
      }
      
      const cert = certsForThisCert[0]; // Get representative certificate for metadata
      
      // Build split configuration
      const splits: SplitParticipant[] = [];
      const splitSeqs = new Set(certsForThisCert.map(c => c.certSplitSeq));
      
      for (const splitSeq of splitSeqs) {
        const splitRecords = certsForThisCert.filter(c => c.certSplitSeq === splitSeq);
        const splitPercent = splitRecords[0]?.certSplitPercent || 0;
        
        // Build hierarchy tiers for this split (including assignment fields)
        const tiers: HierarchyTier[] = splitRecords
          .sort((a, b) => a.splitBrokerSeq - b.splitBrokerSeq)
          .map(r => ({
            level: r.splitBrokerSeq,
            brokerId: r.splitBrokerId,
            brokerName: r.splitBrokerName,
            brokerNPN: r.splitBrokerNPN || null,
            schedule: r.commissionSchedule,
            paidBrokerId: r.paidBrokerId || null,
            paidBrokerName: r.paidBrokerName || null
          }));
        
        const writingBrokerId = tiers[0]?.brokerId || '';
        const writingBrokerName = tiers[0]?.brokerName || null;
        const writingBrokerNPN = tiers[0]?.brokerNPN || null;
        
        // Compute hierarchy hash (IMPORTANT: Include paidBrokerId so assignments differentiate proposals)
        const hierarchyJson = JSON.stringify(
          tiers.map(t => ({ level: t.level, brokerId: t.brokerId, schedule: t.schedule, paidBrokerId: t.paidBrokerId }))
        );
        const hierarchyHash = this.computeHashWithCollisionCheck(hierarchyJson, `hierarchy-${cert.certificateId}-${splitSeq}`);
        
        splits.push({
          splitSeq,
          splitPercent,
          writingBrokerId,
          writingBrokerName,
          writingBrokerNPN,
          tiers,
          hierarchyHash
        });
      }
      
      const totalSplitPercent = splits.reduce((sum, s) => sum + s.splitPercent, 0);
      const splitConfig: SplitConfiguration = { splits, totalSplitPercent };
      
      // Compute config hash
      const configJson = JSON.stringify(
        splits.map(s => ({
          seq: s.splitSeq,
          pct: s.splitPercent,
          hierarchyHash: s.hierarchyHash
        }))
      );
      const configHash = this.computeHashWithCollisionCheck(configJson, `config-${cert.certificateId}`);
      
      criteriaMap.set(groupKey, {
        groupId: cert.groupId,
        groupName: cert.groupName,
        effectiveDate: cert.certEffectiveDate,
        productCode: cert.productCode,
        planCode: cert.planCode || '',
        situsState: cert.situsState,
        splitConfig,
        configJson,
        configHash,
        certificateIds: [cert.certificateId]
      });
    }

    this.selectionCriteria = Array.from(criteriaMap.values());
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`  ✓ Extracted ${this.selectionCriteria.length} selection criteria in ${elapsed}s`);
  }

  // ==========================================================================
  // Step 3: Identify Non-Conformant Cases (Early Detection)
  // ==========================================================================

  async identifyNonConformantCases(pool: any): Promise<void> {
    console.log('Identifying non-conformant cases...');
    const startTime = Date.now();

    // Check 1: DTC policies (GroupId = G00000)
    let dtcCount = 0;
    for (const criteria of this.selectionCriteria) {
      if (criteria.groupId === '00000') { // Note: criteria.groupId doesn't have 'G' prefix yet
        this.phaRecords.push({
          certificateId: criteria.certificateIds[0],
          groupId: criteria.groupId,
          effectiveDate: criteria.effectiveDate,
          splitConfig: criteria.splitConfig,
          reason: 'DTC-NoGroup'
        });
        dtcCount++;
      }
    }

    // Check 2: Non-conformant groups (from database lookup)
    const groupIds = Array.from(new Set(this.selectionCriteria.map(c => c.groupId)));
    if (groupIds.length > 0) {
      const groupIdsList = groupIds.map(id => `'G${id}'`).join(','); // Add 'G' prefix for lookup
      const groupResult = await pool.request().query(`
        SELECT Id, IsNonConformant
        FROM [dbo].[EmployerGroups]
        WHERE Id IN (${groupIdsList}) AND IsNonConformant = 1
      `);

      for (const group of groupResult.recordset) {
        const cleanGroupId = group.Id.replace(/^G/, ''); // Remove 'G' prefix
        this.nonConformantGroups.add(cleanGroupId);

        // Route all criteria in non-conformant groups to PHA
        for (const criteria of this.selectionCriteria) {
          if (criteria.groupId === cleanGroupId) {
            this.phaRecords.push({
              certificateId: criteria.certificateIds[0],
              groupId: criteria.groupId,
              effectiveDate: criteria.effectiveDate,
              splitConfig: criteria.splitConfig,
              reason: 'NonConformant-SplitMismatch'
            });
          }
        }
      }
    }

    // Check 3: Certificates with split percent != 100
    let splitMismatchCount = 0;
    for (const criteria of this.selectionCriteria) {
      if (criteria.splitConfig.totalSplitPercent !== 100) {
        this.phaRecords.push({
          certificateId: criteria.certificateIds[0],
          groupId: criteria.groupId,
          effectiveDate: criteria.effectiveDate,
          splitConfig: criteria.splitConfig,
          reason: 'NonConformant-CertificateSplitMismatch'
        });
        splitMismatchCount++;
      }
    }

    // Remove non-conformant criteria from selectionCriteria to prevent proposal creation
    const originalCount = this.selectionCriteria.length;
    this.selectionCriteria = this.selectionCriteria.filter(criteria => {
      // Check if this criteria was routed to PHA
      return !this.phaRecords.some(pha =>
        pha.certificateId === criteria.certificateIds[0] &&
        pha.groupId === criteria.groupId
      );
    });

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`  ✓ Identified ${this.phaRecords.length} non-conformant cases in ${elapsed}s`);
    console.log(`    - DTC policies: ${dtcCount}`);
    console.log(`    - Non-conformant groups: ${this.nonConformantGroups.size}`);
    console.log(`    - Split mismatches: ${splitMismatchCount}`);
    console.log(`    - Remaining conformant criteria: ${this.selectionCriteria.length}/${originalCount}`);
  }

  // ==========================================================================
  // Step 4: Build Proposals
  // ==========================================================================

  buildProposals(): void {
    console.log('Building proposals...');
    const startTime = Date.now();
    
    // Group selection criteria into proposals
    const proposalMap = new Map<string, Proposal>();
    let processed = 0;
    const totalCriteria = this.selectionCriteria.length;

    for (const criteria of this.selectionCriteria) {
      processed++;
      if (processed % 10000 === 0) {
        const pct = ((processed / totalCriteria) * 100).toFixed(1);
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`  Progress: ${processed}/${totalCriteria} (${pct}%) - ${elapsed}s elapsed`);
      }
      
      const key = `${criteria.groupId}|${criteria.configHash}`;
      
      // Check if group is invalid (routes to PHA)
      if (this.isInvalidGroup(criteria.groupId)) {
        this.phaRecords.push({
          certificateId: criteria.certificateIds[0],
          groupId: criteria.groupId,
          effectiveDate: criteria.effectiveDate,
          splitConfig: criteria.splitConfig,
          reason: 'Invalid GroupId (null/empty/zeros)'
        });
        continue;
      }

      if (!proposalMap.has(key)) {
        this.proposalCounter++;
        // Extract assignments from split configuration
        const assignments = this.extractAssignments(criteria.splitConfig);
        
        // Use ProposalNumber format as the Id
        const proposalId = `PROP-${criteria.groupId}-${this.proposalCounter}`;
        
        // Extract primary broker from first split (writing agent)
        const primarySplit = criteria.splitConfig.splits
          .sort((a, b) => a.splitSeq - b.splitSeq)[0];
        
        const primaryBrokerId = primarySplit?.writingBrokerId || null;  // "P10241"
        const primaryBrokerNumericId = primaryBrokerId ? brokerExternalToInternal(primaryBrokerId) : null;
        
        const proposal: Proposal = {
          id: proposalId,
          proposalNumber: proposalId,  // Same as Id
          groupId: `G${criteria.groupId}`,  // Add 'G' prefix for consistency with production
          groupName: criteria.groupName,  // Now populated from lookup
          brokerId: primaryBrokerNumericId,  // Numeric ID for FK
          brokerName: primarySplit?.writingBrokerName || null,
          brokerUniquePartyId: primaryBrokerId,  // "P10241"
          situsState: criteria.situsState,
          planCodes: [criteria.planCode],
          productCodes: [criteria.productCode],
          configHash: criteria.configHash,
          effectiveDateFrom: criteria.effectiveDate,
          effectiveDateTo: criteria.effectiveDate,
          splitConfig: criteria.splitConfig,
          certificateIds: [...criteria.certificateIds],
          assignments
        };
        proposalMap.set(key, proposal);
      } else {
        const proposal = proposalMap.get(key)!;
        
        // Expand plan codes
        if (!proposal.planCodes.includes(criteria.planCode)) {
          proposal.planCodes.push(criteria.planCode);
        }
        
        // Expand product codes
        if (!proposal.productCodes.includes(criteria.productCode)) {
          proposal.productCodes.push(criteria.productCode);
        }
        
        // Expand date range
        if (criteria.effectiveDate < proposal.effectiveDateFrom) {
          proposal.effectiveDateFrom = criteria.effectiveDate;
        }
        if (criteria.effectiveDate > proposal.effectiveDateTo) {
          proposal.effectiveDateTo = criteria.effectiveDate;
        }
        
        // Add certificate IDs
        for (const certId of criteria.certificateIds) {
          if (!proposal.certificateIds.includes(certId)) {
            proposal.certificateIds.push(certId);
          }
        }
      }
    }

    this.proposals = Array.from(proposalMap.values());
    
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`  ✓ Built ${this.proposals.length} proposals and ${this.phaRecords.length} PHA records in ${elapsed}s`);
    
    // Store deduplicated hierarchies
    for (const proposal of this.proposals) {
      for (const split of proposal.splitConfig.splits) {
        if (!this.hierarchyByHash.has(split.hierarchyHash)) {
          this.hierarchyByHash.set(split.hierarchyHash, {
            writingBrokerId: split.writingBrokerId,
            tiers: split.tiers
          });
        }
      }
    }
  }

  // ==========================================================================
  // Step 4: Generate Staging Output
  // ==========================================================================

  generateStagingOutput(): StagingOutput {
    console.log('Generating staging output...');
    const startTime = Date.now();
    
    const output: StagingOutput = {
      proposals: [],
      proposalProducts: [],
      proposalKeyMappings: [],
      premiumSplitVersions: [],
      premiumSplitParticipants: [],
      hierarchies: [],
      hierarchyVersions: [],
      hierarchyParticipants: [],
      stateRules: [],
      stateRuleStates: [],
      hierarchySplits: [],
      splitDistributions: [],
      policyHierarchyAssignments: [],
      policyHierarchyParticipants: [],
      commissionAssignmentVersions: [],
      commissionAssignmentRecipients: []
    };

    console.log(`  Processing ${this.proposals.length} proposals...`);
    // Generate proposals
    for (const p of this.proposals) {
      output.proposals.push({
        Id: p.id,
        ProposalNumber: p.proposalNumber,
        Status: 1, // Active
        SubmittedDate: p.effectiveDateFrom,
        ProposedEffectiveDate: p.effectiveDateFrom,
        SitusState: p.situsState,
        GroupId: p.groupId,
        GroupName: p.groupName,
        BrokerId: p.brokerId,
        BrokerName: p.brokerName,
        BrokerUniquePartyId: p.brokerUniquePartyId,
        ProductCodes: p.productCodes.join(','),
        PlanCodes: p.planCodes.join(','),
        SplitConfigHash: p.configHash,
        DateRangeFrom: p.effectiveDateFrom.getFullYear(),
        DateRangeTo: p.effectiveDateTo.getFullYear(),
        EffectiveDateFrom: p.effectiveDateFrom,
        EffectiveDateTo: p.effectiveDateTo,
        Notes: `Generated by TypeScript builder. Certificates: ${p.certificateIds.length}`
      });

      // Generate proposal key mappings (will be deduplicated later)
      const years = this.getYearRange(p.effectiveDateFrom, p.effectiveDateTo);
      for (const year of years) {
        for (const productCode of p.productCodes) {
          for (const planCode of p.planCodes) {
            output.proposalKeyMappings.push({
              GroupId: p.groupId,
              EffectiveYear: year,
              ProductCode: productCode,
              PlanCode: planCode,
              ProposalId: p.id,
              SplitConfigHash: p.configHash
            });
          }
        }
      }

      // Generate premium split version
      this.splitVersionCounter++;
      const versionId = `PSV-${this.splitVersionCounter}`;
      output.premiumSplitVersions.push({
        Id: versionId,
        GroupId: p.groupId,
        GroupName: p.groupName,
        ProposalId: p.id,
        ProposalNumber: p.proposalNumber,
        VersionNumber: 'V1',
        EffectiveFrom: p.effectiveDateFrom,
        EffectiveTo: new Date('2099-01-01'),
        TotalSplitPercent: p.splitConfig.totalSplitPercent,
        Status: 1
      });

      // Generate premium split participants (one per split)
      for (const split of p.splitConfig.splits) {
        this.splitParticipantCounter++;
        
        // Get or create hierarchy
        const hierarchyId = this.getOrCreateHierarchy(
          split.hierarchyHash,
          p.groupId,
          p.groupName,
          p.id,
          p.effectiveDateFrom,
          p.situsState,
          output
        );
        
        output.premiumSplitParticipants.push({
          Id: `PSP-${this.splitParticipantCounter}`,
          VersionId: versionId,
          BrokerId: brokerExternalToInternal(split.writingBrokerId),
          BrokerName: split.writingBrokerName,
          BrokerNPN: split.writingBrokerNPN || null,
          BrokerUniquePartyId: split.writingBrokerId,
          SplitPercent: split.splitPercent,
          IsWritingAgent: true,
          HierarchyId: hierarchyId,
          HierarchyName: `Hierarchy for ${p.groupId}`,
          TemplateId: null,
          TemplateName: null,
          Sequence: split.splitSeq,
          WritingBrokerId: brokerExternalToInternal(split.writingBrokerId),
          GroupId: p.groupId,
          EffectiveFrom: p.effectiveDateFrom,
          EffectiveTo: new Date('2099-01-01'),
          Notes: null
        });
      }

      // Generate commission assignments for this proposal (NEW)
      for (const assignment of p.assignments) {
        // Include both source and recipient in ID to ensure uniqueness
        const cavId = `CAV-${p.id}-${assignment.sourceBrokerId}-${assignment.recipientBrokerId}`;
        
        output.commissionAssignmentVersions.push({
          Id: cavId,
          BrokerId: brokerExternalToInternal(assignment.sourceBrokerId),
          BrokerName: assignment.sourceBrokerName,
          ProposalId: p.id,
          GroupId: p.groupId,
          HierarchyId: null,
          HierarchyVersionId: null,
          HierarchyParticipantId: null,
          VersionNumber: '1',
          EffectiveFrom: p.effectiveDateFrom,
          EffectiveTo: p.effectiveDateTo,
          Status: 1, // Active
          Type: 1, // Full assignment
          ChangeDescription: `Commission assignment from ${assignment.sourceBrokerName || assignment.sourceBrokerId} to ${assignment.recipientBrokerName || assignment.recipientBrokerId}`,
          TotalAssignedPercent: 100.00
        });

        const carId = `CAR-${cavId}`;
        
        output.commissionAssignmentRecipients.push({
          Id: carId,
          VersionId: cavId,
          RecipientBrokerId: brokerExternalToInternal(assignment.recipientBrokerId),
          RecipientName: assignment.recipientBrokerName,
          RecipientNPN: null,
          Percentage: 100.00,
          RecipientHierarchyId: null,
          Notes: `Receives commissions from ${assignment.sourceBrokerName || assignment.sourceBrokerId} for proposal ${p.id}`
        });
      }
    }

    // Generate PHA records for non-conformant policies
    for (const pha of this.phaRecords) {
      for (const split of pha.splitConfig.splits) {
        this.phaCounter++;
        const phaAssignmentId = `PHA-${this.phaCounter}`;
        
        output.policyHierarchyAssignments.push({
          PolicyId: pha.certificateId,
          WritingBrokerId: brokerExternalToInternal(split.writingBrokerId),
          SplitSequence: split.splitSeq,
          SplitPercent: split.splitPercent,
          NonConformantReason: pha.reason
        });

        // Generate PHA participants
        for (const tier of split.tiers) {
          this.phpCounter++;
          output.policyHierarchyParticipants.push({
            Id: `PHP-${this.phpCounter}`,
            PolicyHierarchyAssignmentId: phaAssignmentId,
            BrokerId: tier.brokerId,
            BrokerName: tier.brokerName,
            Level: tier.level,
            ScheduleCode: tier.schedule
          });
        }
      }
    }

    // Collect products per (hierarchy, state) from proposals for hierarchy splits
    console.log(`  Collecting products per hierarchy and state from ${this.proposals.length} proposals...`);
    for (const proposal of this.proposals) {
      // Each proposal has splits, and each split has a hierarchy
      for (const split of proposal.splitConfig.splits) {
        const hierarchyHash = split.hierarchyHash;
        const state = proposal.situsState;
        
        if (hierarchyHash && state && proposal.productCodes.length > 0) {
          if (!this.hierarchyStateProducts.has(hierarchyHash)) {
            this.hierarchyStateProducts.set(hierarchyHash, new Map());
          }
          const stateProductMap = this.hierarchyStateProducts.get(hierarchyHash)!;
          
          if (!stateProductMap.has(state)) {
            stateProductMap.set(state, new Set());
          }
          
          // Add all product codes from this proposal to this hierarchy+state
          for (const productCode of proposal.productCodes) {
            stateProductMap.get(state)!.add(productCode);
          }
        }
      }
    }
    console.log(`  ✓ Collected products for ${this.hierarchyStateProducts.size} hierarchies`);

    // Generate state rules for all hierarchies
    console.log(`  Generating state rules for ${output.hierarchyVersions.length} hierarchy versions...`);
    for (const hierarchyVersion of output.hierarchyVersions) {
      const hierarchy = output.hierarchies.find(h => h.Id === hierarchyVersion.HierarchyId);
      if (!hierarchy) continue;
      
      // Find the hierarchy hash for this hierarchy
      const hierarchyHash = Array.from(this.hierarchyIdByHash.entries())
        .find(([_, id]) => id === hierarchy.Id)?.[0];
      
      if (!hierarchyHash) continue;
      
      // Get states for this hierarchy
      const states = this.hierarchyStatesByHash.get(hierarchyHash);
      if (!states || states.size === 0) continue;
      
      const stateArray = Array.from(states).sort();
      
      // Business Rule: Single state → default rule, Multiple states → state-specific rules
      if (stateArray.length === 1) {
        // Create ONE default state rule (applies to all states)
        this.stateRuleCounter++;
        output.stateRules.push({
          Id: `SR-${hierarchyVersion.Id}-DEFAULT`,
          HierarchyVersionId: hierarchyVersion.Id,
          ShortName: 'DEFAULT',
          Name: 'Default Rule',
          Description: `Default state rule for hierarchy ${hierarchy.Name}`,
          Type: 0, // Include
          SortOrder: 1
        });
        // NO state rule states - applies universally
      } else {
        // Create one state rule per state
        let sortOrder = 1;
        for (const stateCode of stateArray) {
          this.stateRuleCounter++;
          const stateRuleId = `SR-${hierarchyVersion.Id}-${stateCode}`;
          
          output.stateRules.push({
            Id: stateRuleId,
            HierarchyVersionId: hierarchyVersion.Id,
            ShortName: stateCode,
            Name: stateCode,
            Description: `State rule for ${stateCode} in hierarchy ${hierarchy.Name}`,
            Type: 0, // Include
            SortOrder: sortOrder++
          });
          
          // Create state rule state association
          this.stateRuleStateCounter++;
          output.stateRuleStates.push({
            Id: `SRS-${this.stateRuleStateCounter}`,
            StateRuleId: stateRuleId,
            StateCode: stateCode,
            StateName: getStateName(stateCode)
          });
          
          // Create hierarchy splits for products in this state
          const stateProductMap = this.hierarchyStateProducts.get(hierarchyHash);
          if (stateProductMap) {
            const productsForState = stateProductMap.get(stateCode);
            if (productsForState && productsForState.size > 0) {
              let productSortOrder = 1;
              for (const productCode of Array.from(productsForState).sort()) {
                this.hierarchySplitCounter++;
                output.hierarchySplits.push({
                  Id: `HS-${this.hierarchySplitCounter}`,
                  StateRuleId: stateRuleId,
                  ProductId: null, // Will be resolved by downstream processes
                  ProductCode: productCode,
                  ProductName: `${productCode} Product`,
                  SortOrder: productSortOrder++
                });
              }
            }
          }
        }
      }
      
      // For DEFAULT rules (single state), create hierarchy splits for all products across all states
      if (stateArray.length === 1) {
        const defaultStateRuleId = `SR-${hierarchyVersion.Id}-DEFAULT`;
        const stateProductMap = this.hierarchyStateProducts.get(hierarchyHash);
        if (stateProductMap) {
          const allProducts = new Set<string>();
          for (const products of stateProductMap.values()) {
            for (const product of products) {
              allProducts.add(product);
            }
          }
          
          if (allProducts.size > 0) {
            let productSortOrder = 1;
            for (const productCode of Array.from(allProducts).sort()) {
              this.hierarchySplitCounter++;
              output.hierarchySplits.push({
                Id: `HS-${this.hierarchySplitCounter}`,
                StateRuleId: defaultStateRuleId,
                ProductId: null,
                ProductCode: productCode,
                ProductName: `${productCode} Product`,
                SortOrder: productSortOrder++
              });
            }
          }
        }
      }
    }
    console.log(`  ✓ Generated ${output.stateRules.length} state rules and ${output.stateRuleStates.length} state rule states`);
    console.log(`  ✓ Generated ${output.hierarchySplits.length} hierarchy splits`);

    // Generate proposal products: Normalize ProductCodes into individual records
    console.log(`  Generating proposal products from ${output.proposals.length} proposals...`);
    for (const proposal of output.proposals) {
      if (!proposal.ProductCodes) continue;
      
      // ProductCodes can be comma-separated string or JSON array
      let productCodes: string[] = [];
      
      // Try JSON first
      if (proposal.ProductCodes.startsWith('[')) {
        try {
          productCodes = JSON.parse(proposal.ProductCodes);
        } catch (e) {
          // Not valid JSON, try comma-separated
          productCodes = proposal.ProductCodes.split(',');
        }
      } else {
        // Comma-separated string
        productCodes = proposal.ProductCodes.split(',');
      }
      
      // Create a ProposalProduct record for each product
      for (const productCode of productCodes) {
        const trimmed = productCode.trim();
        if (!trimmed || trimmed === 'N/A' || trimmed === '*') continue;
        
        this.proposalProductCounter++;
        output.proposalProducts.push({
          Id: this.proposalProductCounter,
          ProposalId: proposal.Id,
          ProductCode: trimmed,
          ProductName: `${trimmed} Product`,
          CommissionStructure: null,
          ResolvedScheduleId: null
        });
      }
    }
    console.log(`  ✓ Generated ${output.proposalProducts.length} proposal products`);

    // Generate split distributions: Link each hierarchy split to each participant with schedule
    console.log(`  Generating split distributions for ${output.hierarchySplits.length} hierarchy splits...`);
    for (const split of output.hierarchySplits) {
      // Find the state rule for this split
      const stateRule = output.stateRules.find(sr => sr.Id === split.StateRuleId);
      if (!stateRule) continue;
      
      // Find the hierarchy version for this state rule
      const hierarchyVersion = output.hierarchyVersions.find(hv => hv.Id === stateRule.HierarchyVersionId);
      if (!hierarchyVersion) continue;
      
      // Find all participants for this hierarchy version
      const participants = output.hierarchyParticipants.filter(hp => hp.HierarchyVersionId === hierarchyVersion.Id);
      
      // Create a distribution for each participant
      for (const participant of participants) {
        this.splitDistributionCounter++;
        output.splitDistributions.push({
          Id: `SD-${this.splitDistributionCounter}`,
          HierarchySplitId: split.Id,
          HierarchyParticipantId: participant.Id,
          ParticipantEntityId: brokerExternalToInternal(participant.EntityId),
          Percentage: 100 / participants.length, // Equal distribution across participants
          ScheduleId: participant.ScheduleId ? String(participant.ScheduleId) : null, // Use resolved numeric ID
          ScheduleName: participant.ScheduleCode ? `Schedule ${participant.ScheduleCode}` : null
        });
      }
    }
    console.log(`  ✓ Generated ${output.splitDistributions.length} split distributions`);

    // Deduplicate key mappings by primary key (GroupId, EffectiveYear, ProductCode, PlanCode)
    const keyMappingMap = new Map<string, StagingProposalKeyMapping>();
    for (const mapping of output.proposalKeyMappings) {
      const key = `${mapping.GroupId}|${mapping.EffectiveYear}|${mapping.ProductCode}|${mapping.PlanCode}`;
      if (!keyMappingMap.has(key)) {
        keyMappingMap.set(key, mapping);
      }
      // If duplicate, keep the first one (arbitrary choice, they should be identical)
    }
    output.proposalKeyMappings = Array.from(keyMappingMap.values());

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`  ✓ Generated all staging entities in ${elapsed}s`);
    console.log(`    Proposals: ${output.proposals.length}, Proposal Products: ${output.proposalProducts.length}, Hierarchies: ${output.hierarchies.length}, Key Mappings: ${output.proposalKeyMappings.length}`);
    
    return output;
  }

  // ==========================================================================
  // Helper: Get or Create Hierarchy
  // ==========================================================================

  private getOrCreateHierarchy(
    hierarchyHash: string,
    groupId: string,
    groupName: string | null,
    proposalId: string,
    effectiveDate: Date,
    situsState: string | null,
    output: StagingOutput
  ): string {
    // Track state for this hierarchy (for state rules generation)
    if (situsState) {
      if (!this.hierarchyStatesByHash.has(hierarchyHash)) {
        this.hierarchyStatesByHash.set(hierarchyHash, new Set());
      }
      this.hierarchyStatesByHash.get(hierarchyHash)!.add(situsState);
    }
    
    // Check if hierarchy already created in this run
    if (this.hierarchyIdByHash.has(hierarchyHash)) {
      return this.hierarchyIdByHash.get(hierarchyHash)!;
    }

    // Create new hierarchy
    const hierarchyData = this.hierarchyByHash.get(hierarchyHash);
    if (!hierarchyData) {
      throw new Error(`Hierarchy not found for hash: ${hierarchyHash}`);
    }

    this.hierarchyCounter++;
    const hierarchyId = `H-${this.hierarchyCounter}`;
    const writingBrokerId = brokerExternalToInternal(hierarchyData.writingBrokerId);
    const writingBrokerName = hierarchyData.tiers[0]?.brokerName || null;

    // Create hierarchy
    this.hvCounter++;
    const versionId = `HV-${this.hvCounter}`;

    output.hierarchies.push({
      Id: hierarchyId,
      Name: `Hierarchy for ${groupId}`,
      GroupId: groupId,
      GroupName: groupName,
      BrokerId: writingBrokerId,
      BrokerName: writingBrokerName,
      ProposalId: proposalId,
      CurrentVersionId: versionId,
      EffectiveDate: effectiveDate,
      SitusState: situsState,
      Status: 1
    });

    // Create hierarchy version
    output.hierarchyVersions.push({
      Id: versionId,
      HierarchyId: hierarchyId,
      VersionNumber: 'V1',
      EffectiveFrom: effectiveDate,
      EffectiveTo: new Date('2099-01-01'),
      Status: 1
    });

    // Create hierarchy participants
    for (const tier of hierarchyData.tiers) {
      this.hpCounter++;
      
      // Resolve numeric schedule ID from schedule code
      const scheduleId = tier.schedule ? this.scheduleIdByExternalId.get(tier.schedule) || null : null;
      
      output.hierarchyParticipants.push({
        Id: `HP-${this.hpCounter}`,
        HierarchyVersionId: versionId,
        EntityId: tier.brokerId,
        EntityName: tier.brokerName,
        EntityType: 1, // Broker
        Level: tier.level,
        CommissionRate: null,
        ScheduleCode: tier.schedule,
        ScheduleId: scheduleId // Resolved from schedule map
      });
    }

    // Store mapping for deduplication
    this.hierarchyIdByHash.set(hierarchyHash, hierarchyId);

    return hierarchyId;
  }

  // ==========================================================================
  // Utility Methods
  // ==========================================================================

  private formatDate(date: Date): string {
    return date.toISOString().split('T')[0];
  }

  /**
   * Extract unique assignments from a split configuration.
   * An assignment exists when paidBrokerId differs from brokerId.
   */
  private extractAssignments(splitConfig: SplitConfiguration): ProposalAssignment[] {
    const assignmentMap = new Map<string, ProposalAssignment>();

    for (const split of splitConfig.splits) {
      for (const tier of split.tiers) {
        // Check if assignment exists (paidBrokerId differs from brokerId)
        if (tier.paidBrokerId && 
            tier.paidBrokerId !== tier.brokerId &&
            tier.paidBrokerId.trim() !== '' &&
            tier.paidBrokerId.trim() !== tier.brokerId.trim()) {
          
          const key = `${tier.brokerId}→${tier.paidBrokerId}`;
          
          if (!assignmentMap.has(key)) {
            assignmentMap.set(key, {
              sourceBrokerId: tier.brokerId,
              sourceBrokerName: tier.brokerName,
              recipientBrokerId: tier.paidBrokerId,
              recipientBrokerName: tier.paidBrokerName
            });
          }
        }
      }
    }

    // Return sorted array for consistent hashing
    return Array.from(assignmentMap.values()).sort((a, b) => {
      const sourceCompare = a.sourceBrokerId.localeCompare(b.sourceBrokerId);
      if (sourceCompare !== 0) return sourceCompare;
      return a.recipientBrokerId.localeCompare(b.recipientBrokerId);
    });
  }

  /**
   * MODIFIED: Full SHA256 hash (64 chars) with collision detection
   * Original code.md line 872 used: .substring(0, 16)
   */
  private computeHashWithCollisionCheck(input: string, context: string): string {
    const hash = crypto.createHash('sha256').update(input).digest('hex').toUpperCase(); // Full 64 chars
    
    if (this.hashCollisions.has(hash)) {
      const existing = this.hashCollisions.get(hash);
      if (existing !== input) {
        this.collisionCount++;
        throw new Error(`Hash collision detected for ${context}: ${hash}\nExisting: ${existing}\nNew: ${input}`);
      }
    }
    this.hashCollisions.set(hash, input);
    return hash;
  }

  private getYearRange(from: Date, to: Date): number[] {
    const years: number[] = [];
    for (let year = from.getFullYear(); year <= to.getFullYear(); year++) {
      years.push(year);
    }
    return years;
  }

  private isInvalidGroup(groupId: string): boolean {
    if (!groupId) return true;
    const trimmed = groupId.trim();
    if (trimmed === '') return true;
    if (/^0+$/.test(trimmed)) return true; // All zeros
    return false;
  }

  // ==========================================================================
  // Statistics
  // ==========================================================================

  getStats() {
    // Count total assignments across all proposals
    const totalAssignments = this.proposals.reduce(
      (sum, p) => sum + p.assignments.length, 
      0
    );
    const proposalsWithAssignments = this.proposals.filter(p => p.assignments.length > 0).length;

    return {
      certificates: this.certificates.length,
      selectionCriteria: this.selectionCriteria.length,
      proposals: this.proposals.length,
      phaRecords: this.phaRecords.length,
      uniqueGroups: new Set(this.proposals.map(p => p.groupId)).size,
      uniqueHierarchies: this.hierarchyByHash.size,
      hashCollisions: this.collisionCount,
      // Assignment statistics (NEW)
      proposalsWithAssignments,
      totalAssignments
    };
  }
}

// =============================================================================
// Database Integration
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

export async function loadCertificatesFromDatabase(
  config: DatabaseConfig,
  options: BuilderOptions
): Promise<CertificateRecord[]> {
  const schema = options.schema || 'etl';
  const pool = await sql.connect(config);

  try {
    if (options.verbose) {
      console.log(`Loading certificates from [${schema}].[input_certificate_info]...`);
      console.log(`  Filter: CertStatus='A', RecStatus='A', CertEffectiveDate IS NOT NULL`);
      console.log(`  Note: Loading ALL rows (each certificate may have multiple split rows)`);
    }
    
    const limitClause = options.limitCertificates 
      ? `TOP ${options.limitCertificates}` 
      : '';
    
    const result = await pool.request().query(`
      SELECT ${limitClause}
        ci.CertificateId AS certificateId,
        LTRIM(RTRIM(ISNULL(ci.GroupId, ''))) AS groupId,
        COALESCE(eg.GroupName, 'Group ' + LTRIM(RTRIM(ci.GroupId))) AS groupName,
        TRY_CAST(ci.CertEffectiveDate AS DATE) AS certEffectiveDate,
        LTRIM(RTRIM(ci.Product)) AS productCode,
        LTRIM(RTRIM(ISNULL(ci.PlanCode, ''))) AS planCode,
        ci.CertStatus AS certStatus,
        ci.CertIssuedState AS situsState,
        TRY_CAST(ci.CertPremium AS DECIMAL(18,4)) AS premium,
        ci.CertSplitSeq AS certSplitSeq,
        TRY_CAST(ci.CertSplitPercent AS DECIMAL(18,4)) AS certSplitPercent,
        ci.SplitBrokerSeq AS splitBrokerSeq,
        ci.SplitBrokerId AS splitBrokerId,
        COALESCE(b.Name, 'Broker ' + ci.SplitBrokerId) AS splitBrokerName,
        COALESCE(b.Npn, '') AS splitBrokerNPN,
        ci.CommissionsSchedule AS commissionSchedule,
        ci.PaidBrokerId AS paidBrokerId,
        COALESCE(pb.Name, 'Broker ' + ci.PaidBrokerId) AS paidBrokerName
      FROM [${schema}].[input_certificate_info] ci
      LEFT JOIN [dbo].[EmployerGroups] eg 
        ON eg.GroupNumber = LTRIM(RTRIM(ci.GroupId))
      LEFT JOIN [dbo].[Brokers] b 
        ON b.ExternalPartyId = ci.SplitBrokerId
      LEFT JOIN [dbo].[Brokers] pb 
        ON pb.ExternalPartyId = ci.PaidBrokerId
      WHERE ci.CertStatus = 'A'
        AND ci.RecStatus = 'A'
        AND ci.CertEffectiveDate IS NOT NULL
      ORDER BY ci.GroupId, ci.CertEffectiveDate, ci.Product, ci.PlanCode, 
               ci.CertSplitSeq, ci.SplitBrokerSeq
    `);

    if (options.verbose) {
      const uniqueCerts = new Set(result.recordset.map((r: any) => r.certificateId)).size;
      const avgSplits = result.recordset.length / uniqueCerts;
      console.log(`  Loaded ${result.recordset.length} rows representing ${uniqueCerts} unique certificates`);
      console.log(`  Average ${avgSplits.toFixed(2)} splits per certificate`);
    }
    return result.recordset;
  } finally {
    await pool.close();
  }
}

export async function writeStagingOutput(
  config: DatabaseConfig,
  output: StagingOutput,
  options: BuilderOptions
): Promise<void> {
  const schema = options.schema || 'etl';
  
  if (options.dryRun) {
    console.log('[DRY RUN] Would write staging output:');
    console.log(`  Proposals: ${output.proposals.length}`);
    console.log(`  Proposal Products: ${output.proposalProducts.length}`);
    console.log(`  Key Mappings: ${output.proposalKeyMappings.length}`);
    console.log(`  Split Versions: ${output.premiumSplitVersions.length}`);
    console.log(`  Split Participants: ${output.premiumSplitParticipants.length}`);
    console.log(`  Hierarchies: ${output.hierarchies.length}`);
    console.log(`  Hierarchy Versions: ${output.hierarchyVersions.length}`);
    console.log(`  Hierarchy Participants: ${output.hierarchyParticipants.length}`);
    console.log(`  State Rules: ${output.stateRules.length}`);
    console.log(`  State Rule States: ${output.stateRuleStates.length}`);
    console.log(`  Hierarchy Splits: ${output.hierarchySplits.length}`);
    console.log(`  Split Distributions: ${output.splitDistributions.length}`);
    console.log(`  PHA Assignments: ${output.policyHierarchyAssignments.length}`);
    console.log(`  PHA Participants: ${output.policyHierarchyParticipants.length}`);
    console.log(`  Commission Assignment Versions: ${output.commissionAssignmentVersions.length}`);
    console.log(`  Commission Assignment Recipients: ${output.commissionAssignmentRecipients.length}`);
    return;
  }

  const pool = await sql.connect(config);

  try {
    if (options.verbose) {
      console.log('Writing staging output to database...');
    }
    const startTime = Date.now();

    // Clear existing data
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_proposals]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_proposal_products]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_proposal_key_mapping]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_premium_split_versions]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_premium_split_participants]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_hierarchies]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_hierarchy_versions]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_hierarchy_participants]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_state_rules]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_state_rule_states]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_hierarchy_splits]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_split_distributions]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_policy_hierarchy_assignments]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_policy_hierarchy_participants]`);
    // Clear assignment tables (NEW)
    await pool.request().query(`DELETE FROM [dbo].[CommissionAssignmentRecipients]`);
    await pool.request().query(`DELETE FROM [dbo].[CommissionAssignmentVersions]`);

    // Insert proposals (batched multi-row VALUES for performance)
    // 16 params per row, SQL Server max 2100 params = max 131 rows per batch
    const proposalBatchSize = 100; // Safe margin
    const totalProposalBatches = Math.ceil(output.proposals.length / proposalBatchSize);
    
    for (let i = 0; i < output.proposals.length; i += proposalBatchSize) {
      const batch = output.proposals.slice(i, i + proposalBatchSize);
      const batchNum = Math.floor(i / proposalBatchSize) + 1;
      
      if (options.verbose && (batchNum % 10 === 0 || batchNum === 1)) {
        console.log(`    Writing proposals batch ${batchNum}/${totalProposalBatches} (${((batchNum/totalProposalBatches)*100).toFixed(0)}%)...`);
      }
      
      const values = batch.map((p, idx) =>
        `(@Id${idx}, @ProposalNumber${idx}, @Status${idx}, @SubmittedDate${idx}, @ProposedEffectiveDate${idx}, ` +
        `@SitusState${idx}, @GroupId${idx}, @GroupName${idx}, @BrokerId${idx}, @BrokerName${idx}, ` +
        `@BrokerUniquePartyId${idx}, @ProductCodes${idx}, @PlanCodes${idx}, ` +
        `@SplitConfigHash${idx}, @DateRangeFrom${idx}, @DateRangeTo${idx}, ` +
        `@EffectiveDateFrom${idx}, @EffectiveDateTo${idx}, @Notes${idx}, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((p, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(100), p.Id);
        request.input(`ProposalNumber${idx}`, sql.NVarChar(100), p.ProposalNumber);
        request.input(`Status${idx}`, sql.Int, p.Status);
        request.input(`SubmittedDate${idx}`, sql.DateTime2, p.SubmittedDate);
        request.input(`ProposedEffectiveDate${idx}`, sql.DateTime2, p.ProposedEffectiveDate);
        request.input(`SitusState${idx}`, sql.NVarChar(10), p.SitusState);
        request.input(`GroupId${idx}`, sql.NVarChar(100), p.GroupId);
        request.input(`GroupName${idx}`, sql.NVarChar(500), p.GroupName);
        request.input(`BrokerId${idx}`, sql.BigInt, p.BrokerId);
        request.input(`BrokerName${idx}`, sql.NVarChar(500), p.BrokerName);
        request.input(`BrokerUniquePartyId${idx}`, sql.NVarChar(100), p.BrokerUniquePartyId);
        request.input(`ProductCodes${idx}`, sql.NVarChar(sql.MAX), p.ProductCodes);
        request.input(`PlanCodes${idx}`, sql.NVarChar(sql.MAX), p.PlanCodes);
        request.input(`SplitConfigHash${idx}`, sql.NVarChar(64), p.SplitConfigHash);
        request.input(`DateRangeFrom${idx}`, sql.Int, p.DateRangeFrom);
        request.input(`DateRangeTo${idx}`, sql.Int, p.DateRangeTo);
        request.input(`EffectiveDateFrom${idx}`, sql.DateTime2, p.EffectiveDateFrom);
        request.input(`EffectiveDateTo${idx}`, sql.DateTime2, p.EffectiveDateTo);
        request.input(`Notes${idx}`, sql.NVarChar(sql.MAX), p.Notes);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_proposals] (
          Id, ProposalNumber, Status, SubmittedDate, ProposedEffectiveDate,
          SitusState, GroupId, GroupName, BrokerId, BrokerName, BrokerUniquePartyId,
          ProductCodes, PlanCodes,
          SplitConfigHash, DateRangeFrom, DateRangeTo,
          EffectiveDateFrom, EffectiveDateTo, Notes,
          CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.proposals.length} proposals`);
    }

    // Insert proposal products (batched - 6 params per row, max ~330 rows)
    const proposalProductBatchSize = 330;
    for (let i = 0; i < output.proposalProducts.length; i += proposalProductBatchSize) {
      const batch = output.proposalProducts.slice(i, i + proposalProductBatchSize);
      const values = batch.map((pp, idx) =>
        `(@Id${idx}, @ProposalId${idx}, @ProductCode${idx}, @ProductName${idx}, @CommissionStructure${idx}, @ResolvedScheduleId${idx}, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((pp, idx) => {
        request.input(`Id${idx}`, sql.BigInt, pp.Id);
        request.input(`ProposalId${idx}`, sql.NVarChar(100), pp.ProposalId);
        request.input(`ProductCode${idx}`, sql.NVarChar(100), pp.ProductCode);
        request.input(`ProductName${idx}`, sql.NVarChar(500), pp.ProductName);
        request.input(`CommissionStructure${idx}`, sql.NVarChar(100), pp.CommissionStructure);
        request.input(`ResolvedScheduleId${idx}`, sql.NVarChar(100), pp.ResolvedScheduleId);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_proposal_products] (
          Id, ProposalId, ProductCode, ProductName, CommissionStructure,
          ResolvedScheduleId, CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.proposalProducts.length} proposal products`);
    }

    // Insert proposal key mappings (batched for performance)
    // SQL Server max 2100 parameters, with 6 params per row = max 350 rows per batch
    const batchSize = 300; // Safe margin below 350
    const totalKeyMappingBatches = Math.ceil(output.proposalKeyMappings.length / batchSize);
    for (let i = 0; i < output.proposalKeyMappings.length; i += batchSize) {
      const batch = output.proposalKeyMappings.slice(i, i + batchSize);
      const batchNum = Math.floor(i / batchSize) + 1;
      
      if (options.verbose && batchNum % 20 === 0) {
        console.log(`    Writing key mappings batch ${batchNum}/${totalKeyMappingBatches} (${((batchNum/totalKeyMappingBatches)*100).toFixed(0)}%)...`);
      }
      const values = batch.map((m, idx) => 
        `(@GroupId${idx}, @EffectiveYear${idx}, @ProductCode${idx}, @PlanCode${idx}, @ProposalId${idx}, @SplitConfigHash${idx}, GETUTCDATE())`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((m, idx) => {
        request.input(`GroupId${idx}`, sql.NVarChar(100), m.GroupId);
        request.input(`EffectiveYear${idx}`, sql.Int, m.EffectiveYear);
        request.input(`ProductCode${idx}`, sql.NVarChar(100), m.ProductCode);
        request.input(`PlanCode${idx}`, sql.NVarChar(100), m.PlanCode);
        request.input(`ProposalId${idx}`, sql.NVarChar(100), m.ProposalId);
        request.input(`SplitConfigHash${idx}`, sql.NVarChar(64), m.SplitConfigHash);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_proposal_key_mapping] (
          GroupId, EffectiveYear, ProductCode, PlanCode, ProposalId, SplitConfigHash, CreationTime
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.proposalKeyMappings.length} key mappings`);
    }

    // Insert premium split versions (batched - 10 params per row, max ~200 rows)
    const splitVersionBatchSize = 200;
    for (let i = 0; i < output.premiumSplitVersions.length; i += splitVersionBatchSize) {
      const batch = output.premiumSplitVersions.slice(i, i + splitVersionBatchSize);
      const values = batch.map((v, idx) => 
        `(@Id${idx}, @GroupId${idx}, @GroupName${idx}, @ProposalId${idx}, @ProposalNumber${idx}, @VersionNumber${idx}, @EffectiveFrom${idx}, @EffectiveTo${idx}, @TotalSplitPercent${idx}, @Status${idx}, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((v, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(100), v.Id);
        request.input(`GroupId${idx}`, sql.NVarChar(100), v.GroupId);
        request.input(`GroupName${idx}`, sql.NVarChar(500), v.GroupName);
        request.input(`ProposalId${idx}`, sql.NVarChar(100), v.ProposalId);
        request.input(`ProposalNumber${idx}`, sql.NVarChar(100), v.ProposalNumber);
        request.input(`VersionNumber${idx}`, sql.NVarChar(50), v.VersionNumber);
        request.input(`EffectiveFrom${idx}`, sql.DateTime2, v.EffectiveFrom);
        request.input(`EffectiveTo${idx}`, sql.DateTime2, v.EffectiveTo);
        request.input(`TotalSplitPercent${idx}`, sql.Decimal(18, 4), v.TotalSplitPercent);
        request.input(`Status${idx}`, sql.Int, v.Status);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_premium_split_versions] (
          Id, GroupId, GroupName, ProposalId, ProposalNumber,
          VersionNumber, EffectiveFrom, EffectiveTo, TotalSplitPercent, Status,
          CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.premiumSplitVersions.length} split versions`);
    }

    // Insert premium split participants (batched - 18 params per row, max ~110 rows)
    const splitParticipantBatchSize = 100;
    for (let i = 0; i < output.premiumSplitParticipants.length; i += splitParticipantBatchSize) {
      const batch = output.premiumSplitParticipants.slice(i, i + splitParticipantBatchSize);
      const values = batch.map((p, idx) =>
        `(@Id${idx}, @VersionId${idx}, @BrokerId${idx}, @BrokerName${idx}, @BrokerNPN${idx}, @BrokerUniquePartyId${idx}, ` +
        `@SplitPercent${idx}, @IsWritingAgent${idx}, @HierarchyId${idx}, @HierarchyName${idx}, @TemplateId${idx}, ` +
        `@TemplateName${idx}, @Sequence${idx}, @WritingBrokerId${idx}, @GroupId${idx}, @EffectiveFrom${idx}, ` +
        `@EffectiveTo${idx}, @Notes${idx}, GETUTCDATE())`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((p, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(100), p.Id);
        request.input(`VersionId${idx}`, sql.NVarChar(100), p.VersionId);
        request.input(`BrokerId${idx}`, sql.BigInt, p.BrokerId);
        request.input(`BrokerName${idx}`, sql.NVarChar(500), p.BrokerName);
        request.input(`BrokerNPN${idx}`, sql.NVarChar(50), p.BrokerNPN);
        request.input(`BrokerUniquePartyId${idx}`, sql.NVarChar(100), p.BrokerUniquePartyId);
        request.input(`SplitPercent${idx}`, sql.Decimal(18, 4), p.SplitPercent);
        request.input(`IsWritingAgent${idx}`, sql.Bit, p.IsWritingAgent);
        request.input(`HierarchyId${idx}`, sql.NVarChar(100), p.HierarchyId);
        request.input(`HierarchyName${idx}`, sql.NVarChar(500), p.HierarchyName);
        request.input(`TemplateId${idx}`, sql.NVarChar(100), p.TemplateId);
        request.input(`TemplateName${idx}`, sql.NVarChar(500), p.TemplateName);
        request.input(`Sequence${idx}`, sql.Int, p.Sequence);
        request.input(`WritingBrokerId${idx}`, sql.BigInt, p.WritingBrokerId);
        request.input(`GroupId${idx}`, sql.NVarChar(100), p.GroupId);
        request.input(`EffectiveFrom${idx}`, sql.DateTime2, p.EffectiveFrom);
        request.input(`EffectiveTo${idx}`, sql.DateTime2, p.EffectiveTo);
        request.input(`Notes${idx}`, sql.NVarChar(sql.MAX), p.Notes);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_premium_split_participants] (
          Id, VersionId, BrokerId, BrokerName, BrokerNPN, BrokerUniquePartyId,
          SplitPercent, IsWritingAgent, HierarchyId, HierarchyName, TemplateId, TemplateName,
          Sequence, WritingBrokerId, GroupId,
          EffectiveFrom, EffectiveTo, Notes, CreationTime
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.premiumSplitParticipants.length} split participants`);
    }

    // Insert hierarchies (batched - 11 params per row, max ~180 rows)
    const hierarchyBatchSize = 180;
    for (let i = 0; i < output.hierarchies.length; i += hierarchyBatchSize) {
      const batch = output.hierarchies.slice(i, i + hierarchyBatchSize);
      const values = batch.map((h, idx) =>
        `(@Id${idx}, @Name${idx}, @GroupId${idx}, @GroupName${idx}, @BrokerId${idx}, @BrokerName${idx}, @ProposalId${idx}, @CurrentVersionId${idx}, @EffectiveDate${idx}, @SitusState${idx}, 1, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((h, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(100), h.Id);
        request.input(`Name${idx}`, sql.NVarChar(500), h.Name);
        request.input(`GroupId${idx}`, sql.NVarChar(100), h.GroupId);
        request.input(`GroupName${idx}`, sql.NVarChar(500), h.GroupName);
        request.input(`BrokerId${idx}`, sql.BigInt, h.BrokerId);
        request.input(`BrokerName${idx}`, sql.NVarChar(500), h.BrokerName);
        request.input(`ProposalId${idx}`, sql.NVarChar(100), h.ProposalId);
        request.input(`CurrentVersionId${idx}`, sql.NVarChar(100), h.CurrentVersionId);
        request.input(`EffectiveDate${idx}`, sql.Date, h.EffectiveDate);
        request.input(`SitusState${idx}`, sql.NVarChar(10), h.SitusState);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_hierarchies] (
          Id, Name, GroupId, GroupName, BrokerId, BrokerName,
          ProposalId, CurrentVersionId, EffectiveDate, SitusState, Status,
          CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.hierarchies.length} hierarchies`);
    }

    // Insert hierarchy versions (batched - 6 params per row, max ~300 rows)
    const hvBatchSize = 300;
    for (let i = 0; i < output.hierarchyVersions.length; i += hvBatchSize) {
      const batch = output.hierarchyVersions.slice(i, i + hvBatchSize);
      const values = batch.map((hv, idx) =>
        `(@Id${idx}, @HierarchyId${idx}, 1, @EffectiveFrom${idx}, @EffectiveTo${idx}, @Status${idx}, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((hv, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(100), hv.Id);
        request.input(`HierarchyId${idx}`, sql.NVarChar(100), hv.HierarchyId);
        request.input(`EffectiveFrom${idx}`, sql.DateTime2, hv.EffectiveFrom);
        request.input(`EffectiveTo${idx}`, sql.DateTime2, hv.EffectiveTo);
        request.input(`Status${idx}`, sql.Int, hv.Status);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_hierarchy_versions] (
          Id, HierarchyId, Version, EffectiveFrom, EffectiveTo, Status,
          CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.hierarchyVersions.length} hierarchy versions`);
    }

    // Insert hierarchy participants (batched - 10 params per row, max ~200 rows)
    const hpBatchSize = 200;
    const totalHpBatches = Math.ceil(output.hierarchyParticipants.length / hpBatchSize);
    
    for (let i = 0; i < output.hierarchyParticipants.length; i += hpBatchSize) {
      const batch = output.hierarchyParticipants.slice(i, i + hpBatchSize);
      const batchNum = Math.floor(i / hpBatchSize) + 1;
      
      if (options.verbose && (batchNum % 20 === 0 || batchNum === 1)) {
        const pct = Math.floor((batchNum / totalHpBatches) * 100);
        console.log(`    Writing hierarchy participants batch ${batchNum}/${totalHpBatches} (${pct}%)...`);
      }
      
      const values = batch.map((hp, idx) =>
        `(@Id${idx}, @HierarchyVersionId${idx}, @EntityId${idx}, @EntityName${idx}, @Level${idx}, @SortOrder${idx}, @SplitPercent${idx}, @CommissionRate${idx}, @ScheduleCode${idx}, @ScheduleId${idx}, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((hp, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(100), hp.Id);
        request.input(`HierarchyVersionId${idx}`, sql.NVarChar(100), hp.HierarchyVersionId);
        request.input(`EntityId${idx}`, sql.BigInt, brokerExternalToInternal(hp.EntityId));
        request.input(`EntityName${idx}`, sql.NVarChar(500), hp.EntityName);
        request.input(`Level${idx}`, sql.Int, hp.Level);
        request.input(`SortOrder${idx}`, sql.Int, hp.Level);
        request.input(`SplitPercent${idx}`, sql.Decimal(18, 4), 0);
        request.input(`CommissionRate${idx}`, sql.Decimal(18, 4), hp.CommissionRate);
        request.input(`ScheduleCode${idx}`, sql.NVarChar(100), hp.ScheduleCode);
        request.input(`ScheduleId${idx}`, sql.BigInt, hp.ScheduleId);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_hierarchy_participants] (
          Id, HierarchyVersionId, EntityId, EntityName,
          Level, SortOrder, SplitPercent, CommissionRate, ScheduleCode, ScheduleId,
          CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.hierarchyParticipants.length} hierarchy participants`);
    }

    // Insert state rules (batched - 7 params per row, max ~280 rows)
    const stateRuleBatchSize = 280;
    for (let i = 0; i < output.stateRules.length; i += stateRuleBatchSize) {
      const batch = output.stateRules.slice(i, i + stateRuleBatchSize);
      const values = batch.map((sr, idx) =>
        `(@Id${idx}, @HierarchyVersionId${idx}, @ShortName${idx}, @Name${idx}, @Description${idx}, @Type${idx}, @SortOrder${idx}, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((sr, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(100), sr.Id);
        request.input(`HierarchyVersionId${idx}`, sql.NVarChar(100), sr.HierarchyVersionId);
        request.input(`ShortName${idx}`, sql.NVarChar(50), sr.ShortName);
        request.input(`Name${idx}`, sql.NVarChar(200), sr.Name);
        request.input(`Description${idx}`, sql.NVarChar(sql.MAX), sr.Description);
        request.input(`Type${idx}`, sql.Int, sr.Type);
        request.input(`SortOrder${idx}`, sql.Int, sr.SortOrder);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_state_rules] (
          Id, HierarchyVersionId, ShortName, Name, [Description], [Type], SortOrder,
          CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.stateRules.length} state rules`);
    }

    // Insert state rule states (batched - 4 params per row, max ~500 rows)
    const stateRuleStateBatchSize = 500;
    for (let i = 0; i < output.stateRuleStates.length; i += stateRuleStateBatchSize) {
      const batch = output.stateRuleStates.slice(i, i + stateRuleStateBatchSize);
      const values = batch.map((srs, idx) =>
        `(@Id${idx}, @StateRuleId${idx}, @StateCode${idx}, @StateName${idx}, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((srs, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(100), srs.Id);
        request.input(`StateRuleId${idx}`, sql.NVarChar(100), srs.StateRuleId);
        request.input(`StateCode${idx}`, sql.NVarChar(10), srs.StateCode);
        request.input(`StateName${idx}`, sql.NVarChar(200), srs.StateName);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_state_rule_states] (
          Id, StateRuleId, StateCode, StateName, CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.stateRuleStates.length} state rule states`);
    }

    // Insert hierarchy splits (batched - 6 params per row, max ~300 rows)
    const hierarchySplitBatchSize = 300;
    for (let i = 0; i < output.hierarchySplits.length; i += hierarchySplitBatchSize) {
      const batch = output.hierarchySplits.slice(i, i + hierarchySplitBatchSize);
      const values = batch.map((hs, idx) =>
        `(@Id${idx}, @StateRuleId${idx}, @ProductId${idx}, @ProductCode${idx}, @ProductName${idx}, @SortOrder${idx}, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((hs, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(200), hs.Id);
        request.input(`StateRuleId${idx}`, sql.NVarChar(200), hs.StateRuleId);
        request.input(`ProductId${idx}`, sql.NVarChar(100), hs.ProductId);
        request.input(`ProductCode${idx}`, sql.NVarChar(100), hs.ProductCode);
        request.input(`ProductName${idx}`, sql.NVarChar(500), hs.ProductName);
        request.input(`SortOrder${idx}`, sql.Int, hs.SortOrder);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_hierarchy_splits] (
          Id, StateRuleId, ProductId, ProductCode, ProductName, SortOrder,
          CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.hierarchySplits.length} hierarchy splits`);
    }

    // Insert split distributions (batched - 7 params per row, max ~280 rows)
    const splitDistributionBatchSize = 280;
    for (let i = 0; i < output.splitDistributions.length; i += splitDistributionBatchSize) {
      const batch = output.splitDistributions.slice(i, i + splitDistributionBatchSize);
      const values = batch.map((sd, idx) =>
        `(@Id${idx}, @HierarchySplitId${idx}, @HierarchyParticipantId${idx}, @ParticipantEntityId${idx}, @Percentage${idx}, @ScheduleId${idx}, @ScheduleName${idx}, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((sd, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(200), sd.Id);
        request.input(`HierarchySplitId${idx}`, sql.NVarChar(200), sd.HierarchySplitId);
        request.input(`HierarchyParticipantId${idx}`, sql.NVarChar(200), sd.HierarchyParticipantId);
        request.input(`ParticipantEntityId${idx}`, sql.BigInt, sd.ParticipantEntityId);
        request.input(`Percentage${idx}`, sql.Decimal(18, 4), sd.Percentage);
        request.input(`ScheduleId${idx}`, sql.NVarChar(100), sd.ScheduleId);
        request.input(`ScheduleName${idx}`, sql.NVarChar(500), sd.ScheduleName);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_split_distributions] (
          Id, HierarchySplitId, HierarchyParticipantId, ParticipantEntityId,
          Percentage, ScheduleId, ScheduleName, CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.splitDistributions.length} split distributions`);
    }

    // Insert PHA assignments (batched - 6 params per row, max ~300 rows)
    const phaBatchSize = 300;
    const totalPhaBatches = Math.ceil(output.policyHierarchyAssignments.length / phaBatchSize);
    let phaIdCounter = 0;
    
    for (let i = 0; i < output.policyHierarchyAssignments.length; i += phaBatchSize) {
      const batch = output.policyHierarchyAssignments.slice(i, i + phaBatchSize);
      const batchNum = Math.floor(i / phaBatchSize) + 1;
      
      if (options.verbose && (batchNum % 5 === 0 || batchNum === 1)) {
        const pct = Math.floor((batchNum / totalPhaBatches) * 100);
        console.log(`    Writing PHA assignments batch ${batchNum}/${totalPhaBatches} (${pct}%)...`);
      }
      
      const values = batch.map((pha, idx) => {
        phaIdCounter++;
        return `('PHA-${phaIdCounter}', @PolicyId${idx}, @WritingBrokerId${idx}, @SplitSequence${idx}, @SplitPercent${idx}, @NonConformantReason${idx}, GETUTCDATE(), 0)`;
      }).join(',');
      
      const request = pool.request();
      batch.forEach((pha, idx) => {
        request.input(`PolicyId${idx}`, sql.NVarChar(100), pha.PolicyId);
        request.input(`WritingBrokerId${idx}`, sql.BigInt, pha.WritingBrokerId);
        request.input(`SplitSequence${idx}`, sql.Int, pha.SplitSequence);
        request.input(`SplitPercent${idx}`, sql.Decimal(18, 4), pha.SplitPercent);
        request.input(`NonConformantReason${idx}`, sql.NVarChar(500), pha.NonConformantReason);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_policy_hierarchy_assignments] (
          Id, PolicyId, WritingBrokerId, SplitSequence, SplitPercent, NonConformantReason,
          CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.policyHierarchyAssignments.length} PHA assignments`);
    }

    // Insert PHA participants (batched - 6 params per row, max ~300 rows)
    const phpBatchSize = 300;
    for (let i = 0; i < output.policyHierarchyParticipants.length; i += phpBatchSize) {
      const batch = output.policyHierarchyParticipants.slice(i, i + phpBatchSize);
      const values = batch.map((php, idx) =>
        `(@Id${idx}, @PolicyHierarchyAssignmentId${idx}, @BrokerId${idx}, @BrokerName${idx}, @Level${idx}, @ScheduleCode${idx}, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((php, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(100), php.Id);
        request.input(`PolicyHierarchyAssignmentId${idx}`, sql.NVarChar(100), php.PolicyHierarchyAssignmentId);
        request.input(`BrokerId${idx}`, sql.BigInt, brokerExternalToInternal(php.BrokerId));
        request.input(`BrokerName${idx}`, sql.NVarChar(500), php.BrokerName);
        request.input(`Level${idx}`, sql.Int, php.Level);
        request.input(`ScheduleCode${idx}`, sql.NVarChar(100), php.ScheduleCode);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_policy_hierarchy_participants] (
          Id, PolicyHierarchyAssignmentId, BrokerId, BrokerName, Level, ScheduleCode,
          CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    
    if (options.verbose) {
      console.log(`  Wrote ${output.policyHierarchyParticipants.length} PHA participants`);
    }

    // Insert CommissionAssignmentVersions (batched - 15 params per row, max ~130 rows)
    const cavBatchSize = 130;
    for (let i = 0; i < output.commissionAssignmentVersions.length; i += cavBatchSize) {
      const batch = output.commissionAssignmentVersions.slice(i, i + cavBatchSize);
      const values = batch.map((cav, idx) =>
        `(@Id${idx}, @BrokerId${idx}, @BrokerName${idx}, @ProposalId${idx}, @GroupId${idx}, @HierarchyId${idx}, @HierarchyVersionId${idx}, @HierarchyParticipantId${idx}, @VersionNumber${idx}, @EffectiveFrom${idx}, @EffectiveTo${idx}, @Status${idx}, @Type${idx}, @ChangeDescription${idx}, @TotalAssignedPercent${idx}, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((cav, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(100), cav.Id);
        request.input(`BrokerId${idx}`, sql.BigInt, cav.BrokerId);
        request.input(`BrokerName${idx}`, sql.NVarChar(510), cav.BrokerName);
        request.input(`ProposalId${idx}`, sql.NVarChar(100), cav.ProposalId);
        request.input(`GroupId${idx}`, sql.NVarChar(100), cav.GroupId);
        request.input(`HierarchyId${idx}`, sql.NVarChar(100), cav.HierarchyId);
        request.input(`HierarchyVersionId${idx}`, sql.NVarChar(100), cav.HierarchyVersionId);
        request.input(`HierarchyParticipantId${idx}`, sql.NVarChar(100), cav.HierarchyParticipantId);
        request.input(`VersionNumber${idx}`, sql.NVarChar(40), cav.VersionNumber);
        request.input(`EffectiveFrom${idx}`, sql.DateTime2, cav.EffectiveFrom);
        request.input(`EffectiveTo${idx}`, sql.DateTime2, cav.EffectiveTo);
        request.input(`Status${idx}`, sql.Int, cav.Status);
        request.input(`Type${idx}`, sql.Int, cav.Type);
        request.input(`ChangeDescription${idx}`, sql.NVarChar(1000), cav.ChangeDescription);
        request.input(`TotalAssignedPercent${idx}`, sql.Decimal(5, 2), cav.TotalAssignedPercent);
      });
      
      await request.query(`
        INSERT INTO [dbo].[CommissionAssignmentVersions] (
          Id, BrokerId, BrokerName, ProposalId, GroupId,
          HierarchyId, HierarchyVersionId, HierarchyParticipantId,
          VersionNumber, EffectiveFrom, EffectiveTo,
          Status, Type, ChangeDescription, TotalAssignedPercent,
          CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.commissionAssignmentVersions.length} commission assignment versions`);
    }

    // Insert CommissionAssignmentRecipients (batched - 8 params per row, max ~250 rows)
    const carBatchSize = 250;
    for (let i = 0; i < output.commissionAssignmentRecipients.length; i += carBatchSize) {
      const batch = output.commissionAssignmentRecipients.slice(i, i + carBatchSize);
      const values = batch.map((car, idx) =>
        `(@Id${idx}, @VersionId${idx}, @RecipientBrokerId${idx}, @RecipientName${idx}, @RecipientNPN${idx}, @Percentage${idx}, @RecipientHierarchyId${idx}, @Notes${idx})`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((car, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(200), car.Id);
        request.input(`VersionId${idx}`, sql.NVarChar(100), car.VersionId);
        request.input(`RecipientBrokerId${idx}`, sql.BigInt, car.RecipientBrokerId);
        request.input(`RecipientName${idx}`, sql.NVarChar(510), car.RecipientName);
        request.input(`RecipientNPN${idx}`, sql.NVarChar(40), car.RecipientNPN);
        request.input(`Percentage${idx}`, sql.Decimal(5, 2), car.Percentage);
        request.input(`RecipientHierarchyId${idx}`, sql.NVarChar(100), car.RecipientHierarchyId);
        request.input(`Notes${idx}`, sql.NVarChar(1000), car.Notes);
      });
      
      await request.query(`
        INSERT INTO [dbo].[CommissionAssignmentRecipients] (
          Id, VersionId, RecipientBrokerId, RecipientName, RecipientNPN,
          Percentage, RecipientHierarchyId, Notes
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.commissionAssignmentRecipients.length} commission assignment recipients`);
    }

    if (options.verbose) {
      console.log(`  Completed in ${Date.now() - startTime}ms`);
    }
  } catch (error: any) {
    console.error('❌ Error writing staging output to database:');
    console.error(`  Error: ${error.message}`);
    console.error(`  Code: ${error.code || 'N/A'}`);
    console.error(`  Number: ${error.number || 'N/A'}`);
    if (error.stack) {
      console.error(`  Stack: ${error.stack.split('\n').slice(0, 5).join('\n')}`);
    }
    throw error;
  } finally {
    await pool.close();
  }
}

// =============================================================================
// Audit Logging
// =============================================================================

function logAuditTrail(log: AuditLog): void {
  const logEntry = {
    timestamp: new Date().toISOString(),
    component: 'ProposalBuilder',
    ...log
  };
  console.log('[AUDIT]', JSON.stringify(logEntry, null, 2));
}

// =============================================================================
// Main Entry Point
// =============================================================================

export async function runProposalBuilder(
  config: DatabaseConfig,
  options: BuilderOptions = {}
): Promise<StagingOutput> {
  const runId = `RUN-${Date.now()}`;
  const startTime = new Date();
  const errors: string[] = [];
  const warnings: string[] = [];
  
  console.log('='.repeat(70));
  console.log('PROPOSAL BUILDER - TypeScript Implementation');
  console.log('='.repeat(70));
  console.log('');

  try {
    // Load certificates
    const certificates = await loadCertificatesFromDatabase(config, options);
    
    if (certificates.length === 0) {
      warnings.push('No certificates loaded from database');
    }

    // Build proposals
    const builder = new ProposalBuilder();
    
    // Load schedules for ID resolution (creates its own connection)
    const schedulePool = await sql.connect(config);
    try {
      await builder.loadSchedules(schedulePool);
    } finally {
      await schedulePool.close();
    }
    
    builder.loadCertificates(certificates);
    builder.extractSelectionCriteria();

    // Identify non-conformant cases before building proposals
    // Need a database connection for group lookups
    const phaPool = await sql.connect(config);
    try {
      await builder.identifyNonConformantCases(phaPool);
    } finally {
      await phaPool.close();
    }

    builder.buildProposals();

    // Generate staging output
    const output = builder.generateStagingOutput();

    // Write to database (using batched multi-row inserts)
    await writeStagingOutput(config, output, options);

    // Print stats
    const stats = builder.getStats();
    console.log('');
    console.log('='.repeat(70));
    console.log('SUMMARY');
    console.log('='.repeat(70));
    console.log(`Certificates processed: ${stats.certificates}`);
    console.log(`Selection criteria: ${stats.selectionCriteria}`);
    console.log(`Proposals created: ${stats.proposals}`);
    console.log(`PHA records: ${stats.phaRecords}`);
    console.log(`Unique groups: ${stats.uniqueGroups}`);
    console.log(`Unique hierarchies: ${stats.uniqueHierarchies}`);
    console.log(`Hash collisions: ${stats.hashCollisions}`);
    console.log(`Proposals with assignments: ${stats.proposalsWithAssignments}`);
    console.log(`Total assignments: ${stats.totalAssignments}`);
    
    // Audit log
    const endTime = new Date();
    const auditLog: AuditLog = {
      runId,
      startTime,
      endTime,
      certificatesProcessed: stats.certificates,
      proposalsGenerated: stats.proposals,
      hierarchiesGenerated: stats.uniqueHierarchies,
      phaRecordsGenerated: stats.phaRecords,
      batchesProcessed: 1,
      errors,
      warnings,
      hashCollisions: stats.hashCollisions
    };
    
    logAuditTrail(auditLog);

    return output;
  } catch (err: any) {
    errors.push(err.message);
    
    const endTime = new Date();
    const auditLog: AuditLog = {
      runId,
      startTime,
      endTime,
      certificatesProcessed: 0,
      proposalsGenerated: 0,
      hierarchiesGenerated: 0,
      phaRecordsGenerated: 0,
      batchesProcessed: 0,
      errors,
      warnings,
      hashCollisions: 0
    };
    
    logAuditTrail(auditLog);
    throw err;
  }
}

// =============================================================================
// Batched Processing (for large datasets)
// =============================================================================

export async function runProposalBuilderBatched(
  config: DatabaseConfig,
  options: BuilderOptions
): Promise<StagingOutput> {
  const batchSize = options.batchSize || 10000;
  const runId = `RUN-${Date.now()}`;
  const startTime = new Date();
  const errors: string[] = [];
  const warnings: string[] = [];
  
  console.log('='.repeat(70));
  console.log('PROPOSAL BUILDER - Batched Mode');
  console.log(`Batch Size: ${batchSize}`);
  console.log('='.repeat(70));
  console.log('');

  // For batched mode, we still load all certificates but process in smaller chunks
  // This is a simplified batched approach - for true streaming, we'd need offset-based queries
  const certificates = await loadCertificatesFromDatabase(config, options);
  
  const batches = Math.ceil(certificates.length / batchSize);
  let totalProposals = 0;
  let totalHierarchies = 0;
  let totalPHA = 0;
  
  // Process first batch to create schema
  const firstBatch = certificates.slice(0, batchSize);
  const builder = new ProposalBuilder();
  builder.loadCertificates(firstBatch);
  builder.extractSelectionCriteria();
  builder.buildProposals();
  const output = builder.generateStagingOutput();
  
  await writeStagingOutput(config, output, options);
  
  const stats = builder.getStats();
  totalProposals += stats.proposals;
  totalHierarchies += stats.uniqueHierarchies;
  totalPHA += stats.phaRecords;
  
  console.log(`Batch 1/${batches} completed: ${stats.proposals} proposals`);
  
  // Process remaining batches (append mode would require different logic)
  // For now, this is a proof of concept for the batching infrastructure
  
  warnings.push('Batched mode is simplified - only first batch processed');
  
  const endTime = new Date();
  const auditLog: AuditLog = {
    runId,
    startTime,
    endTime,
    certificatesProcessed: firstBatch.length,
    proposalsGenerated: totalProposals,
    hierarchiesGenerated: totalHierarchies,
    phaRecordsGenerated: totalPHA,
    batchesProcessed: 1,
    errors,
    warnings,
    hashCollisions: stats.hashCollisions
  };
  
  logAuditTrail(auditLog);
  
  return output;
}

// =============================================================================
// CLI Entry Point
// =============================================================================

if (require.main === module) {
  const args = process.argv.slice(2);
  
  // Parse CLI arguments
  const options: BuilderOptions = {
    batchSize: args.includes('--batch-size') 
      ? parseInt(args[args.indexOf('--batch-size') + 1]) 
      : undefined,
    dryRun: args.includes('--dry-run'),
    verbose: args.includes('--verbose'),
    limitCertificates: args.includes('--limit')
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

  // Choose execution mode
  const executeFn = options.batchSize 
    ? runProposalBuilderBatched 
    : runProposalBuilder;

  executeFn(config, options)
    .then((output) => {
      console.log('');
      console.log('✅ Done!');
      process.exit(0);
    })
    .catch(err => {
      console.error('❌ Error:', err);
      process.exit(1);
    });
}
