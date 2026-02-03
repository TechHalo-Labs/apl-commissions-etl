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
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { BlobServiceClient, ContainerClient } from '@azure/storage-blob';

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
  productPlanPairs: Set<string>;
  configHash: string;
  effectiveDateFrom: Date;
  effectiveDateTo: Date;
  splitConfig: SplitConfiguration;
  certificateIds: string[];
  // NOTE: assignments removed - now tracked at broker level via brokerAssignments map
}

/** Policy Hierarchy Assignment (non-conformant cases) */
interface PolicyHierarchyAssignment {
  certificateId: string;
  groupId: string;
  effectiveDate: Date;
  splitConfig: SplitConfiguration;
  reason: string;
  entryType?: number;
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
  EffectiveDateTo: Date;  // 2099-01-01 = open-ended for commission calculations
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
  EffectiveTo: Date;  // 2099-01-01 = open-ended for commission calculations
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
  EffectiveTo: Date;  // 2099-01-01 = open-ended for commission calculations
  Notes: string | null;
}

interface StagingHierarchy {
  Id: string;
  Name: string;
  GroupId: string;
  GroupName: string | null;
  BrokerId: number;
  BrokerName: string | null;
  // Can be null for PHA-generated hierarchies
  ProposalId: string | null;
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
  EffectiveTo: Date;  // 2099-01-01 = open-ended for commission calculations
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
  Id: string;
  PolicyId: string;
  HierarchyId: string;  // Links to hierarchy created for this PHA
  WritingBrokerId: number;
  SplitSequence: number;
  SplitPercent: number;
  NonConformantReason: string | null;
  EntryType: number;
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
/** Execution mode for the proposal builder */
export type ExecutionMode = 'transform' | 'export' | 'full';

export interface BuilderOptions {
  mode?: ExecutionMode;       // Default: 'transform'
  batchSize?: number;         // Default: null (process all), Set to 1000-5000 for batching
  dryRun?: boolean;           // Default: false
  verbose?: boolean;          // Default: false
  limitCertificates?: number; // For testing
  schema?: string;            // Target schema (default: 'etl')
  referenceSchema?: string;   // Schema for reference tables like Schedules (default: 'dbo')
  productionSchema?: string;  // Production schema for export (default: 'dbo')
  groups?: string[];          // Optional: Only process these group IDs (e.g., ['26683', '12345'])
  bulkMode?: 'db' | 'blob';   // Default: 'db' (direct inserts) or 'blob' (bulk insert from Blob)
  bulkPrefix?: string;        // Optional: Blob folder prefix for bulk files
  blobConfig?: {
    containerUrl?: string;
    endpoint?: string;
    container?: string;
    token?: string;
  };
}

export interface EntropyOptions {
  highEntropyUniqueRatio: number;
  highEntropyShannon: number;
  dominantCoverageThreshold: number;
  phaClusterSizeThreshold: number;
  logEntropyByGroup?: boolean;
  verbose?: boolean;
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

export class ProposalBuilder {
  private certificates: CertificateRecord[] = [];
  private selectionCriteria: SelectionCriteria[] = [];
  private proposals: Proposal[] = [];
  private phaRecords: PolicyHierarchyAssignment[] = [];
  
  // Hash collision detection
  private hashCollisions = new Map<string, string>();
  private collisionCount = 0;
  
  // Proposal-specific hierarchies (each belongs to exactly one proposal)
  // Now includes proposalId to ensure 1:1 relationship
  private hierarchyByHash = new Map<string, {
    writingBrokerId: string;
    tiers: HierarchyTier[];
    groupId: string;
    splitPercent: number;
    proposalId: string;
  }>();
  
  // Map from hierarchy hash to generated hierarchy ID (for deduplication)
  private hierarchyIdByHash = new Map<string, string>();
  
  // Track used hierarchy IDs to prevent collisions (same broker/tier/split across different groups)
  private usedHierarchyIds = new Set<string>();
  
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
  
  // Excluded groups (loaded from stg_excluded_groups)
  private excludedGroups = new Set<string>();
  
  // Track (state, products) per hierarchy for generating hierarchy splits
  private hierarchyStateProducts = new Map<string, Map<string, Set<string>>>();
  
  // Schedule lookup map: ExternalId -> numeric Id
  private scheduleIdByExternalId = new Map<string, number>();

  // Broker-level commission assignments: brokerId -> { assignment details, effectiveDate }
  // Tracks the most recent assignment per broker (not per proposal)
  private brokerAssignments = new Map<string, {
    sourceBrokerId: string;
    sourceBrokerName: string | null;
    recipientBrokerId: string;
    recipientBrokerName: string | null;
    effectiveDate: Date;
  }>();

  // ==========================================================================
  // Step 0a: Load Excluded Groups (from stg_excluded_groups)
  // ==========================================================================

  async loadExcludedGroups(pool: any, schema: string = 'etl'): Promise<void> {
    console.log('Loading excluded groups from stg_excluded_groups...');
    try {
      const result = await pool.request().query(`
        SELECT GroupId, ExclusionReason
        FROM [${schema}].[stg_excluded_groups]
      `);
      
      for (const row of result.recordset) {
        // Store both with and without G prefix for matching
        this.excludedGroups.add(row.GroupId);
        // Also store without G prefix for matching against raw groupId
        if (row.GroupId.startsWith('G')) {
          this.excludedGroups.add(row.GroupId.substring(1));
        }
      }
      
      console.log(`  ✓ Loaded ${result.recordset.length} excluded groups`);
      if (result.recordset.length > 0) {
        const reasons = new Map<string, number>();
        for (const row of result.recordset) {
          reasons.set(row.ExclusionReason, (reasons.get(row.ExclusionReason) || 0) + 1);
        }
        for (const [reason, count] of reasons) {
          console.log(`    - ${reason}: ${count}`);
        }
      }
    } catch (err: any) {
      console.log(`  ⚠️ Could not load excluded groups: ${err.message}`);
      console.log(`  ⚠️ Proceeding without exclusion filter`);
    }
  }

  // Check if a group is excluded
  isExcludedGroup(groupId: string): boolean {
    if (!groupId) return false;
    const trimmed = groupId.trim();
    // Check both with and without G prefix
    return this.excludedGroups.has(trimmed) || 
           this.excludedGroups.has(`G${trimmed}`) ||
           this.excludedGroups.has(trimmed.replace(/^G/, ''));
  }

  // ==========================================================================
  // Step 0b: Load Schedules (for ID resolution)
  // ==========================================================================

  async loadSchedules(pool: any, referenceSchema: string = 'dbo'): Promise<void> {
    console.log(`Loading schedules for ID resolution from [${referenceSchema}].[Schedules]...`);
    const result = await pool.request().query(`
      SELECT Id, ExternalId
      FROM [${referenceSchema}].[Schedules]
      WHERE ExternalId IS NOT NULL
    `);
    
    for (const row of result.recordset) {
      // Trim whitespace from ExternalId to handle inconsistent data
      const trimmedExternalId = row.ExternalId?.trim();
      if (trimmedExternalId) {
        this.scheduleIdByExternalId.set(trimmedExternalId, row.Id);
      }
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
        
        // Track broker-level assignments: if splitBrokerId != paidBrokerId, it's an assignment
        // Keep only the most recent assignment per broker (based on certificate effective date)
        for (const r of splitRecords) {
          if (r.splitBrokerId && r.paidBrokerId && 
              r.splitBrokerId.trim() !== '' && r.paidBrokerId.trim() !== '' &&
              r.splitBrokerId !== r.paidBrokerId) {
            const existing = this.brokerAssignments.get(r.splitBrokerId);
            const certDate = new Date(cert.certEffectiveDate);
            
            // Keep most recent assignment for this broker
            if (!existing || certDate > existing.effectiveDate) {
              this.brokerAssignments.set(r.splitBrokerId, {
                sourceBrokerId: r.splitBrokerId,
                sourceBrokerName: r.splitBrokerName,
                recipientBrokerId: r.paidBrokerId,
                recipientBrokerName: r.paidBrokerName,
                effectiveDate: certDate
              });
            }
          }
        }
        
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
        
        // Compute hierarchy hash (CRITICAL: Include groupId so hierarchies are NOT shared across groups)
        // This fixes the bug where hierarchies were being reused for wrong groups
        // NOTE: paidBrokerId is intentionally EXCLUDED - it's for assignment tracking, not proposal grouping
        const hierarchyJson = JSON.stringify({
          groupId: cert.groupId,  // GROUP-SPECIFIC hierarchy
          splitPercent: splitPercent,  // Include split percent for unique ID generation
          tiers: tiers.map(t => ({ level: t.level, brokerId: t.brokerId, schedule: t.schedule }))
        });
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
      // NOTE: seq (splitSeq) is intentionally EXCLUDED - it's just an identifier, not a meaningful differentiator
      // This allows certificates with same hierarchy structure to merge regardless of their sequence number
      const configJson = JSON.stringify(
        splits.map(s => ({
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
    console.log(`  ✓ Collected ${this.brokerAssignments.size} broker-level assignments (most recent per broker)`);
  }

  // ==========================================================================
  // Step 3: Identify Non-Conformant Cases (Early Detection)
  // ==========================================================================

  async identifyNonConformantCases(pool: any): Promise<void> {
    console.log('Identifying non-conformant cases...');
    const startTime = Date.now();

    // NOTE: DTC check (GroupId = '00000') REMOVED - DTC groups are processed normally
    // NOTE: IsNonConformant database check REMOVED - using exclusion table instead
    // Groups are excluded via stg_excluded_groups table during certificate loading
    
    // Only check: Certificates with split percent != 100
    // This is a data quality issue that prevents valid proposal creation
    let splitMismatchCount = 0;
    for (const criteria of this.selectionCriteria) {
      if (criteria.splitConfig.totalSplitPercent !== 100) {
        this.phaRecords.push({
          certificateId: criteria.certificateIds[0],
          groupId: criteria.groupId,
          effectiveDate: criteria.effectiveDate,
          splitConfig: criteria.splitConfig,
          reason: 'NonConformant-CertificateSplitMismatch',
          entryType: 1
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
    console.log(`    - Split mismatches: ${splitMismatchCount}`);
    console.log(`    - Remaining conformant criteria: ${this.selectionCriteria.length}/${originalCount}`);
  }

  // ==========================================================================
  // Step 3b: Entropy Routing (V2)
  // ==========================================================================
  applyEntropyRouting(options: EntropyOptions): void {
    console.log('Applying entropy-based routing...');
    const startTime = Date.now();
    const remaining: SelectionCriteria[] = [];

    const addCriteriaToPha = (criteria: SelectionCriteria, reason: string, entryType: number) => {
      for (const certId of criteria.certificateIds) {
        this.phaRecords.push({
          certificateId: certId,
          groupId: criteria.groupId,
          effectiveDate: criteria.effectiveDate,
          splitConfig: criteria.splitConfig,
          reason,
          entryType
        });
      }
    };

    const byGroup = new Map<string, SelectionCriteria[]>();
    for (const criteria of this.selectionCriteria) {
      if (!byGroup.has(criteria.groupId)) byGroup.set(criteria.groupId, []);
      byGroup.get(criteria.groupId)!.push(criteria);
    }

    let routedToPha = 0;
    const computeMetrics = (clusters: Map<string, SelectionCriteria[]>, totalRecords: number) => {
      const uniqueConfigs = clusters.size;
      const simpleEntropy = uniqueConfigs / totalRecords;
      const dominantRecords = Math.max(...Array.from(clusters.values()).map(c => c.length));
      const dominantPct = dominantRecords / totalRecords;
      const probs = Array.from(clusters.values()).map(c => c.length / totalRecords);
      const shannonEntropy = -probs.reduce((sum, p) => sum + (p * Math.log2(p)), 0);
      return { uniqueConfigs, simpleEntropy, dominantRecords, dominantPct, shannonEntropy };
    };

    for (const [groupId, groupCriteria] of byGroup) {
      // Hard rule: invalid group -> PHA
      if (this.isInvalidGroup(groupId)) {
        for (const criteria of groupCriteria) {
          addCriteriaToPha(criteria, 'Invalid GroupId (null/empty/zeros)', 2);
          routedToPha++;
        }
        continue;
      }

      const clusters = new Map<string, SelectionCriteria[]>();
      for (const criteria of groupCriteria) {
        const key = criteria.configHash;
        if (!clusters.has(key)) clusters.set(key, []);
        clusters.get(key)!.push(criteria);
      }

      const totalRecords = groupCriteria.length;
      const {
        uniqueConfigs,
        simpleEntropy,
        dominantRecords,
        dominantPct,
        shannonEntropy
      } = computeMetrics(clusters, totalRecords);

      const isHighEntropy = (
        simpleEntropy > options.highEntropyUniqueRatio ||
        shannonEntropy > options.highEntropyShannon ||
        dominantPct < options.dominantCoverageThreshold
      );

      if (options.logEntropyByGroup || options.verbose) {
        console.log(`  Entropy ${groupId}: unique=${uniqueConfigs}, total=${totalRecords}, dominant=${dominantRecords}, simple=${simpleEntropy.toFixed(3)}, shannon=${shannonEntropy.toFixed(3)}`);
      }

      if (isHighEntropy) {
        for (const criteria of groupCriteria) {
          addCriteriaToPha(criteria, 'BusinessDrivenEntropy', 2);
          routedToPha++;
        }
        continue;
      }

      for (const cluster of clusters.values()) {
        if (cluster.length < options.phaClusterSizeThreshold) {
          for (const criteria of cluster) {
            addCriteriaToPha(criteria, 'HumanErrorOutlier', 1);
            routedToPha++;
          }
        } else {
          remaining.push(...cluster);
        }
      }
    }

    this.selectionCriteria = remaining;
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`  ✓ Entropy routing complete in ${elapsed}s`);
    console.log(`    - Routed to PHA: ${routedToPha}`);
    console.log(`    - Remaining conformant criteria: ${this.selectionCriteria.length}`);
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
          reason: 'Invalid GroupId (null/empty/zeros)',
          entryType: 2
        });
        continue;
      }

      const productPlanKey = `${criteria.productCode}||${criteria.planCode}`;

      if (!proposalMap.has(key)) {
        this.proposalCounter++;
        // NOTE: Assignments are now tracked at broker level via brokerAssignments map
        // (collected during extractSelectionCriteria)
        
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
          productPlanPairs: new Set([productPlanKey]),
          configHash: criteria.configHash,
          effectiveDateFrom: criteria.effectiveDate,
          effectiveDateTo: criteria.effectiveDate,
          splitConfig: criteria.splitConfig,
          certificateIds: [...criteria.certificateIds]
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

        // Track product+plan pairs
        proposal.productPlanPairs.add(productPlanKey);
        
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
    
    // Store proposal-specific hierarchies (each hierarchy belongs to exactly one proposal)
    // Remove deduplication logic - each proposal gets its own hierarchy even if structures are identical
    for (const proposal of this.proposals) {
      for (const split of proposal.splitConfig.splits) {
        // Create proposal-specific hierarchy hash by including proposalId
        const proposalSpecificHash = `${split.hierarchyHash}-PROPOSAL-${proposal.id}`;

        this.hierarchyByHash.set(proposalSpecificHash, {
          writingBrokerId: split.writingBrokerId,
          tiers: split.tiers,
          groupId: proposal.groupId,
          splitPercent: split.splitPercent,
          proposalId: proposal.id  // Track which proposal this hierarchy belongs to
        });

        // Update the split to use the proposal-specific hash
        split.hierarchyHash = proposalSpecificHash;
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
    // Helper: Subtract one day from date
    // REASON: Commission runner uses > (not >=) for date comparison
    // So we need proposal.EffectiveDateFrom to be one day BEFORE the actual effective date
    // Example: Certificate effective 2024-10-01 needs proposal.EffectiveDateFrom = 2024-09-30
    //          so that (2024-10-01 > 2024-09-30) = true
    const subtractOneDay = (date: Date): Date => {
      const result = new Date(date);
      result.setDate(result.getDate() - 1);
      return result;
    };

    // Generate proposals
    for (const p of this.proposals) {
      output.proposals.push({
        Id: p.id,
        ProposalNumber: p.proposalNumber,
        Status: 1, // ProposalStatus.Active (0=Pending, 1=Active, 2=Superseded)
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
        EffectiveDateFrom: subtractOneDay(p.effectiveDateFrom),  // -1 day for > comparison
        EffectiveDateTo: new Date('2099-01-01'),  // Far-future = open-ended
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
      // NOTE: Use proposal ID in the versionId to avoid collisions with existing production data
      const versionId = `PSV-${p.id}`;  // e.g., PSV-PROP-26683-1
      output.premiumSplitVersions.push({
        Id: versionId,
        GroupId: p.groupId,
        GroupName: p.groupName,
        ProposalId: p.id,
        ProposalNumber: p.proposalNumber,
        VersionNumber: 'V1',
        EffectiveFrom: subtractOneDay(p.effectiveDateFrom),  // -1 day for > comparison
        EffectiveTo: new Date('2099-01-01'),  // Far-future = open-ended
        TotalSplitPercent: p.splitConfig.totalSplitPercent,
        Status: 1  // SplitVersionStatus.Active (Draft=0, Active=1)
      });

      // Generate premium split participants (one per split)
      for (const split of p.splitConfig.splits) {
        // Get or create hierarchy
        const hierarchyId = this.getOrCreateHierarchy(
          split.hierarchyHash,
          p.groupId,
          p.groupName,
          p.id,
          subtractOneDay(p.effectiveDateFrom),
          p.situsState,
          output
        );
        
        // NOTE: Use proposal ID + split sequence to avoid collisions with existing production data
        output.premiumSplitParticipants.push({
          Id: `PSP-${p.id}-${split.splitSeq}`,  // e.g., PSP-PROP-26683-1-1
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
          EffectiveFrom: subtractOneDay(p.effectiveDateFrom),  // -1 day for > comparison
          EffectiveTo: new Date('2099-01-01'),  // Far-future = open-ended
          Notes: null
        });
      }

      // NOTE: Per-proposal commission assignments REMOVED
      // Assignments are now generated at broker level after all proposals (see below)
    }

    // Generate BROKER-LEVEL commission assignments (NEW LOGIC)
    // ProposalId = '__DEFAULT__' - assignments are per broker, not per proposal
    // Only most recent assignment per broker is kept (tracked in brokerAssignments map)
    console.log(`  Generating ${this.brokerAssignments.size} broker-level commission assignments...`);
    for (const [brokerId, assignment] of this.brokerAssignments) {
      const cavId = `CAV-${brokerId}`;  // One per broker
      
      output.commissionAssignmentVersions.push({
        Id: cavId,
        BrokerId: brokerExternalToInternal(assignment.sourceBrokerId),
        BrokerName: assignment.sourceBrokerName,
        ProposalId: '__DEFAULT__',  // KEY CHANGE: Broker-level, not proposal-level
        GroupId: null,
        HierarchyId: null,
        HierarchyVersionId: null,
        HierarchyParticipantId: null,
        VersionNumber: '1',
        EffectiveFrom: subtractOneDay(assignment.effectiveDate),
        EffectiveTo: new Date('2099-01-01'),  // Far-future = open-ended
        Status: 3, // AssignmentStatus.Active (Draft=0, Future=1, Historical=2, Active=3)
        Type: 1, // Full assignment
        ChangeDescription: `Broker assignment: ${assignment.sourceBrokerName || assignment.sourceBrokerId} → ${assignment.recipientBrokerName || assignment.recipientBrokerId}`,
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
        Notes: `Broker assignment - most recent effective ${assignment.effectiveDate.toISOString().split('T')[0]}`
      });
    }
    console.log(`  ✓ Generated ${output.commissionAssignmentVersions.length} broker-level assignments`);

    // Generate PHA records for non-conformant policies
    // Each PHA gets its own unique hierarchy to ensure referential integrity
    console.log(`  Generating ${this.phaRecords.length} PHA records with hierarchies...`);
    for (const pha of this.phaRecords) {
      for (const split of pha.splitConfig.splits) {
        this.phaCounter++;
        const phaGroupKey = pha.groupId.replace(/[^A-Za-z0-9]/g, '') || 'UNKNOWN';
        const phaAssignmentId = `PHA-${phaGroupKey}-${this.phaCounter}`;
        
        // Create a unique hierarchy for this PHA
        // Note: We don't deduplicate PHA hierarchies because of unique constraint on (PolicyId, HierarchyId, WritingBrokerId)
        this.hierarchyCounter++;
        const phaHierarchyId = `H-PHA-${phaGroupKey}-${this.hierarchyCounter}`;
        this.hvCounter++;
        const phaHierarchyVersionId = `HV-PHA-${phaGroupKey}-${this.hvCounter}`;
        
        const writingBrokerId = brokerExternalToInternal(split.writingBrokerId);
        const writingBrokerName = split.tiers[0]?.brokerName || null;
        const groupId = `G${pha.groupId}`;
        const phaEffectiveFrom = subtractOneDay(pha.effectiveDate);
        
        // Create hierarchy for PHA
        output.hierarchies.push({
          Id: phaHierarchyId,
          Name: `PHA Hierarchy for Policy ${pha.certificateId}`,
          GroupId: groupId,
          GroupName: null,
          BrokerId: writingBrokerId,
          BrokerName: writingBrokerName,
          ProposalId: null,  // PHA hierarchies don't have proposals
          CurrentVersionId: phaHierarchyVersionId,
          EffectiveDate: phaEffectiveFrom,
          SitusState: null,
          Status: 0  // HierarchyStatus.Active (Active=0, Inactive=1)
        });

        // Create hierarchy version for PHA
        output.hierarchyVersions.push({
          Id: phaHierarchyVersionId,
          HierarchyId: phaHierarchyId,
          VersionNumber: 'V1',
          EffectiveFrom: phaEffectiveFrom,
          EffectiveTo: new Date('2099-01-01'),
          Status: 1  // HierarchyVersionStatus.Active (Draft=0, Active=1)
        });

        // Create hierarchy participants for PHA
        for (const tier of split.tiers) {
          this.hpCounter++;
          const scheduleId = tier.schedule ? this.scheduleIdByExternalId.get(tier.schedule.trim()) || null : null;
          
          output.hierarchyParticipants.push({
            Id: `HP-PHA-${phaGroupKey}-${this.hpCounter}`,
            HierarchyVersionId: phaHierarchyVersionId,
            EntityId: tier.brokerId,
            EntityName: tier.brokerName,
            EntityType: 1, // Broker
            Level: tier.level,
            CommissionRate: null,
            ScheduleCode: tier.schedule,
            ScheduleId: scheduleId
          });
        }
        
        // Create PHA assignment with HierarchyId
        output.policyHierarchyAssignments.push({
          Id: phaAssignmentId,
          PolicyId: pha.certificateId,
          HierarchyId: phaHierarchyId,
          WritingBrokerId: writingBrokerId,
          SplitSequence: split.splitSeq,
          SplitPercent: split.splitPercent,
          NonConformantReason: pha.reason,
          EntryType: pha.entryType ?? 0
        });

        // Generate PHA participants
        for (const tier of split.tiers) {
          this.phpCounter++;
          output.policyHierarchyParticipants.push({
            Id: `PHP-${phaGroupKey}-${this.phpCounter}`,
            PolicyHierarchyAssignmentId: phaAssignmentId,
            BrokerId: tier.brokerId,
            BrokerName: tier.brokerName,
            Level: tier.level,
            ScheduleCode: tier.schedule
          });
        }
      }
    }
    console.log(`  ✓ Generated ${output.policyHierarchyAssignments.length} PHA assignments with hierarchies`);

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
      
      // FIXED: Always create state-specific rules (never DEFAULT) for proper referential integrity
      // This ensures sr.ShortName = p.[State] joins work correctly in commission processing
      // Previously, single-state hierarchies used ShortName='DEFAULT' which broke the join
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
          Id: `SRS-${stateRuleId}-${stateCode}`,
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
              const hierarchySplitId = `HS-${stateRuleId}-${productCode}`;
              output.hierarchySplits.push({
                Id: hierarchySplitId,
                StateRuleId: stateRuleId,
                ProductId: productCode,  // Products.Id = ProductCode in dbo.Products
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
        
        // FIX: Resolve ScheduleId from ScheduleCode (ExternalId) -> numeric Id
        // This ensures we always use the correct numeric ID from the Schedules table
        // The commission runner parses ScheduleId as long - must be numeric, not ExternalId
        // Note: .trim() handles whitespace in source data (CertificateInfo.CommissionsSchedule)
        const trimmedScheduleCode = participant.ScheduleCode?.trim() || null;
        const resolvedScheduleId = trimmedScheduleCode 
          ? this.scheduleIdByExternalId.get(trimmedScheduleCode) || null 
          : null;
        
        if (trimmedScheduleCode && !resolvedScheduleId) {
          console.warn(`  ⚠️ Schedule not found for code: ${trimmedScheduleCode}`);
        }
        
        output.splitDistributions.push({
          Id: `SD-${split.Id}-${participant.Id}`,
          HierarchySplitId: split.Id,
          HierarchyParticipantId: participant.Id,
          ParticipantEntityId: brokerExternalToInternal(participant.EntityId),
          Percentage: 100 / participants.length, // Equal distribution across participants
          ScheduleId: resolvedScheduleId ? String(resolvedScheduleId) : null, // FIXED: Use resolved numeric ID
          ScheduleName: participant.ScheduleCode ? `Schedule ${participant.ScheduleCode}` : null
        });
      }
    }
    console.log(`  ✓ Generated ${output.splitDistributions.length} split distributions`);

    // POST-PROCESSING: Fix overlapping date ranges for proposals in the same group
    // This may create CONTINUATION proposals with their own key mappings
    this.fixOverlappingDateRanges(output);

    // DO NOT deduplicate key mappings - multiple proposals can cover the same (GroupId, Year, Product, Plan)
    // The validation query checks if the cert date falls within the proposal's date range,
    // so having multiple mappings for the same year is correct when proposals cover different parts of that year.
    // 
    // Example: PROP-25565-1 covers Jan-Jun 2024, PROP-25565-1-CONT covers Jun-Dec 2024
    // Both should have key mappings for Year 2024, and the date range check selects the right one.

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`  ✓ Generated all staging entities in ${elapsed}s`);
    console.log(`    Proposals: ${output.proposals.length}, Proposal Products: ${output.proposalProducts.length}, Hierarchies: ${output.hierarchies.length}, Key Mappings: ${output.proposalKeyMappings.length}`);
    
    return output;
  }

  // ==========================================================================
  // Helper: Fix Overlapping Date Ranges
  // ==========================================================================
  // When multiple proposals exist for the same group, we need to handle product+plan overlap:
  // 
  // - Product+plan pairs that EXIST in the next proposal: truncate (superseded by new schedules)
  // - Product+plan pairs that DON'T EXIST in next proposal: create CONTINUATION proposal
  // 
  // Example: Group G26683 has:
  //   - PROP-26683-1 (2024-10-01): GAO21HFM, GC14 CL1, GCI21HFM, MEDLINK9CM with AF01/AF02
  //   - PROP-26683-2 (2025-10-01): GAO21HFM, GC14 CL1, GCI21HFM with NW-PB4/NW-RZ4
  // 
  // Result after fix:
  //   - PROP-26683-1: ALL 4 products (2024-09-30 → 2025-09-30) AF01/AF02 [TRUNCATED]
  //   - PROP-26683-2: 3 products (2025-09-30 → 2099-01-01) NW-PB4/NW-RZ4
  //   - PROP-26683-1-CONT: MEDLINK9CM only (2025-09-30 → 2099-01-01) AF01/AF02 [NEW]

  private fixOverlappingDateRanges(output: StagingOutput): void {
    const parsePairKey = (pairKey: string): { productCode: string; planCode: string } => {
      const [productCode, planCode] = pairKey.split('||');
      return { productCode, planCode };
    };

    // Build product+plan sets for each proposal (from internal proposals)
    const proposalPairsById = new Map<string, Set<string>>();
    for (const proposal of this.proposals) {
      proposalPairsById.set(proposal.id, new Set(proposal.productPlanPairs));
    }

    // Group proposals by GroupId
    const proposalsByGroup = new Map<string, StagingProposal[]>();
    for (const proposal of output.proposals) {
      const groupId = proposal.GroupId;
      if (!proposalsByGroup.has(groupId)) {
        proposalsByGroup.set(groupId, []);
      }
      proposalsByGroup.get(groupId)!.push(proposal);
    }

    // Track new proposals to add (can't modify array while iterating)
    const continuationProposals: StagingProposal[] = [];

    for (const [groupId, proposals] of proposalsByGroup) {
      if (proposals.length <= 1) {
        // Single proposal - extend to full date range
        if (proposals.length === 1) {
          const p = proposals[0];
          p.EffectiveDateFrom = new Date('1901-01-01');
          p.EffectiveDateTo = new Date('2099-01-01');
          this.updateRelatedEntitiesDateRange(output, p.Id, p.EffectiveDateFrom, p.EffectiveDateTo);
        }
        continue;
      }

      // Sort by EffectiveDateFrom ascending (original cert-based dates)
      proposals.sort((a, b) => a.EffectiveDateFrom.getTime() - b.EffectiveDateFrom.getTime());
      
      // Store original start dates (these are based on cert effective dates)
      const originalStartDates = new Map<string, Date>();
      for (const p of proposals) {
        originalStartDates.set(p.Id, new Date(p.EffectiveDateFrom));
      }

      // ========================================================================
      // PASS 2: Extend where safe (no product+plan overlap)
      // ========================================================================
      // For each proposal, try to extend start backward and end forward
      // Only extend if no other proposal shares any product+plan in that direction

      // Get product+plan sets for each proposal
      const getProductPlanSet = (proposalId: string): Set<string> => {
        // Try internal proposals first
        let pairs = proposalPairsById.get(proposalId);
        if (!pairs) {
          // Fall back to parsing from StagingProposal
          const stagingProposal = proposals.find(p => p.Id === proposalId);
          if (stagingProposal) {
            pairs = new Set<string>();
            const products = stagingProposal.ProductCodes.split(',');
            const plans = stagingProposal.PlanCodes.split(',');
            for (const product of products) {
              for (const plan of plans) {
                pairs.add(`${product.trim()}||${plan.trim()}`);
              }
            }
          } else {
            pairs = new Set<string>();
          }
        }
        return pairs;
      };

      // Check if two proposals share any product+plan
      const hasOverlap = (pairs1: Set<string>, pairs2: Set<string>): boolean => {
        for (const pair of pairs1) {
          if (pairs2.has(pair)) return true;
        }
        return false;
      };

      for (let i = 0; i < proposals.length; i++) {
        const current = proposals[i];
        const currentPairs = getProductPlanSet(current.Id);
        const currentOriginalStart = originalStartDates.get(current.Id)!;

        // Extend START backward
        // Find the LAST earlier proposal that overlaps on products
        // If found, start at day after that proposal's original start (they share time boundary)
        // If not found, extend to 1901-01-01
        let newStart: Date = new Date('1901-01-01');
        for (let j = i - 1; j >= 0; j--) {
          const earlier = proposals[j];
          const earlierPairs = getProductPlanSet(earlier.Id);
          if (hasOverlap(currentPairs, earlierPairs)) {
            // Found overlapping earlier proposal - can't extend past this one's start
            // Keep our original start date (they share a time boundary)
            newStart = currentOriginalStart;
            break;
          }
        }
        current.EffectiveDateFrom = newStart;

        // Extend END forward
        // Find the FIRST later proposal that overlaps on products
        // If found, end at day before that proposal's original start
        // If not found, extend to 2099-01-01
        let newEnd: Date = new Date('2099-01-01');
        for (let j = i + 1; j < proposals.length; j++) {
          const later = proposals[j];
          const laterPairs = getProductPlanSet(later.Id);
          if (hasOverlap(currentPairs, laterPairs)) {
            // Found overlapping later proposal - end before its original start
            const laterOriginalStart = originalStartDates.get(later.Id)!;
            newEnd = new Date(laterOriginalStart);
            newEnd.setDate(newEnd.getDate() - 1);
            break;
          }
        }
        current.EffectiveDateTo = newEnd;

        // Update related entities (PSV, PSP)
        this.updateRelatedEntitiesDateRange(output, current.Id, current.EffectiveDateFrom, current.EffectiveDateTo);
        
        const startStr = current.EffectiveDateFrom.toISOString().split('T')[0];
        const endStr = current.EffectiveDateTo.toISOString().split('T')[0];
        console.log(`  ${current.Id} → ${startStr} to ${endStr} (products: ${current.ProductCodes})`);
      }

      // ========================================================================
      // PASS 3: Create continuation proposals for non-overlapping products
      // ========================================================================
      // If proposal A's end was truncated because of overlap with later proposal B:
      // - Product+plan pairs in A that DON'T overlap with B need to continue
      // - The continuation starts the day after A ends
      
      for (let i = 0; i < proposals.length; i++) {
        const current = proposals[i];
        const currentPairs = getProductPlanSet(current.Id);

        // Only process if current was truncated (doesn't extend to 2099)
        if (current.EffectiveDateTo.getTime() >= new Date('2099-01-01').getTime()) {
          continue;
        }

        // Find the proposal that caused the truncation (first later one with overlap)
        let truncatingProposal: StagingProposal | null = null;
        for (let j = i + 1; j < proposals.length; j++) {
          const later = proposals[j];
          const laterPairs = getProductPlanSet(later.Id);
          if (hasOverlap(currentPairs, laterPairs)) {
            truncatingProposal = later;
            break;
          }
        }

        if (!truncatingProposal) continue;

        const truncatingPairs = getProductPlanSet(truncatingProposal.Id);

        // Find product+plan pairs that are ONLY in current (not in truncating)
        const onlyInCurrentPairs = [...currentPairs].filter(p => !truncatingPairs.has(p));

        if (onlyInCurrentPairs.length === 0) continue;

        const continuationPairs = onlyInCurrentPairs.map(parsePairKey);
        const continuationProducts = Array.from(new Set(continuationPairs.map(pair => pair.productCode)));
        const continuationPlans = Array.from(new Set(continuationPairs.map(pair => pair.planCode)));
        const continuationLabel = continuationPairs.slice(0, 5).map(pair => `${pair.productCode}/${pair.planCode}`).join(', ');

        this.proposalCounter++;
        const contId = `${current.Id}-CONT`;
        
        // Continuation starts the day after current ends
        const contStart = new Date(current.EffectiveDateTo);
        contStart.setDate(contStart.getDate() + 1);
        
        console.log(`  Continuation: ${contId} from ${contStart.toISOString().split('T')[0]} for: ${continuationLabel}${onlyInCurrentPairs.length > 5 ? '...' : ''}`);

        continuationProposals.push({
          Id: contId,
          ProposalNumber: contId,
          Status: current.Status,
          SubmittedDate: contStart,
          ProposedEffectiveDate: contStart,
          SitusState: current.SitusState,
          GroupId: current.GroupId,
          GroupName: current.GroupName,
          BrokerId: current.BrokerId,
          BrokerName: current.BrokerName,
          BrokerUniquePartyId: current.BrokerUniquePartyId,
          ProductCodes: continuationProducts.join(','),
          PlanCodes: continuationPlans.join(','),
          SplitConfigHash: current.SplitConfigHash,
          DateRangeFrom: contStart.getFullYear(),
          DateRangeTo: 2099,
          EffectiveDateFrom: contStart,
          EffectiveDateTo: new Date('2099-01-01'),
          Notes: `Continuation of ${current.Id} for product+plan pairs not in ${truncatingProposal.Id}`
        });

        // Create continuation entities
        this.createContinuationEntities(
          current, 
          contId, 
          continuationPairs, 
          contStart, 
          output
        );
      }
      
      console.log(`  ✓ Fixed date ranges for group ${groupId}: ${proposals.length} proposals processed`);
    }

    // Add continuation proposals to output
    output.proposals.push(...continuationProposals);
  }

  /**
   * Update PSV and PSP date ranges for a proposal
   */
  private updateRelatedEntitiesDateRange(output: StagingOutput, proposalId: string, from: Date, to: Date): void {
    const psv = output.premiumSplitVersions.find(v => v.ProposalId === proposalId);
    if (psv) {
      psv.EffectiveFrom = from;
      psv.EffectiveTo = to;
      for (const psp of output.premiumSplitParticipants) {
        if (psp.VersionId === psv.Id) {
          psp.EffectiveFrom = from;
          psp.EffectiveTo = to;
        }
      }
    }
  }

  /**
   * Create all supporting entities for a continuation proposal
   */
  private createContinuationEntities(
    sourceProposal: StagingProposal,
    contProposalId: string,
    pairs: Array<{ productCode: string; planCode: string }>,
    effectiveFrom: Date,
    output: StagingOutput
  ): void {
    const uniqueProducts = Array.from(new Set(pairs.map(pair => pair.productCode)));

    // Find source hierarchy via PSP -> Hierarchy chain
    const sourcePsv = output.premiumSplitVersions.find(v => v.ProposalId === sourceProposal.Id);
    if (!sourcePsv) return;

    const sourcePsps = output.premiumSplitParticipants.filter(p => p.VersionId === sourcePsv.Id);
    if (sourcePsps.length === 0) return;

    // Create new PSV for continuation
    this.splitVersionCounter++;
    const contPsvId = `PSV-${contProposalId}`;
    
    output.premiumSplitVersions.push({
      Id: contPsvId,
      GroupId: sourcePsv.GroupId,
      GroupName: sourcePsv.GroupName,
      ProposalId: contProposalId,
      ProposalNumber: contProposalId,
      VersionNumber: 'V1',
      EffectiveFrom: effectiveFrom,
      EffectiveTo: new Date('2099-01-01'),
      TotalSplitPercent: sourcePsv.TotalSplitPercent,
      Status: sourcePsv.Status
    });

    // For each source PSP, create continuation PSP with NEW hierarchy
    for (const sourcePsp of sourcePsps) {
      const sourceHierarchy = output.hierarchies.find(h => h.Id === sourcePsp.HierarchyId);
      if (!sourceHierarchy) continue;

      // Create new hierarchy for continuation (1:1 with proposal)
      this.hierarchyCounter++;
      const contHierarchyId = `H-${contProposalId}-${sourcePsp.Sequence}`;
      const contHvId = `${contHierarchyId}-V1`;

      output.hierarchies.push({
        Id: contHierarchyId,
        Name: `Continuation hierarchy for ${contProposalId}`,
        GroupId: sourceHierarchy.GroupId,
        GroupName: sourceHierarchy.GroupName,
        BrokerId: sourceHierarchy.BrokerId,
        BrokerName: sourceHierarchy.BrokerName,
        ProposalId: contProposalId,
        CurrentVersionId: contHvId,
        EffectiveDate: effectiveFrom,
        SitusState: sourceHierarchy.SitusState,
        Status: sourceHierarchy.Status
      });

      // Find source hierarchy version
      const sourceHv = output.hierarchyVersions.find(hv => hv.HierarchyId === sourceHierarchy.Id);
      if (!sourceHv) continue;

      output.hierarchyVersions.push({
        Id: contHvId,
        HierarchyId: contHierarchyId,
        VersionNumber: 'V1',
        EffectiveFrom: effectiveFrom,
        EffectiveTo: new Date('2099-01-01'),
        Status: sourceHv.Status
      });

      // Copy hierarchy participants (same brokers, same schedules)
      const sourceHps = output.hierarchyParticipants.filter(hp => hp.HierarchyVersionId === sourceHv.Id);
      for (const sourceHp of sourceHps) {
        this.hpCounter++;
        output.hierarchyParticipants.push({
          Id: `${contHierarchyId}-L${sourceHp.Level}`,
          HierarchyVersionId: contHvId,
          EntityId: sourceHp.EntityId,
          EntityName: sourceHp.EntityName,
          EntityType: sourceHp.EntityType,
          Level: sourceHp.Level,
          CommissionRate: sourceHp.CommissionRate,
          ScheduleCode: sourceHp.ScheduleCode,
          ScheduleId: sourceHp.ScheduleId
        });
      }

      // Create PSP for continuation
      this.splitParticipantCounter++;
      output.premiumSplitParticipants.push({
        Id: `PSP-${contProposalId}-${sourcePsp.Sequence}`,
        VersionId: contPsvId,
        BrokerId: sourcePsp.BrokerId,
        BrokerName: sourcePsp.BrokerName,
        BrokerNPN: sourcePsp.BrokerNPN,
        BrokerUniquePartyId: sourcePsp.BrokerUniquePartyId,
        SplitPercent: sourcePsp.SplitPercent,
        IsWritingAgent: sourcePsp.IsWritingAgent,
        HierarchyId: contHierarchyId,
        HierarchyName: `Continuation hierarchy for ${contProposalId}`,
        TemplateId: null,
        TemplateName: null,
        Sequence: sourcePsp.Sequence,
        WritingBrokerId: sourcePsp.WritingBrokerId,
        GroupId: sourcePsp.GroupId,
        EffectiveFrom: effectiveFrom,
        EffectiveTo: new Date('2099-01-01'),
        Notes: `Continuation of ${sourcePsp.Id}`
      });

    // Create state rules, hierarchy splits, split distributions for continuation
    // (Copy from source, filtered to continuation products only)
      this.createContinuationStateRulesAndSplits(
        sourceHv.Id,
        contHvId,
      uniqueProducts,
        output
      );
    }

    // Create proposal products for continuation
    for (const product of uniqueProducts) {
      this.proposalProductCounter++;
      output.proposalProducts.push({
        Id: this.proposalProductCounter,  // Must be numeric for database
        ProposalId: contProposalId,
        ProductCode: product,
        ProductName: `${product} Product`,
        CommissionStructure: null,
        ResolvedScheduleId: null
      });
    }

    // Create key mappings for continuation product+plan pairs
    const years = this.getYearRange(effectiveFrom, new Date('2099-01-01'));
    for (const year of years) {
      for (const pair of pairs) {
        output.proposalKeyMappings.push({
          GroupId: sourceProposal.GroupId,
          EffectiveYear: year,
          ProductCode: pair.productCode,
          PlanCode: pair.planCode,
          ProposalId: contProposalId,
          SplitConfigHash: sourceProposal.SplitConfigHash
        });
      }
    }
  }

  /**
   * Copy state rules and hierarchy splits for continuation, filtered to specific products
   */
  private createContinuationStateRulesAndSplits(
    sourceHvId: string,
    contHvId: string,
    products: string[],
    output: StagingOutput
  ): void {
    const productSet = new Set(products);
    
    // Find source state rules
    const sourceStateRules = output.stateRules.filter(sr => sr.HierarchyVersionId === sourceHvId);
    
    for (const sourceSr of sourceStateRules) {
      this.stateRuleCounter++;
      const contSrId = `SR-${contHvId}-${sourceSr.ShortName}`;
      
      output.stateRules.push({
        Id: contSrId,
        HierarchyVersionId: contHvId,
        ShortName: sourceSr.ShortName,
        Name: sourceSr.Name,
        Description: sourceSr.Description,
        Type: sourceSr.Type,
        SortOrder: sourceSr.SortOrder
      });

      // Copy state rule states
      const sourceSrss = output.stateRuleStates.filter(srs => srs.StateRuleId === sourceSr.Id);
      for (const sourceSrs of sourceSrss) {
        this.stateRuleStateCounter++;
        output.stateRuleStates.push({
          Id: `SRS-${contSrId}-${sourceSrs.StateCode}`,
          StateRuleId: contSrId,
          StateCode: sourceSrs.StateCode,
          StateName: sourceSrs.StateName
        });
      }

      // Copy hierarchy splits, FILTERED to continuation products only
      const sourceHss = output.hierarchySplits.filter(
        hs => hs.StateRuleId === sourceSr.Id && productSet.has(hs.ProductCode)
      );
      
      for (const sourceHs of sourceHss) {
        this.hierarchySplitCounter++;
        const contHsId = `HS-${contSrId}-${sourceHs.ProductCode}`;
        
        output.hierarchySplits.push({
          Id: contHsId,
          StateRuleId: contSrId,
          ProductId: sourceHs.ProductId,
          ProductCode: sourceHs.ProductCode,
          ProductName: sourceHs.ProductName,
          SortOrder: sourceHs.SortOrder
        });

        // Copy split distributions - need to map old HP IDs to new HP IDs
        const sourceSds = output.splitDistributions.filter(sd => sd.HierarchySplitId === sourceHs.Id);
        for (const sourceSd of sourceSds) {
          this.splitDistributionCounter++;
          
          // Map the HierarchyParticipantId from source to continuation
          // Source format: H-PROP-26683-1-P20885-1-V1-L1
          // We need to find the corresponding participant in the continuation hierarchy
          const sourceHpId = sourceSd.HierarchyParticipantId;
          const contHpId = sourceHpId.replace(sourceHvId.replace('-V1', ''), contHvId.replace('-V1', ''));
          
          output.splitDistributions.push({
            Id: `SD-${contHsId}-${contHpId}`,
            HierarchySplitId: contHsId,
            HierarchyParticipantId: contHpId,
            ParticipantEntityId: sourceSd.ParticipantEntityId,
            Percentage: sourceSd.Percentage,
            ScheduleId: sourceSd.ScheduleId,
            ScheduleName: sourceSd.ScheduleName
          });
        }
      }
    }
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
    // NEW: Hierarchy IDs are now unique per proposal, eliminating duplicates
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

    // Generate unique hierarchy ID: H-{ProposalId}-{WritingBrokerId}-{seq}
    // Example: H-PROP-0006-1-P10035-1 (proposal PROP-0006-1, writing broker P10035, sequence 1)
    // ProposalId uniqueness guarantees no duplicates
    this.hierarchyCounter++;
    const hierarchyId = `H-${hierarchyData.proposalId}-${hierarchyData.writingBrokerId}-${this.hierarchyCounter}`;

    // No collision detection needed since ProposalId + counter guarantees uniqueness
    this.usedHierarchyIds.add(hierarchyId);
    
    const writingBrokerId = brokerExternalToInternal(hierarchyData.writingBrokerId);
    const writingBrokerName = hierarchyData.tiers[0]?.brokerName || null;

    // Create hierarchy version with ID derived from hierarchy ID
    const versionId = `${hierarchyId}-V1`;

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
      Status: 0  // HierarchyStatus.Active (Active=0, Inactive=1)
    });

    // Create hierarchy version
    output.hierarchyVersions.push({
      Id: versionId,
      HierarchyId: hierarchyId,
      VersionNumber: 'V1',
      EffectiveFrom: effectiveDate,
      EffectiveTo: new Date('2099-01-01'),
      Status: 1  // HierarchyVersionStatus.Active (Draft=0, Active=1)
    });

    // Create hierarchy participants with IDs derived from hierarchy ID
    for (const tier of hierarchyData.tiers) {
      // Resolve numeric schedule ID from schedule code (trim handles whitespace in source)
      const scheduleId = tier.schedule ? this.scheduleIdByExternalId.get(tier.schedule.trim()) || null : null;
      
      // Participant ID: {hierarchyId}-L{level} (e.g., H-P20667-2-50-L1)
      const participantId = `${hierarchyId}-L${tier.level}`;
      
      output.hierarchyParticipants.push({
        Id: participantId,
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

  // NOTE: extractAssignments() method REMOVED
  // Broker assignments are now collected at the certificate level during extractSelectionCriteria()
  // and stored in the brokerAssignments Map (one per broker, most recent effective date)

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
    if (/^G0+$/.test(trimmed)) return true; // G followed by all zeros
    return false;
  }

  seedProposalProductCounter(seed: number): void {
    const numericSeed = Number(seed);
    if (Number.isFinite(numericSeed) && numericSeed > this.proposalProductCounter) {
      this.proposalProductCounter = numericSeed;
    }
  }

  // ==========================================================================
  // Statistics
  // ==========================================================================

  getStats() {
    // Broker-level assignment statistics
    const brokerAssignmentCount = this.brokerAssignments.size;

    return {
      certificates: this.certificates.length,
      selectionCriteria: this.selectionCriteria.length,
      proposals: this.proposals.length,
      phaRecords: this.phaRecords.length,
      uniqueGroups: new Set(this.proposals.map(p => p.groupId)).size,
      uniqueHierarchies: this.hierarchyByHash.size,
      hashCollisions: this.collisionCount,
      // Broker-level assignment statistics (CHANGED from proposal-level)
      brokerAssignments: brokerAssignmentCount
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
    // Load excluded groups first
    let excludedGroupIds: Set<string> = new Set();
    try {
      const excludedResult = await pool.request().query(`
        SELECT GroupId FROM [${schema}].[stg_excluded_groups]
      `);
      for (const row of excludedResult.recordset) {
        excludedGroupIds.add(row.GroupId);
        // Also add without G prefix for matching
        if (row.GroupId.startsWith('G')) {
          excludedGroupIds.add(row.GroupId.substring(1));
        }
      }
      if (options.verbose) {
        console.log(`Loaded ${excludedResult.recordset.length} excluded groups from stg_excluded_groups`);
      }
    } catch (err: any) {
      if (options.verbose) {
        console.log(`  ⚠️ Could not load excluded groups: ${err.message}`);
      }
    }

    if (options.verbose) {
      console.log(`Loading certificates from [${schema}].[input_certificate_info]...`);
      console.log(`  Filter: CertStatus='A', RecStatus='A', CertEffectiveDate IS NOT NULL`);
      if (options.groups && options.groups.length > 0) {
        console.log(`  Groups filter: Only processing ${options.groups.length} groups: ${options.groups.join(', ')}`);
      }
      if (excludedGroupIds.size > 0) {
        console.log(`  Exclusion: ${excludedGroupIds.size} groups will be skipped`);
      }
      console.log(`  Note: Loading ALL rows (each certificate may have multiple split rows)`);
    }
    
    const limitClause = options.limitCertificates 
      ? `TOP ${options.limitCertificates}` 
      : '';
    
    // Build exclusion clause if we have excluded groups
    let exclusionClause = '';
    if (excludedGroupIds.size > 0) {
      const excludedList = Array.from(excludedGroupIds)
        .map(id => `'${id.replace(/'/g, "''")}'`)
        .join(',');
      exclusionClause = `AND LTRIM(RTRIM(ISNULL(ci.GroupId, ''))) NOT IN (${excludedList})
        AND CONCAT('G', LTRIM(RTRIM(ISNULL(ci.GroupId, '')))) NOT IN (${excludedList})`;
    }
    
    // Build groups filter clause if specific groups are requested
    let groupsFilterClause = '';
    if (options.groups && options.groups.length > 0) {
      // Normalize group IDs: strip ALL leading letters (G, AB, etc.), trim whitespace
      // input_certificate_info.GroupId stores numeric-only values
      const normalizedGroups = options.groups.map(g => {
        const trimmed = g.trim();
        return trimmed.replace(/^[A-Za-z]+/, ''); // Strip all leading letters
      });
      const groupsList = normalizedGroups
        .map(id => `'${id.replace(/'/g, "''")}'`)
        .join(',');
      groupsFilterClause = `AND LTRIM(RTRIM(ISNULL(ci.GroupId, ''))) IN (${groupsList})`;
    }
    
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
      WHERE LTRIM(RTRIM(ci.CertStatus)) = 'A'
        AND LTRIM(RTRIM(ci.RecStatus)) = 'A'
        AND ci.CertEffectiveDate IS NOT NULL
        ${exclusionClause}
        ${groupsFilterClause}
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

interface BlobBulkConfig {
  containerUrl: string;
  containerLocation: string;
  sasToken: string;
  containerName: string;
}

function formatDateForCsv(value: Date): string {
  return value.toISOString().replace('T', ' ').replace('Z', '');
}

function escapeCsvValue(value: unknown): string {
  if (value === null || value === undefined) return '';
  // Convert booleans to 1/0 for SQL Server BIT columns
  if (typeof value === 'boolean') return value ? '1' : '0';
  const raw = value instanceof Date ? formatDateForCsv(value) : String(value);
  if (raw.includes('"')) {
    return `"${raw.replace(/"/g, '""')}"`;
  }
  if (/[,\n\r]/.test(raw)) {
    return `"${raw}"`;
  }
  return raw;
}

function writeCsvFile(
  filePath: string,
  headers: string[],
  rows: unknown[][]
): void {
  const lines = [headers.join(',')];
  for (const row of rows) {
    lines.push(row.map(escapeCsvValue).join(','));
  }
  fs.writeFileSync(filePath, lines.join('\n'));
}

function parseBlobEndpoint(connectionString: string): string {
  const parts = connectionString.split(';').reduce((acc, part) => {
    const [key, value] = part.split('=');
    if (key && value) acc[key.trim().toLowerCase()] = value.trim();
    return acc;
  }, {} as Record<string, string>);
  return parts['blobendpoint'] || '';
}

function getBlobBulkConfig(options: BuilderOptions): BlobBulkConfig {
  const blobConfig = options.blobConfig;
  if (blobConfig?.containerUrl) {
    const parsed = new URL(blobConfig.containerUrl);
    const containerLocation = `${parsed.origin}${parsed.pathname}`;
    const sasToken = parsed.search.startsWith('?') ? parsed.search.slice(1) : parsed.search;
    const pathParts = parsed.pathname.split('/').filter(Boolean);
    const containerName = pathParts[pathParts.length - 1] || '';
    if (!containerName) {
      throw new Error('Blob containerUrl must include the container path');
    }
    return {
      containerUrl: blobConfig.containerUrl,
      containerLocation,
      sasToken,
      containerName
    };
  }

  if (blobConfig?.endpoint && blobConfig.container && blobConfig.token) {
    const blobEndpoint = parseBlobEndpoint(blobConfig.endpoint);
    const sasToken = blobConfig.token.replace(/^\?/, '');
    const containerLocation = `${blobEndpoint}${blobConfig.container}`;
    const containerUrl = `${containerLocation}?${sasToken}`;
    return {
      containerUrl,
      containerLocation,
      sasToken,
      containerName: blobConfig.container
    };
  }

  const containerUrlEnv = process.env.BLOB_CONTAINER_URL;
  if (containerUrlEnv) {
    const parsed = new URL(containerUrlEnv);
    const containerLocation = `${parsed.origin}${parsed.pathname}`;
    const sasToken = parsed.search.startsWith('?') ? parsed.search.slice(1) : parsed.search;
    const pathParts = parsed.pathname.split('/').filter(Boolean);
    const containerName = pathParts[pathParts.length - 1] || '';
    if (!containerName) {
      throw new Error('BLOB_CONTAINER_URL must include the container path');
    }
    return { containerUrl: containerUrlEnv, containerLocation, sasToken, containerName };
  }

  const endpoint = process.env.BLOB_ENDPOINT || '';
  const containerName = process.env.BLOB_CONTAINER || process.env.BLOB_CONTAINER_NAME || '';
  const sasToken = (process.env.BLOB_TOKEN || '').replace(/^\?/, '');

  const blobEndpoint = endpoint ? parseBlobEndpoint(endpoint) : '';
  if (!blobEndpoint || !containerName || !sasToken) {
    throw new Error('Missing blob config. Set BLOB_CONTAINER_URL or BLOB_ENDPOINT + BLOB_CONTAINER + BLOB_TOKEN.');
  }

  const containerLocation = `${blobEndpoint}${containerName}`;
  const containerUrl = `${containerLocation}?${sasToken}`;
  return { containerUrl, containerLocation, sasToken, containerName };
}

async function ensureBlobExternalDataSource(
  pool: sql.ConnectionPool,
  containerLocation: string,
  sasToken: string
): Promise<void> {
  const safeSas = sasToken.replace(/'/g, "''");
  const safeLocation = containerLocation.replace(/'/g, "''");

  console.log(`  Setting up External Data Source: ${containerLocation}`);
  
  // Update or create the credential
  await pool.request().query(`
    IF EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'BlobSasCred')
    BEGIN
      EXEC('ALTER DATABASE SCOPED CREDENTIAL [BlobSasCred] WITH IDENTITY = ''SHARED ACCESS SIGNATURE'', SECRET = ''${safeSas}''');
    END
    ELSE
    BEGIN
      EXEC('CREATE DATABASE SCOPED CREDENTIAL [BlobSasCred] WITH IDENTITY = ''SHARED ACCESS SIGNATURE'', SECRET = ''${safeSas}''');
    END
  `);

  // Drop and recreate the external data source to ensure correct location
  await pool.request().query(`
    IF EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'BlobStaging')
    BEGIN
      DROP EXTERNAL DATA SOURCE [BlobStaging];
    END
  `);
  
  await pool.request().query(`
    CREATE EXTERNAL DATA SOURCE [BlobStaging] 
    WITH (TYPE = BLOB_STORAGE, LOCATION = '${safeLocation}', CREDENTIAL = [BlobSasCred])
  `);
  
  console.log(`    ✓ External Data Source ready`);
}

async function loadTableColumns(
  pool: sql.ConnectionPool,
  schema: string,
  tableName: string
): Promise<string[]> {
  const result = await pool.request().query(`
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '${schema}'
      AND TABLE_NAME = '${tableName}'
    ORDER BY ORDINAL_POSITION
  `);
  return result.recordset.map((row: any) => row.COLUMN_NAME);
}

function getColumnValue(
  tableName: string,
  row: any,
  columnName: string,
  creationTime: Date
): unknown {
  switch (tableName) {
    case 'stg_proposals':
      return ({
        Id: row.Id,
        ProposalNumber: row.ProposalNumber,
        Status: row.Status,
        SubmittedDate: row.SubmittedDate,
        ProposedEffectiveDate: row.ProposedEffectiveDate,
        SitusState: row.SitusState,
        GroupId: row.GroupId,
        GroupName: row.GroupName,
        BrokerId: row.BrokerId,
        BrokerName: row.BrokerName,
        BrokerUniquePartyId: row.BrokerUniquePartyId,
        ProductCodes: row.ProductCodes,
        PlanCodes: row.PlanCodes,
        SplitConfigHash: row.SplitConfigHash,
        DateRangeFrom: row.DateRangeFrom,
        DateRangeTo: row.DateRangeTo,
        EffectiveDateFrom: row.EffectiveDateFrom,
        EffectiveDateTo: row.EffectiveDateTo,
        Notes: row.Notes,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_proposal_products':
      return ({
        Id: row.Id,
        ProposalId: row.ProposalId,
        ProductCode: row.ProductCode,
        ProductName: row.ProductName,
        CommissionStructure: row.CommissionStructure,
        ResolvedScheduleId: row.ResolvedScheduleId,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_proposal_key_mapping':
      return ({
        GroupId: row.GroupId,
        EffectiveYear: row.EffectiveYear,
        ProductCode: row.ProductCode,
        PlanCode: row.PlanCode,
        ProposalId: row.ProposalId,
        SplitConfigHash: row.SplitConfigHash,
        CreationTime: creationTime
      } as Record<string, unknown>)[columnName];
    case 'stg_premium_split_versions':
      return ({
        Id: row.Id,
        GroupId: row.GroupId,
        GroupName: row.GroupName,
        ProposalId: row.ProposalId,
        ProposalNumber: row.ProposalNumber,
        VersionNumber: row.VersionNumber,
        EffectiveFrom: row.EffectiveFrom,
        EffectiveTo: row.EffectiveTo,
        TotalSplitPercent: row.TotalSplitPercent,
        Status: row.Status,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_premium_split_participants':
      return ({
        Id: row.Id,
        VersionId: row.VersionId,
        BrokerId: row.BrokerId,
        BrokerName: row.BrokerName,
        BrokerNPN: row.BrokerNPN,
        BrokerUniquePartyId: row.BrokerUniquePartyId,
        SplitPercent: row.SplitPercent,
        IsWritingAgent: row.IsWritingAgent,
        HierarchyId: row.HierarchyId,
        HierarchyName: row.HierarchyName,
        TemplateId: row.TemplateId,
        TemplateName: row.TemplateName,
        Sequence: row.Sequence,
        WritingBrokerId: row.WritingBrokerId,
        GroupId: row.GroupId,
        EffectiveFrom: row.EffectiveFrom,
        EffectiveTo: row.EffectiveTo,
        Notes: row.Notes,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_hierarchies':
      return ({
        Id: row.Id,
        Name: row.Name,
        GroupId: row.GroupId,
        GroupName: row.GroupName,
        BrokerId: row.BrokerId,
        BrokerName: row.BrokerName,
        ProposalId: row.ProposalId,
        CurrentVersionId: row.CurrentVersionId,
        EffectiveDate: row.EffectiveDate,
        SitusState: row.SitusState,
        Status: row.Status,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_hierarchy_versions':
      return ({
        Id: row.Id,
        HierarchyId: row.HierarchyId,
        VersionNumber: row.VersionNumber,
        EffectiveFrom: row.EffectiveFrom,
        EffectiveTo: row.EffectiveTo,
        Status: row.Status,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_hierarchy_participants':
      return ({
        Id: row.Id,
        HierarchyVersionId: row.HierarchyVersionId,
        EntityId: brokerExternalToInternal(row.EntityId),  // Convert P12345 to 12345
        EntityName: row.EntityName,
        Level: row.Level,
        SortOrder: row.Level,  // Use Level as SortOrder
        SplitPercent: 0,
        CommissionRate: row.CommissionRate,
        ScheduleCode: row.ScheduleCode,
        ScheduleId: row.ScheduleId,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_state_rules':
      return ({
        Id: row.Id,
        HierarchyVersionId: row.HierarchyVersionId,
        ShortName: row.ShortName,
        Name: row.Name,
        Description: row.Description,
        Type: row.Type,
        SortOrder: row.SortOrder,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_state_rule_states':
      return ({
        Id: row.Id,
        StateRuleId: row.StateRuleId,
        StateCode: row.StateCode,
        StateName: row.StateName,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_hierarchy_splits':
      return ({
        Id: row.Id,
        StateRuleId: row.StateRuleId,
        ProductId: row.ProductId,
        ProductCode: row.ProductCode,
        ProductName: row.ProductName,
        ScheduleCode: row.ScheduleCode,
        ScheduleId: row.ScheduleId,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_split_distributions':
      return ({
        Id: row.Id,
        HierarchySplitId: row.HierarchySplitId,
        HierarchyParticipantId: row.HierarchyParticipantId,
        ParticipantEntityId: row.ParticipantEntityId,
        Percentage: row.Percentage,
        ScheduleId: row.ScheduleId,
        ScheduleName: row.ScheduleName,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_policy_hierarchy_assignments':
      return ({
        Id: row.Id,
        PolicyId: row.PolicyId,
        HierarchyId: row.HierarchyId,
        WritingBrokerId: row.WritingBrokerId,
        SplitSequence: row.SplitSequence,
        SplitPercent: row.SplitPercent,
        NonConformantReason: row.NonConformantReason,
        EntryType: row.EntryType ?? 0,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_policy_hierarchy_participants':
      return ({
        Id: row.Id,
        PolicyHierarchyAssignmentId: row.PolicyHierarchyAssignmentId,
        BrokerId: brokerExternalToInternal(row.BrokerId),  // Convert P12345 to 12345
        BrokerName: row.BrokerName,
        Level: row.Level,
        ScheduleCode: row.ScheduleCode,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_commission_assignment_versions':
      return ({
        Id: row.Id,
        BrokerId: row.BrokerId,  // Already numeric from earlier code
        BrokerName: row.BrokerName,
        ProposalId: row.ProposalId,
        GroupId: row.GroupId,
        HierarchyId: row.HierarchyId,
        HierarchyVersionId: row.HierarchyVersionId,
        HierarchyParticipantId: row.HierarchyParticipantId,
        VersionNumber: row.VersionNumber,
        EffectiveFrom: row.EffectiveFrom,
        EffectiveTo: row.EffectiveTo,
        Status: row.Status,
        Type: row.Type,
        ChangeDescription: row.ChangeDescription,
        TotalAssignedPercent: row.TotalAssignedPercent,
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    case 'stg_commission_assignment_recipients':
      // Map code property names to actual schema column names
      return ({
        Id: row.Id,
        AssignmentVersionId: row.VersionId,  // Schema column name differs from code
        RecipientBrokerId: row.RecipientBrokerId,  // Already numeric from earlier code
        RecipientBrokerName: row.RecipientName,  // Schema column name differs from code
        Percent: row.Percentage,  // Schema column name differs from code
        RecipientType: 1,  // Default to Broker type
        CreationTime: creationTime,
        IsDeleted: 0
      } as Record<string, unknown>)[columnName];
    default:
      return null;
  }
}

async function writeStagingOutputToBlob(
  config: DatabaseConfig,
  output: StagingOutput,
  options: BuilderOptions
): Promise<void> {
  const schema = options.schema || 'etl';
  const creationTime = new Date();
  const runId = options.bulkPrefix || `etl-bulk-${Date.now()}`;
  const { containerUrl, containerLocation, sasToken, containerName } = getBlobBulkConfig(options);

  const tempDir = path.join(os.tmpdir(), runId);
  fs.mkdirSync(tempDir, { recursive: true });

  const pool = await sql.connect(config);
  // Use ContainerClient directly with full containerUrl (which includes container name + SAS token)
  const storageAccountUrl = `https://${new URL(containerUrl).hostname}`;
  const fullBlobUrl = `${storageAccountUrl}/${containerName}?${sasToken}`;
  const blobClient = new ContainerClient(fullBlobUrl);

  try {
    await ensureBlobExternalDataSource(pool, containerLocation, sasToken);
    await clearStagingData(pool, schema, output, options);

    const tables: Array<{ name: string; rows: any[] }> = [
      { name: 'stg_proposals', rows: output.proposals },
      { name: 'stg_proposal_products', rows: output.proposalProducts },
      { name: 'stg_proposal_key_mapping', rows: output.proposalKeyMappings },
      { name: 'stg_premium_split_versions', rows: output.premiumSplitVersions },
      { name: 'stg_premium_split_participants', rows: output.premiumSplitParticipants },
      { name: 'stg_hierarchies', rows: output.hierarchies },
      { name: 'stg_hierarchy_versions', rows: output.hierarchyVersions },
      { name: 'stg_hierarchy_participants', rows: output.hierarchyParticipants },
      { name: 'stg_state_rules', rows: output.stateRules },
      { name: 'stg_state_rule_states', rows: output.stateRuleStates },
      { name: 'stg_hierarchy_splits', rows: output.hierarchySplits },
      { name: 'stg_split_distributions', rows: output.splitDistributions },
      { name: 'stg_policy_hierarchy_assignments', rows: output.policyHierarchyAssignments },
      { name: 'stg_policy_hierarchy_participants', rows: output.policyHierarchyParticipants },
      { name: 'stg_commission_assignment_versions', rows: output.commissionAssignmentVersions },
      { name: 'stg_commission_assignment_recipients', rows: output.commissionAssignmentRecipients }
    ];

    for (const table of tables) {
      if (table.rows.length === 0) continue;
      const columns = await loadTableColumns(pool, schema, table.name);
      const csvRows = table.rows.map(row =>
        columns.map(column => getColumnValue(table.name, row, column, creationTime))
      );
      const fileName = `${table.name}.csv`;
      const localPath = path.join(tempDir, fileName);
      writeCsvFile(localPath, columns, csvRows);

      const blobPath = `${runId}/${fileName}`;
      const blob = blobClient.getBlockBlobClient(blobPath);
      console.log(`  Uploading ${fileName} to blob: ${blobPath}...`);
      await blob.uploadFile(localPath);
      console.log(`    ✓ Upload complete for ${fileName}`);

      console.log(`  Running BULK INSERT for ${table.name}...`);
      await pool.request().query(`
        BULK INSERT [${schema}].[${table.name}]
        FROM '${blobPath}'
        WITH (
          DATA_SOURCE = 'BlobStaging',
          FORMAT = 'CSV',
          FIRSTROW = 2,
          FIELDTERMINATOR = ',',
          ROWTERMINATOR = '0x0A',
          KEEPNULLS,
          CODEPAGE = '65001'
        )
      `);
      console.log(`    ✓ Bulk insert complete for ${table.name}`);
    }
  } finally {
    await pool.close();
  }
}

async function clearStagingData(
  pool: sql.ConnectionPool,
  schema: string,
  output: StagingOutput,
  options: BuilderOptions
): Promise<void> {
  if (options.groups && options.groups.length > 0) {
    const groupsWithPrefix = options.groups.map(g => {
      const trimmed = g.trim();
      return /^[A-Za-z]/.test(trimmed) ? trimmed : `G${trimmed}`;
    });
    const groupsWithNumericPrefix = options.groups.map(g => {
      const trimmed = g.trim();
      const numericPart = trimmed.replace(/^[A-Za-z]+/, '');
      return `G${numericPart}`;
    });
    const groupsListString = Array.from(new Set([...groupsWithPrefix, ...groupsWithNumericPrefix]))
      .map(g => `'${g}'`)
      .join(',');
    
    console.log(`  SELECTIVE STAGING CLEAR for groups: ${groupsWithPrefix.join(', ')}`);
    
    await pool.request().query(`
      DELETE car FROM [${schema}].[stg_commission_assignment_recipients] car
      INNER JOIN [${schema}].[stg_commission_assignment_versions] cav ON cav.Id = car.AssignmentVersionId
      INNER JOIN [${schema}].[stg_proposals] p ON p.Id = cav.ProposalId
      WHERE p.GroupId IN (${groupsListString})
    `);
    await pool.request().query(`
      DELETE cav FROM [${schema}].[stg_commission_assignment_versions] cav
      INNER JOIN [${schema}].[stg_proposals] p ON p.Id = cav.ProposalId
      WHERE p.GroupId IN (${groupsListString})
    `);
    await pool.request().query(`
      DELETE php FROM [${schema}].[stg_policy_hierarchy_participants] php
      INNER JOIN [${schema}].[stg_policy_hierarchy_assignments] pha ON pha.Id = php.PolicyHierarchyAssignmentId
      INNER JOIN [${schema}].[stg_hierarchies] h ON h.Id = pha.HierarchyId
      WHERE h.GroupId IN (${groupsListString})
    `);
    await pool.request().query(`
      DELETE pha FROM [${schema}].[stg_policy_hierarchy_assignments] pha
      INNER JOIN [${schema}].[stg_hierarchies] h ON h.Id = pha.HierarchyId
      WHERE h.GroupId IN (${groupsListString})
    `);
    await pool.request().query(`
      DELETE sd FROM [${schema}].[stg_split_distributions] sd
      INNER JOIN [${schema}].[stg_hierarchy_splits] hs ON hs.Id = sd.HierarchySplitId
      INNER JOIN [${schema}].[stg_state_rules] sr ON sr.Id = hs.StateRuleId
      INNER JOIN [${schema}].[stg_hierarchy_versions] hv ON hv.Id = sr.HierarchyVersionId
      INNER JOIN [${schema}].[stg_hierarchies] h ON h.Id = hv.HierarchyId
      WHERE h.GroupId IN (${groupsListString})
    `);
    await pool.request().query(`
      DELETE hs FROM [${schema}].[stg_hierarchy_splits] hs
      INNER JOIN [${schema}].[stg_state_rules] sr ON sr.Id = hs.StateRuleId
      INNER JOIN [${schema}].[stg_hierarchy_versions] hv ON hv.Id = sr.HierarchyVersionId
      INNER JOIN [${schema}].[stg_hierarchies] h ON h.Id = hv.HierarchyId
      WHERE h.GroupId IN (${groupsListString})
    `);
    await pool.request().query(`
      DELETE srs FROM [${schema}].[stg_state_rule_states] srs
      INNER JOIN [${schema}].[stg_state_rules] sr ON sr.Id = srs.StateRuleId
      INNER JOIN [${schema}].[stg_hierarchy_versions] hv ON hv.Id = sr.HierarchyVersionId
      INNER JOIN [${schema}].[stg_hierarchies] h ON h.Id = hv.HierarchyId
      WHERE h.GroupId IN (${groupsListString})
    `);
    await pool.request().query(`
      DELETE sr FROM [${schema}].[stg_state_rules] sr
      INNER JOIN [${schema}].[stg_hierarchy_versions] hv ON hv.Id = sr.HierarchyVersionId
      INNER JOIN [${schema}].[stg_hierarchies] h ON h.Id = hv.HierarchyId
      WHERE h.GroupId IN (${groupsListString})
    `);
    await pool.request().query(`
      DELETE hp FROM [${schema}].[stg_hierarchy_participants] hp
      INNER JOIN [${schema}].[stg_hierarchy_versions] hv ON hv.Id = hp.HierarchyVersionId
      INNER JOIN [${schema}].[stg_hierarchies] h ON h.Id = hv.HierarchyId
      WHERE h.GroupId IN (${groupsListString})
    `);
    await pool.request().query(`
      DELETE hv FROM [${schema}].[stg_hierarchy_versions] hv
      INNER JOIN [${schema}].[stg_hierarchies] h ON h.Id = hv.HierarchyId
      WHERE h.GroupId IN (${groupsListString})
    `);
    await pool.request().query(`
      DELETE psp FROM [${schema}].[stg_premium_split_participants] psp
      INNER JOIN [${schema}].[stg_premium_split_versions] psv ON psv.Id = psp.VersionId
      INNER JOIN [${schema}].[stg_proposals] p ON p.Id = psv.ProposalId
      WHERE p.GroupId IN (${groupsListString})
    `);
    await pool.request().query(`
      DELETE psv FROM [${schema}].[stg_premium_split_versions] psv
      INNER JOIN [${schema}].[stg_proposals] p ON p.Id = psv.ProposalId
      WHERE p.GroupId IN (${groupsListString})
    `);
    await pool.request().query(`DELETE FROM [${schema}].[stg_hierarchies] WHERE GroupId IN (${groupsListString})`);
    await pool.request().query(`
      DELETE pp FROM [${schema}].[stg_proposal_products] pp
      INNER JOIN [${schema}].[stg_proposals] p ON p.Id = pp.ProposalId
      WHERE p.GroupId IN (${groupsListString})
    `);
    await pool.request().query(`DELETE FROM [${schema}].[stg_proposal_key_mapping] WHERE GroupId IN (${groupsListString})`);
    await pool.request().query(`DELETE FROM [${schema}].[stg_proposals] WHERE GroupId IN (${groupsListString})`);
    
    console.log(`  ✅ Cleared staging data for groups: ${groupsWithPrefix.join(', ')}`);

    if (output.commissionAssignmentVersions.length > 0) {
      const cavIds = output.commissionAssignmentVersions.map(c => `'${String(c.Id).replace(/'/g, "''")}'`);
      const batchSize = 500;
      for (let i = 0; i < cavIds.length; i += batchSize) {
        const batch = cavIds.slice(i, i + batchSize).join(',');
        await pool.request().query(`
          DELETE FROM [${schema}].[stg_commission_assignment_recipients]
          WHERE AssignmentVersionId IN (${batch})
        `);
        await pool.request().query(`
          DELETE FROM [${schema}].[stg_commission_assignment_versions]
          WHERE Id IN (${batch})
        `);
      }
    }
  } else {
    console.log('  FULL STAGING CLEAR (truncating all staging tables)');
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
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_commission_assignment_versions]`);
    await pool.request().query(`TRUNCATE TABLE [${schema}].[stg_commission_assignment_recipients]`);
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

  if (options.bulkMode === 'blob') {
    await writeStagingOutputToBlob(config, output, options);
    return;
  }

  const pool = await sql.connect(config);

  try {
    if (options.verbose) {
      console.log('Writing staging output to database...');
    }
    const startTime = Date.now();

    await clearStagingData(pool, schema, output, options);
    // NOTE: We intentionally do NOT delete from production (dbo) here.
    // Export to production is handled by separate export scripts.

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

    // Insert PHA assignments (batched - now includes HierarchyId for referential integrity)
    // 8 params per row * 250 rows = 2000 params (under 2100 limit)
    const phaBatchSize = 250;
    const totalPhaBatches = Math.ceil(output.policyHierarchyAssignments.length / phaBatchSize);
    
    for (let i = 0; i < output.policyHierarchyAssignments.length; i += phaBatchSize) {
      const batch = output.policyHierarchyAssignments.slice(i, i + phaBatchSize);
      const batchNum = Math.floor(i / phaBatchSize) + 1;
      
      if (options.verbose && (batchNum % 5 === 0 || batchNum === 1)) {
        const pct = Math.floor((batchNum / totalPhaBatches) * 100);
        console.log(`    Writing PHA assignments batch ${batchNum}/${totalPhaBatches} (${pct}%)...`);
      }
      
      const values = batch.map((pha, idx) => {
        return `(@Id${idx}, @PolicyId${idx}, @HierarchyId${idx}, @WritingBrokerId${idx}, @SplitSequence${idx}, @SplitPercent${idx}, @NonConformantReason${idx}, @EntryType${idx}, GETUTCDATE(), 0)`;
      }).join(',');
      
      const request = pool.request();
      batch.forEach((pha, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(100), pha.Id);
        request.input(`PolicyId${idx}`, sql.NVarChar(100), pha.PolicyId);
        request.input(`HierarchyId${idx}`, sql.NVarChar(100), pha.HierarchyId);
        request.input(`WritingBrokerId${idx}`, sql.BigInt, pha.WritingBrokerId);
        request.input(`SplitSequence${idx}`, sql.Int, pha.SplitSequence);
        request.input(`SplitPercent${idx}`, sql.Decimal(18, 4), pha.SplitPercent);
        request.input(`NonConformantReason${idx}`, sql.NVarChar(500), pha.NonConformantReason);
        request.input(`EntryType${idx}`, sql.TinyInt, pha.EntryType ?? 0);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_policy_hierarchy_assignments] (
          Id, PolicyId, HierarchyId, WritingBrokerId, SplitSequence, SplitPercent, NonConformantReason, EntryType,
          CreationTime, IsDeleted
        ) VALUES ${values}
      `);
    }
    if (options.verbose) {
      console.log(`  Wrote ${output.policyHierarchyAssignments.length} PHA assignments with HierarchyId`);
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

    // Insert CommissionAssignmentVersions to STAGING (batched - 15 params per row, max ~130 rows)
    const cavBatchSize = 130;
    for (let i = 0; i < output.commissionAssignmentVersions.length; i += cavBatchSize) {
      const batch = output.commissionAssignmentVersions.slice(i, i + cavBatchSize);
      const values = batch.map((cav, idx) =>
        `(@Id${idx}, @BrokerId${idx}, @BrokerName${idx}, @ProposalId${idx}, NULL, @HierarchyId${idx}, @HierarchyVersionId${idx}, @HierarchyParticipantId${idx}, @VersionNumber${idx}, @EffectiveFrom${idx}, @EffectiveTo${idx}, @Status${idx}, @Type${idx}, @ChangeDescription${idx}, @TotalAssignedPercent${idx}, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((cav, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(100), cav.Id);
        request.input(`BrokerId${idx}`, sql.BigInt, cav.BrokerId);
        request.input(`BrokerName${idx}`, sql.NVarChar(510), cav.BrokerName);
        request.input(`ProposalId${idx}`, sql.NVarChar(100), cav.ProposalId);
        // GroupId is NULL for broker-level assignments
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
        INSERT INTO [${schema}].[stg_commission_assignment_versions] (
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

    // Insert CommissionAssignmentRecipients to STAGING (batched)
    // NOTE: Staging table has different columns than production:
    // - AssignmentVersionId (not VersionId)
    // - RecipientBrokerName (not RecipientName)
    // - Percent (not Percentage)
    // - No RecipientNPN, RecipientHierarchyId, Notes columns
    const carBatchSize = 250;
    for (let i = 0; i < output.commissionAssignmentRecipients.length; i += carBatchSize) {
      const batch = output.commissionAssignmentRecipients.slice(i, i + carBatchSize);
      const values = batch.map((car, idx) =>
        `(@Id${idx}, @AssignmentVersionId${idx}, @RecipientBrokerId${idx}, @RecipientBrokerName${idx}, @Percent${idx}, 1, GETUTCDATE(), 0)`
      ).join(',');
      
      const request = pool.request();
      batch.forEach((car, idx) => {
        request.input(`Id${idx}`, sql.NVarChar(200), car.Id);
        request.input(`AssignmentVersionId${idx}`, sql.NVarChar(100), car.VersionId);
        request.input(`RecipientBrokerId${idx}`, sql.BigInt, car.RecipientBrokerId);
        request.input(`RecipientBrokerName${idx}`, sql.NVarChar(510), car.RecipientName);
        request.input(`Percent${idx}`, sql.Decimal(5, 2), car.Percentage);
      });
      
      await request.query(`
        INSERT INTO [${schema}].[stg_commission_assignment_recipients] (
          Id, AssignmentVersionId, RecipientBrokerId, RecipientBrokerName,
          [Percent], RecipientType, CreationTime, IsDeleted
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
    // Uses reference schema (default: dbo) for Schedules table
    const schedulePool = await sql.connect(config);
    try {
      await builder.loadSchedules(schedulePool, options.referenceSchema || 'dbo');
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
    console.log(`Broker-level assignments: ${stats.brokerAssignments}`);
    
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
// Export Staging to Production
// =============================================================================

/**
 * Execute a SQL file with SQLCMD variable replacement
 * Reuses pattern from export-all-destructive.ts
 */
async function executeSqlFile(
  pool: sql.ConnectionPool, 
  filePath: string,
  etlSchema: string,
  productionSchema: string,
  verbose: boolean = false
): Promise<{ rowsAffected: number }> {
  const fileName = path.basename(filePath);
  console.log(`\n  Executing ${fileName}...`);
  
  let sqlContent = fs.readFileSync(filePath, 'utf8');
  
  // Replace SQLCMD variables
  sqlContent = sqlContent.replace(/\$\(PRODUCTION_SCHEMA\)/g, productionSchema);
  sqlContent = sqlContent.replace(/\$\(ETL_SCHEMA\)/g, etlSchema);
  
  // Split on GO statements (case-insensitive, on its own line)
  const batches = sqlContent.split(/^\s*GO\s*$/im).filter(b => b.trim().length > 0);
  
  if (verbose) {
    console.log(`    ${batches.length} batch(es) found`);
  }
  
  let totalRowsAffected = 0;
  
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i].trim();
    if (batch.length === 0) continue;
    
    try {
      const result = await pool.request().query(batch);
      if (result.rowsAffected && result.rowsAffected.length > 0) {
        const batchRows = result.rowsAffected.reduce((a, b) => a + b, 0);
        totalRowsAffected += batchRows;
        if (batchRows > 0 && verbose) {
          console.log(`    Batch ${i + 1}: ${batchRows} rows affected`);
        }
      }
    } catch (err: any) {
      console.error(`    ❌ Error in batch ${i + 1}: ${err.message}`);
      throw err;
    }
  }
  
  console.log(`  ✅ ${fileName}: ${totalRowsAffected} rows affected`);
  return { rowsAffected: totalRowsAffected };
}

/**
 * Export staging tables to production (dbo) schema
 * Executes SQL export files in FK dependency order
 */
export async function exportStagingToProduction(
  config: DatabaseConfig,
  options: BuilderOptions
): Promise<void> {
  const etlSchema = options.schema || 'etl';
  const productionSchema = options.productionSchema || 'dbo';
  const verbose = options.verbose || false;
  const dryRun = options.dryRun || false;
  
  console.log('='.repeat(70));
  console.log('EXPORT: Staging to Production');
  console.log('='.repeat(70));
  console.log(`  ETL Schema: ${etlSchema}`);
  console.log(`  Production Schema: ${productionSchema}`);
  console.log(`  Dry Run: ${dryRun}`);
  if (options.groups && options.groups.length > 0) {
    console.log(`  Selective Export: ${options.groups.length} groups (${options.groups.join(', ')})`);
  } else {
    console.log(`  Selective Export: No (full export)`);
  }
  console.log('');
  
  if (dryRun) {
    console.log('DRY RUN: This export would:');
    
    if (options.groups && options.groups.length > 0) {
      const normalizedGroups = options.groups.map(g => {
        const trimmed = g.trim();
        // If already has letter prefix, keep it; otherwise add 'G'
        return /^[A-Za-z]/.test(trimmed) ? trimmed : `G${trimmed}`;
      });
      console.log(`\n  SELECTIVE EXPORT for groups: ${normalizedGroups.join(', ')}`);
      console.log('\n  STEP 1 - DELETE existing production data for THESE GROUPS ONLY:');
    } else {
      console.log('\n  FULL EXPORT (all data)');
      console.log('\n  STEP 1 - DELETE ALL existing production data from (DESTRUCTIVE):');
    }
    
    const tables = [
      'CommissionAssignmentRecipients', 'CommissionAssignmentVersions',
      'PolicyHierarchyAssignments', 'SplitDistributions', 'HierarchySplits',
      'StateRuleStates', 'StateRules', 'HierarchyParticipants', 'HierarchyVersions',
      'PremiumSplitParticipants', 'PremiumSplitVersions', 'Hierarchies',
      'ProposalProducts', 'Proposals'
    ];
    for (const table of tables) {
      console.log(`    - ${productionSchema}.${table}`);
    }
    console.log('\n  NOTE: Brokers are NOT deleted (shared across system, additive only)');
    console.log('\n  STEP 2 - INSERT staging data using:');
    const scripts = [
      'sql/export/02-export-brokers.sql (ADDITIVE - missing brokers only)',
      'sql/export/07-export-proposals.sql',
      'sql/export/08-export-hierarchies.sql',
      'sql/export/11-export-splits.sql',
      'sql/export/14-export-policy-hierarchy-assignments.sql',
      'sql/export/13-export-commission-assignments.sql',
    ];
    for (const script of scripts) {
      console.log(`    - ${script}`);
    }
    console.log('\n✅ Dry run complete (no changes made)');
    return;
  }
  
  const pool = await sql.connect({
    ...config,
    requestTimeout: 600000, // 10 minutes for large exports
  });
  
  try {
    // Ensure stg_excluded_groups table exists (referenced by export scripts)
    console.log('  Ensuring stg_excluded_groups table exists...');
    await pool.request().query(`
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('${etlSchema}') AND name = 'stg_excluded_groups')
      BEGIN
        CREATE TABLE [${etlSchema}].[stg_excluded_groups] (
          GroupId NVARCHAR(50) NOT NULL PRIMARY KEY,
          Reason NVARCHAR(500) NULL,
          CreatedAt DATETIME2 DEFAULT GETUTCDATE()
        );
        PRINT 'Created stg_excluded_groups table';
      END
    `);
    console.log('  ✅ stg_excluded_groups table ready');
    
    // ==========================================================================
    // STEP 1: DELETE existing production data (FK order - children first)
    // ==========================================================================
    console.log('\n' + '='.repeat(70));
    if (options.groups && options.groups.length > 0) {
      console.log('STEP 1: Deleting production data for SELECTED GROUPS (FK order)');
    } else {
      console.log('STEP 1: Deleting ALL production data (FK order - children first)');
    }
    console.log('='.repeat(70));
    
    let totalDeleted = 0;
    
    // Check if selective delete (by groups)
    if (options.groups && options.groups.length > 0) {
      // SELECTIVE DELETE: Only delete data for specified groups
      // Create TWO group lists:
      // 1. String format with 'G' prefix for nvarchar columns (Proposals, Hierarchies)
      // 2. Numeric format (all letters stripped) for bigint columns (PremiumSplitVersions)
      const groupsWithPrefix = options.groups.map(g => {
        const trimmed = g.trim();
        // If it already has a letter prefix, keep it; otherwise add 'G'
        return /^[A-Za-z]/.test(trimmed) ? trimmed : `G${trimmed}`;
      });
      const groupsNumeric = options.groups.map(g => {
        const trimmed = g.trim();
        // Strip ALL leading letters to get numeric part (handles G, AB, etc.)
        const numericPart = trimmed.replace(/^[A-Za-z]+/, '');
        return numericPart;
      });
      
      // For nvarchar columns (Proposals.GroupId, Hierarchies.GroupId)
      const groupsListString = groupsWithPrefix.map(g => `'${g}'`).join(',');
      // For bigint columns (PremiumSplitVersions.GroupId) - numeric only, no quotes
      const groupsListNumeric = groupsNumeric.join(',');
      
      console.log(`\n  Deleting data for groups: ${groupsWithPrefix.join(', ')}`);
      
      // Delete in FK dependency order, filtering by group
      // Each query deletes records that belong to the specified groups
      // NOTE: Use groupsListString for nvarchar columns, groupsListNumeric for bigint columns
      const selectiveDeleteQueries = [
        // 1. CommissionAssignmentRecipients - via CommissionAssignmentVersions -> Proposals
        {
          name: 'CommissionAssignmentRecipients',
          query: `DELETE car FROM [${productionSchema}].[CommissionAssignmentRecipients] car
                  INNER JOIN [${productionSchema}].[CommissionAssignmentVersions] cav ON cav.Id = car.VersionId
                  INNER JOIN [${productionSchema}].[Proposals] p ON p.Id = cav.ProposalId
                  WHERE p.GroupId IN (${groupsListString})`
        },
        // 2. CommissionAssignmentVersions - via Proposals
        {
          name: 'CommissionAssignmentVersions',
          query: `DELETE cav FROM [${productionSchema}].[CommissionAssignmentVersions] cav
                  INNER JOIN [${productionSchema}].[Proposals] p ON p.Id = cav.ProposalId
                  WHERE p.GroupId IN (${groupsListString})`
        },
        // 3. PolicyHierarchyAssignments - via Hierarchies (Hierarchies.GroupId is nvarchar)
        {
          name: 'PolicyHierarchyAssignments (via Hierarchies)',
          query: `DELETE pha FROM [${productionSchema}].[PolicyHierarchyAssignments] pha
                  INNER JOIN [${productionSchema}].[Hierarchies] h ON h.Id = pha.HierarchyId
                  WHERE h.GroupId IN (${groupsListString})`
        },
        // 4. SplitDistributions - via HierarchySplits -> StateRules -> HierarchyVersions -> Hierarchies
        {
          name: 'SplitDistributions',
          query: `DELETE sd FROM [${productionSchema}].[SplitDistributions] sd
                  INNER JOIN [${productionSchema}].[HierarchySplits] hs ON hs.Id = sd.HierarchySplitId
                  INNER JOIN [${productionSchema}].[StateRules] sr ON sr.Id = hs.StateRuleId
                  INNER JOIN [${productionSchema}].[HierarchyVersions] hv ON hv.Id = sr.HierarchyVersionId
                  INNER JOIN [${productionSchema}].[Hierarchies] h ON h.Id = hv.HierarchyId
                  WHERE h.GroupId IN (${groupsListString})`
        },
        // 5. HierarchySplits - via StateRules -> HierarchyVersions -> Hierarchies
        {
          name: 'HierarchySplits',
          query: `DELETE hs FROM [${productionSchema}].[HierarchySplits] hs
                  INNER JOIN [${productionSchema}].[StateRules] sr ON sr.Id = hs.StateRuleId
                  INNER JOIN [${productionSchema}].[HierarchyVersions] hv ON hv.Id = sr.HierarchyVersionId
                  INNER JOIN [${productionSchema}].[Hierarchies] h ON h.Id = hv.HierarchyId
                  WHERE h.GroupId IN (${groupsListString})`
        },
        // 6. StateRuleStates - via StateRules -> HierarchyVersions -> Hierarchies
        {
          name: 'StateRuleStates',
          query: `DELETE srs FROM [${productionSchema}].[StateRuleStates] srs
                  INNER JOIN [${productionSchema}].[StateRules] sr ON sr.Id = srs.StateRuleId
                  INNER JOIN [${productionSchema}].[HierarchyVersions] hv ON hv.Id = sr.HierarchyVersionId
                  INNER JOIN [${productionSchema}].[Hierarchies] h ON h.Id = hv.HierarchyId
                  WHERE h.GroupId IN (${groupsListString})`
        },
        // 7. StateRules - via HierarchyVersions -> Hierarchies
        {
          name: 'StateRules',
          query: `DELETE sr FROM [${productionSchema}].[StateRules] sr
                  INNER JOIN [${productionSchema}].[HierarchyVersions] hv ON hv.Id = sr.HierarchyVersionId
                  INNER JOIN [${productionSchema}].[Hierarchies] h ON h.Id = hv.HierarchyId
                  WHERE h.GroupId IN (${groupsListString})`
        },
        // 8. HierarchyParticipants - via HierarchyVersions -> Hierarchies
        {
          name: 'HierarchyParticipants',
          query: `DELETE hp FROM [${productionSchema}].[HierarchyParticipants] hp
                  INNER JOIN [${productionSchema}].[HierarchyVersions] hv ON hv.Id = hp.HierarchyVersionId
                  INNER JOIN [${productionSchema}].[Hierarchies] h ON h.Id = hv.HierarchyId
                  WHERE h.GroupId IN (${groupsListString})`
        },
        // 9. HierarchyVersions - via Hierarchies
        {
          name: 'HierarchyVersions',
          query: `DELETE hv FROM [${productionSchema}].[HierarchyVersions] hv
                  INNER JOIN [${productionSchema}].[Hierarchies] h ON h.Id = hv.HierarchyId
                  WHERE h.GroupId IN (${groupsListString})`
        },
        // 10. PremiumSplitParticipants - via PremiumSplitVersions (bigint GroupId)
        {
          name: 'PremiumSplitParticipants',
          query: `DELETE psp FROM [${productionSchema}].[PremiumSplitParticipants] psp
                  INNER JOIN [${productionSchema}].[PremiumSplitVersions] psv ON psv.Id = psp.VersionId
                  WHERE psv.GroupId IN (${groupsListNumeric})`
        },
        // 11. PremiumSplitVersions - direct GroupId (bigint)
        {
          name: 'PremiumSplitVersions',
          query: `DELETE FROM [${productionSchema}].[PremiumSplitVersions] WHERE GroupId IN (${groupsListNumeric})`
        },
        // 12. Hierarchies - direct GroupId (nvarchar)
        {
          name: 'Hierarchies',
          query: `DELETE FROM [${productionSchema}].[Hierarchies] WHERE GroupId IN (${groupsListString})`
        },
        // 13. ProposalProducts - via Proposals
        {
          name: 'ProposalProducts',
          query: `DELETE pp FROM [${productionSchema}].[ProposalProducts] pp
                  INNER JOIN [${productionSchema}].[Proposals] p ON p.Id = pp.ProposalId
                  WHERE p.GroupId IN (${groupsListString})`
        },
        // 14. Proposals - direct GroupId (nvarchar)
        {
          name: 'Proposals',
          query: `DELETE FROM [${productionSchema}].[Proposals] WHERE GroupId IN (${groupsListString})`
        }
      ];
      
      for (const { name, query } of selectiveDeleteQueries) {
        try {
          const result = await pool.request().query(query);
          const deleted = result.rowsAffected[0] || 0;
          totalDeleted += deleted;
          if (deleted > 0 || verbose) {
            console.log(`  🗑️  ${name}: ${deleted} rows deleted`);
          }
        } catch (err: any) {
          console.error(`  ❌ Error deleting ${name}: ${err.message}`);
        }
      }
      
    } else {
      // FULL DELETE: Delete all production data
      const deletions = [
        // Commission assignments (leaf tables)
        { table: 'CommissionAssignmentRecipients', schema: productionSchema },
        { table: 'CommissionAssignmentVersions', schema: productionSchema },
        // Policy hierarchy assignments
        { table: 'PolicyHierarchyAssignments', schema: productionSchema },
        // Split distributions and hierarchy splits (deepest in hierarchy chain)
        { table: 'SplitDistributions', schema: productionSchema },
        { table: 'HierarchySplits', schema: productionSchema },
        { table: 'StateRuleStates', schema: productionSchema },
        { table: 'StateRules', schema: productionSchema },
        // Hierarchy participants and versions
        { table: 'HierarchyParticipants', schema: productionSchema },
        { table: 'HierarchyVersions', schema: productionSchema },
        // Premium split participants and versions
        { table: 'PremiumSplitParticipants', schema: productionSchema },
        { table: 'PremiumSplitVersions', schema: productionSchema },
        // Hierarchies (after versions and participants)
        { table: 'Hierarchies', schema: productionSchema },
        // Proposal products and proposals
        { table: 'ProposalProducts', schema: productionSchema },
        { table: 'Proposals', schema: productionSchema },
        // NOTE: Brokers are NOT deleted - they're shared across the system
        // and referenced by EmployerGroups, Policies, etc.
        // Broker export is ADDITIVE (insert missing only)
      ];
      
      for (const { table, schema } of deletions) {
        try {
          // Check if table exists
          const checkResult = await pool.request().query(`
            SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME = '${table}'
          `);
          
          if (checkResult.recordset[0].cnt === 0) {
            if (verbose) console.log(`  ⏭️  ${schema}.${table} does not exist, skipping`);
            continue;
          }
          
          const deleteResult = await pool.request().query(`DELETE FROM [${schema}].[${table}]`);
          const deleted = deleteResult.rowsAffected[0] || 0;
          totalDeleted += deleted;
          console.log(`  🗑️  ${table}: ${deleted} rows deleted`);
        } catch (err: any) {
          console.error(`  ❌ Error deleting ${table}: ${err.message}`);
        }
      }
    }
    
    console.log(`\n  Total deleted: ${totalDeleted} rows`);
    
    // ==========================================================================
    // STEP 2: INSERT staging data (FK order - parents first)
    // ==========================================================================
    console.log('\n' + '='.repeat(70));
    console.log('STEP 2: Exporting staging to production (FK order - parents first)');
    console.log('='.repeat(70));
    
    // Check for missing brokers that proposals will reference
    console.log('\n  Checking for brokers referenced by proposals...');
    const missingBrokers = await pool.request().query(`
      SELECT DISTINCT sp.BrokerUniquePartyId
      FROM [${etlSchema}].[stg_proposals] sp
      WHERE sp.BrokerUniquePartyId IS NOT NULL
        AND sp.BrokerUniquePartyId NOT IN (
          SELECT ExternalPartyId FROM [${productionSchema}].[Brokers] WHERE ExternalPartyId IS NOT NULL
        )
        AND sp.BrokerUniquePartyId NOT IN (
          SELECT ExternalPartyId FROM [${etlSchema}].[stg_brokers] WHERE ExternalPartyId IS NOT NULL
        )
    `);
    
    if (missingBrokers.recordset.length > 0) {
      console.log(`  ⚠️  WARNING: ${missingBrokers.recordset.length} brokers referenced by proposals not found in staging or production:`);
      for (const row of missingBrokers.recordset.slice(0, 10)) {
        console.log(`      - ${row.BrokerUniquePartyId}`);
      }
      if (missingBrokers.recordset.length > 10) {
        console.log(`      ... and ${missingBrokers.recordset.length - 10} more`);
      }
    } else {
      console.log('  ✅ All brokers referenced by proposals exist');
    }
    
    // Export SQL files in FK dependency order (parents first)
    const exportScripts = [
      // 0. Brokers (required FK for Proposals.BrokerUniquePartyId) - ADDITIVE
      'sql/export/02-export-brokers.sql',
      // 0a. Broker licenses (additive)
      'sql/export/13-export-licenses.sql',
      // 1. Proposals (parent of splits, referenced by hierarchies)
      'sql/export/07-export-proposals.sql',
      // 2. Hierarchies, HierarchyVersions, HierarchyParticipants, StateRules, StateRuleStates, HierarchySplits, SplitDistributions
      'sql/export/08-export-hierarchies.sql',
      // 3. PremiumSplitVersions, PremiumSplitParticipants (references Proposals and Hierarchies)
      'sql/export/11-export-splits.sql',
      // 4. PolicyHierarchyAssignments (references Policies, Hierarchies, Brokers)
      'sql/export/14-export-policy-hierarchy-assignments.sql',
      // 5. CommissionAssignmentVersions, CommissionAssignmentRecipients
      'sql/export/13-export-commission-assignments.sql',
    ];
    
    let totalRowsAffected = 0;
    const results: { script: string; rows: number }[] = [];
    
    for (const scriptPath of exportScripts) {
      const fullPath = path.join(process.cwd(), scriptPath);
      
      if (!fs.existsSync(fullPath)) {
        console.log(`  ⚠️  Skipping ${scriptPath} (not found)`);
        continue;
      }
      
      const result = await executeSqlFile(pool, fullPath, etlSchema, productionSchema, verbose);
      totalRowsAffected += result.rowsAffected;
      results.push({ script: path.basename(scriptPath), rows: result.rowsAffected });
    }
    
    // Summary
    console.log('\n' + '='.repeat(70));
    console.log('EXPORT SUMMARY');
    console.log('='.repeat(70));
    console.log('\nExport Results:');
    for (const r of results) {
      console.log(`  ${r.script}: ${r.rows} rows`);
    }
    console.log(`\nTotal rows affected: ${totalRowsAffected}`);
    
    // Verification query
    console.log('\nProduction Counts:');
    const verification = await pool.request().query(`
      SELECT 'Proposals' as Entity, COUNT(*) as [Count] FROM [${productionSchema}].[Proposals]
      UNION ALL SELECT 'ProposalProducts', COUNT(*) FROM [${productionSchema}].[ProposalProducts]
      UNION ALL SELECT 'Hierarchies', COUNT(*) FROM [${productionSchema}].[Hierarchies]
      UNION ALL SELECT 'HierarchyVersions', COUNT(*) FROM [${productionSchema}].[HierarchyVersions]
      UNION ALL SELECT 'HierarchyParticipants', COUNT(*) FROM [${productionSchema}].[HierarchyParticipants]
      UNION ALL SELECT 'StateRules', COUNT(*) FROM [${productionSchema}].[StateRules]
      UNION ALL SELECT 'StateRuleStates', COUNT(*) FROM [${productionSchema}].[StateRuleStates]
      UNION ALL SELECT 'HierarchySplits', COUNT(*) FROM [${productionSchema}].[HierarchySplits]
      UNION ALL SELECT 'SplitDistributions', COUNT(*) FROM [${productionSchema}].[SplitDistributions]
      UNION ALL SELECT 'PremiumSplitVersions', COUNT(*) FROM [${productionSchema}].[PremiumSplitVersions]
      UNION ALL SELECT 'PremiumSplitParticipants', COUNT(*) FROM [${productionSchema}].[PremiumSplitParticipants]
      UNION ALL SELECT 'PolicyHierarchyAssignments', COUNT(*) FROM [${productionSchema}].[PolicyHierarchyAssignments]
      UNION ALL SELECT 'CommissionAssignmentVersions', COUNT(*) FROM [${productionSchema}].[CommissionAssignmentVersions]
      UNION ALL SELECT 'CommissionAssignmentRecipients', COUNT(*) FROM [${productionSchema}].[CommissionAssignmentRecipients]
      ORDER BY 1
    `);
    
    for (const row of verification.recordset) {
      console.log(`  ${row.Entity}: ${row.Count}`);
    }
    
  } finally {
    await pool.close();
  }
  
  console.log('\n✅ Export complete!');
}

// =============================================================================
// CLI Entry Point
// =============================================================================

if (require.main === module) {
  const args = process.argv.slice(2);
  
  // Parse --mode option
  const modeArg = args.includes('--mode') 
    ? args[args.indexOf('--mode') + 1] as ExecutionMode
    : 'transform';
  
  // Validate mode
  if (!['transform', 'export', 'full'].includes(modeArg)) {
    console.error(`ERROR: Invalid mode '${modeArg}'. Must be one of: transform, export, full`);
    process.exit(1);
  }
  
  // Parse --groups option (comma-separated or multiple --groups flags)
  let groups: string[] | undefined;
  if (args.includes('--groups')) {
    const groupsIdx = args.indexOf('--groups');
    const groupsArg = args[groupsIdx + 1];
    if (groupsArg && !groupsArg.startsWith('--')) {
      // Support comma-separated: --groups 26683,12345,67890
      // Also support space-separated until next flag
      groups = groupsArg.split(',').map(g => g.trim()).filter(g => g.length > 0);
    }
  }
  
  // Parse CLI arguments
  const options: BuilderOptions = {
    mode: modeArg,
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
      : 'etl',
    referenceSchema: args.includes('--reference-schema')
      ? args[args.indexOf('--reference-schema') + 1]
      : 'dbo',
    productionSchema: args.includes('--production-schema')
      ? args[args.indexOf('--production-schema') + 1]
      : 'dbo',
    groups: groups
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

  // Execute based on mode
  async function execute() {
    const mode = options.mode || 'transform';
    
    console.log(`\nExecution Mode: ${mode.toUpperCase()}`);
    if (options.groups && options.groups.length > 0) {
      console.log(`Groups Filter: ${options.groups.join(', ')}`);
    }
    console.log('');
    
    if (mode === 'transform' || mode === 'full') {
      // Run transform (proposal building)
      const transformFn = options.batchSize 
        ? runProposalBuilderBatched 
        : runProposalBuilder;
      
      await transformFn(config, options);
    }
    
    if (mode === 'export' || mode === 'full') {
      // Run export (staging to production)
      await exportStagingToProduction(config, options);
    }
    
    console.log('');
    console.log('✅ Done!');
  }
  
  execute()
    .then(() => process.exit(0))
    .catch(err => {
      console.error('❌ Error:', err);
      process.exit(1);
    });
}
