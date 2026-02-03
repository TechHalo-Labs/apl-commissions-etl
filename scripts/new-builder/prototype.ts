/**
 * APL Commission System - Redesigned Proposal Builder (TypeScript)
 * 
 * Processes certificate data into staging entities, with entropy analysis to decide proposal vs. PHA routing.
 * 
 * Flow:
 * 1. Load certificates (batched by group).
 * 2. For each group: Analyze entropy.
 * 3. If high entropy: Route all to PHA.
 * 4. Else: Generate proposals for large clusters, PHA for small/outliers.
 * 5. Write to staging tables (bulk inserts).
 * 
 * Configurable via BuilderOptions (e.g., entropy thresholds).
 * 
 * Dependencies: crypto, mssql, fs, path (as before).
 */

import * as crypto from 'crypto';
import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import { groupBy } from 'lodash'; // Add lodash for grouping (npm i lodash)

// Reuse your original interfaces (CertificateRecord, HierarchyTier, etc.)
// ... (omit for brevity; assume they are imported or defined here)

// Entropy classification enum
enum EntropyLevel {
  Low = 'Low',                // Mostly conformant
  HumanError = 'HumanError',  // Outliers, handle with proposals + PHA
  BusinessDriven = 'BusinessDriven' // High chaos, full PHA
}

// Extended BuilderOptions with entropy thresholds
interface BuilderOptions {
  mode?: ExecutionMode; // 'transform' | 'export' | 'full'
  batchSize?: number;   // For certificate loading
  dryRun?: boolean;
  verbose?: boolean;
  limitCertificates?: number;
  schema?: string;      // 'etl'
  referenceSchema?: string; // 'dbo'
  productionSchema?: string; // 'dbo'
  groups?: string[];    // Filter groups
  // New: Entropy thresholds (adjustable)
  highEntropyUniqueRatio: number; // e.g., 0.2 (20% unique configs = high)
  highEntropyShannon: number;     // e.g., 5.0
  dominantCoverageThreshold: number; // e.g., 0.5 (50%)
  phaClusterSizeThreshold: number;   // Clusters < this size -> PHA (e.g., 3)
}

// Default options
const defaultOptions: BuilderOptions = {
  mode: 'transform',
  batchSize: 5000,
  dryRun: false,
  verbose: false,
  schema: 'etl',
  referenceSchema: 'dbo',
  productionSchema: 'dbo',
  highEntropyUniqueRatio: 0.2,
  highEntropyShannon: 5.0,
  dominantCoverageThreshold: 0.5,
  phaClusterSizeThreshold: 3,
};

// Interface for entropy results
interface EntropyResult {
  level: EntropyLevel;
  uniqueConfigs: number;
  totalRecords: number;
  dominantRecords: number;
  simpleEntropy: number;
  shannonEntropy: number;
  reason: string;
}

// Interface for cluster (similar to your SelectionCriteria)
interface Cluster {
  configHash: string;
  recordCount: number;
  splitConfig: SplitConfiguration;
  certificateIds: string[];
  // Other fields as needed (effectiveDate, productCode, etc.)
}

// Interface for staging writer (DI)
interface IStagingWriter {
  writeProposals(proposals: StagingProposal[]): Promise<void>;
  writePHA(phas: StagingPolicyHierarchyAssignment[]): Promise<void>;
  // Add methods for other entities (hierarchies, splits, etc.)
}

// Mock writer for testing/dry-run
class MockStagingWriter implements IStagingWriter {
  async writeProposals(proposals: StagingProposal[]): Promise<void> {
    console.log(`[MOCK] Would write ${proposals.length} proposals`);
  }
  async writePHA(phas: StagingPolicyHierarchyAssignment[]): Promise<void> {
    console.log(`[MOCK] Would write ${phas.length} PHAs`);
  }
  // Implement others...
}

// Real DB writer (using mssql)
class DbStagingWriter implements IStagingWriter {
  private pool: sql.ConnectionPool;

  constructor(connectionString: string) {
    this.pool = new sql.ConnectionPool(connectionString);
    this.pool.connect();
  }

  async writeProposals(proposals: StagingProposal[]): Promise<void> {
    const request = this.pool.request();
    // Bulk insert logic (use table-valued params or loops for simplicity)
    for (const p of proposals) {
      await request.query(`INSERT INTO ${options.schema}.stg_proposals (...) VALUES (...)`);
    }
  }

  async writePHA(phas: StagingPolicyHierarchyAssignment[]): Promise<void> {
    // Similar bulk insert
  }
  // Implement others...
}

/**
 * Entropy Analyzer (Single Responsibility: Analyze group entropy)
 * Testable: Pure functions, no side effects.
 */
class EntropyAnalyzer {
  private options: BuilderOptions;

  constructor(options: BuilderOptions) {
    this.options = options;
  }

  /**
   * Calculate entropy metrics for a group's clusters.
   * @param clusters Grouped by configHash
   */
  calculate(clusters: Cluster[]): EntropyResult {
    const totalRecords = clusters.reduce((sum, c) => sum + c.recordCount, 0);
    const uniqueConfigs = clusters.length;
    const simpleEntropy = uniqueConfigs / totalRecords;

    // Shannon entropy
    const probs = clusters.map(c => c.recordCount / totalRecords);
    const shannonEntropy = -probs.reduce((sum, p) => sum + (p * Math.log2(p)), 0);

    // Dominant
    const sorted = [...clusters].sort((a, b) => b.recordCount - a.recordCount);
    const dominantRecords = sorted[0]?.recordCount || 0;
    const dominantPct = dominantRecords / totalRecords;

    let level: EntropyLevel;
    let reason: string;

    if (uniqueConfigs / totalRecords > this.options.highEntropyUniqueRatio ||
        shannonEntropy > this.options.highEntropyShannon ||
        dominantPct < this.options.dominantCoverageThreshold) {
      level = EntropyLevel.BusinessDriven;
      reason = 'High variability; likely intentional non-conformance.';
    } else if (dominantPct > 0.7 && uniqueConfigs / totalRecords < 0.1) {
      level = EntropyLevel.Low;
      reason = 'Mostly conformant; minimal outliers.';
    } else {
      level = EntropyLevel.HumanError;
      reason = 'Outliers present; likely data errors.';
    }

    return { level, uniqueConfigs, totalRecords, dominantRecords, simpleEntropy, shannonEntropy, reason };
  }
}

/**
 * Cluster Generator (Single Responsibility: Group certificates into clusters)
 */
class ClusterGenerator {
  generate(certificates: CertificateRecord[]): Cluster[] {
    // Group by computed configHash (your hash logic)
    const grouped = groupBy(certificates, cert => this.computeConfigHash(cert.splitConfig));
    
    return Object.entries(grouped).map(([hash, certs]) => ({
      configHash: hash,
      recordCount: certs.length,
      splitConfig: certs[0].splitConfig, // Assume same per group
      certificateIds: certs.map(c => c.certificateId),
    }));
  }

  private computeConfigHash(splitConfig: SplitConfiguration): string {
    const json = JSON.stringify(splitConfig);
    return crypto.createHash('sha256').update(json).digest('hex');
  }
}

/**
 * Proposal Generator (Single Responsibility: Create proposals from clusters)
 */
class ProposalGenerator {
  generate(clusters: Cluster[], groupId: string, entropy: EntropyResult): Proposal[] {
    const proposals: Proposal[] = [];

    // Filter clusters based on entropy
    const minSize = entropy.level === EntropyLevel.Low ? 1 : this.options.phaClusterSizeThreshold;

    for (const cluster of clusters) {
      if (cluster.recordCount >= minSize) {
        proposals.push(this.createProposalFromCluster(cluster, groupId));
      }
    }
    return proposals;
  }

  private createProposalFromCluster(cluster: Cluster, groupId: string): Proposal {
    // Your original proposal creation logic
    // ... (generate ID, set dates, products, etc.)
    return {
      id: `P-${groupId}-${cluster.configHash.slice(0, 8)}`,
      // Fill other fields...
      splitConfig: cluster.splitConfig,
      certificateIds: cluster.certificateIds,
    };
  }
}

/**
 * PHA Generator (Single Responsibility: Create PHAs for outliers or high entropy)
 */
class PHAGenerator {
  generate(certificates: CertificateRecord[], clusters: Cluster[], entropy: EntropyResult): PolicyHierarchyAssignment[] {
    const phas: PolicyHierarchyAssignment[] = [];

    if (entropy.level === EntropyLevel.BusinessDriven) {
      // Bulk PHA for all
      for (const cert of certificates) {
        phas.push(this.createPHAFromCert(cert, 'BusinessDrivenEntropy'));
      }
    } else {
      // PHA for small clusters
      const smallClusters = clusters.filter(c => c.recordCount < this.options.phaClusterSizeThreshold);
      for (const cluster of smallClusters) {
        for (const certId of cluster.certificateIds) {
          const cert = certificates.find(c => c.certificateId === certId)!;
          phas.push(this.createPHAFromCert(cert, 'HumanErrorOutlier'));
        }
      }
    }
    return phas;
  }

  private createPHAFromCert(cert: CertificateRecord, reason: string): PolicyHierarchyAssignment {
    // Your PHA creation logic
    return {
      certificateId: cert.certificateId,
      groupId: cert.groupId,
      effectiveDate: cert.certEffectiveDate,
      splitConfig: cert.splitConfig, // From cert
      reason,
    };
  }
}

/**
 * Main Orchestrator (Dependency Inversion: Injects components)
 */
class ProposalBuilder {
  private options: BuilderOptions;
  private writer: IStagingWriter;
  private analyzer: EntropyAnalyzer;
  private clusterGen: ClusterGenerator;
  private proposalGen: ProposalGenerator;
  private phaGen: PHAGenerator;
  private auditLog: AuditLog = { /* init */ };

  constructor(options: Partial<BuilderOptions> = {}, connectionString: string) {
    this.options = { ...defaultOptions, ...options };
    this.writer = this.options.dryRun ? new MockStagingWriter() : new DbStagingWriter(connectionString);
    this.analyzer = new EntropyAnalyzer(this.options);
    this.clusterGen = new ClusterGenerator();
    this.proposalGen = new ProposalGenerator();
    this.phaGen = new PHAGenerator();
  }

  async run(): Promise<AuditLog> {
    const certificates = await this.loadCertificates();
    const groups = groupBy(certificates, 'groupId');

    for (const [groupId, groupCerts] of Object.entries(groups)) {
      if (this.options.groups && !this.options.groups.includes(groupId)) continue;

      const clusters = this.clusterGen.generate(groupCerts);
      const entropy = this.analyzer.calculate(clusters);

      if (entropy.level === EntropyLevel.BusinessDriven) {
        const phas = this.phaGen.generate(groupCerts, clusters, entropy);
        await this.writePHAEntities(phas); // Also writes related hierarchies, etc.
      } else {
        const proposals = this.proposalGen.generate(clusters, groupId, entropy);
        const phas = this.phaGen.generate(groupCerts, clusters, entropy);
        await this.writeProposalEntities(proposals); // Writes proposals, splits, hierarchies
        await this.writePHAEntities(phas);
      }

      // Update audit
      this.auditLog.certificatesProcessed += groupCerts.length;
      // ...
    }

    return this.auditLog;
  }

  private async loadCertificates(): Promise<CertificateRecord[]> {
    // Query input_certificate_info (batched)
    const request = new sql.Request();
    // ... (implement batching with OFFSET/FETCH)
    return []; // Placeholder
  }

  private async writeProposalEntities(proposals: Proposal[]): Promise<void> {
    // Generate all staging entities from proposals (splits, hierarchies, etc.)
    const output: StagingOutput = this.generateStagingFromProposals(proposals);
    await this.writer.writeProposals(output.proposals);
    // Call other write methods...
  }

  private async writePHAEntities(phas: PolicyHierarchyAssignment[]): Promise<void> {
    // Generate hierarchies/participants for PHAs
    const output: StagingOutput = this.generateStagingFromPHAs(phas);
    await this.writer.writePHA(output.policyHierarchyAssignments);
    // ...
  }

  private generateStagingFromProposals(proposals: Proposal[]): StagingOutput {
    // Your original transformation logic
    return { proposals: [], /* fill */ };
  }

  private generateStagingFromPHAs(phas: PolicyHierarchyAssignment[]): StagingOutput {
    // Similar, but for PHA
    return { policyHierarchyAssignments: [], /* fill */ };
  }
}

// Usage
const builder = new ProposalBuilder({ verbose: true }, 'your-connection-string');
builder.run().then(log => console.log('Audit:', log));

// =============================================================================
// Test Stubs (Incrementally Testable)
// =============================================================================

// Test EntropyAnalyzer
const analyzer = new EntropyAnalyzer(defaultOptions);
const testClusters: Cluster[] = [ /* mock data */ ];
const result = analyzer.calculate(testClusters);
expect(result.level).toBe(EntropyLevel.HumanError); // Using jest or similar

// Test ClusterGenerator
const gen = new ClusterGenerator();
const clusters = gen.generate([ /* mock certs */ ]);
expect(clusters.length).toBe(2);

// Etc. for each class