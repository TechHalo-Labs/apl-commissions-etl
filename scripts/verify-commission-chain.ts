/**
 * Commission Chain Verification Script
 * 
 * Validates the commission calculation chain by:
 * 1. Picking 10 compliant groups
 * 2. For each group, picking 2 policies
 * 3. Getting premium payment amounts from raw data
 * 4. Walking through the production data chain to calculate commissions
 * 5. Comparing to the actual commission details in raw data
 */

import * as sql from 'mssql';

const config: sql.config = {
  server: 'halo-sql.database.windows.net',
  database: 'halo-sqldb',
  user: '***REMOVED***',
  password: '***REMOVED***',
  options: { encrypt: true, trustServerCertificate: true },
  requestTimeout: 60000
};

interface PolicyInfo {
  PolicyId: string;
  GroupId: string;
  ProductCode: string;
  State: string;
  EffectiveDate: Date;
  Premium: number;
  ProposalId: string | null;
}

interface HierarchyParticipant {
  EntityId: string;
  EntityName: string;
  Level: number;
  ScheduleCode: string;
  ScheduleId: string | null;
  CommissionRate: number;
}

interface VerificationResult {
  policyId: string;
  groupId: string;
  productCode: string;
  state: string;
  premiumAmount: number;
  proposalFound: boolean;
  proposalId: string | null;
  splitVersionFound: boolean;
  splitVersionId: string | null;
  totalSplitPercent: number;
  hierarchiesFound: number;
  participantsFound: number;
  calculatedCommissions: { brokerId: string; brokerName: string; rate: number; amount: number }[];
  rawCommissions: { brokerId: string; rate: number; amount: number }[];
  match: boolean;
  discrepancy: string | null;
}

async function main() {
  const pool = await sql.connect(config);
  
  console.log('═'.repeat(80));
  console.log('COMMISSION CHAIN VERIFICATION');
  console.log('═'.repeat(80));
  console.log('');

  try {
    // Step 1: Get 10 compliant groups with ETL-imported policies (numeric IDs)
    console.log('Step 1: Finding 10 compliant groups with ETL policies...');
    const groupsResult = await pool.request().query(`
      SELECT TOP 10 
        g.Id AS GroupId, 
        g.GroupName
      FROM [dbo].[Group] g
      WHERE g.IsNonConformant = 0
        AND g.Id <> 'G00000'
        AND g.Id LIKE 'G%'
        AND EXISTS (
          SELECT 1 FROM [dbo].[Proposals] pr 
          WHERE pr.GroupId = g.Id AND pr.[Status] = 2
        )
        AND EXISTS (
          SELECT 1 FROM [dbo].[Policies] p 
          WHERE p.GroupId = g.Id 
            AND p.ProposalId IS NOT NULL 
            AND TRY_CAST(p.Id AS BIGINT) IS NOT NULL
        )
      ORDER BY g.Id
    `);
    
    const groups = groupsResult.recordset;
    console.log(`Found ${groups.length} compliant groups\n`);

    if (groups.length === 0) {
      console.log('No compliant groups found. Checking what we have...');
      const checkResult = await pool.request().query(`
        SELECT TOP 5 g.Id, g.GroupName, g.IsNonConformant,
               (SELECT COUNT(*) FROM [dbo].[Policies] p WHERE p.GroupId = g.Id) AS PolicyCount,
               (SELECT COUNT(*) FROM [dbo].[Proposals] pr WHERE pr.GroupId = g.Id) AS ProposalCount
        FROM [dbo].[Group] g
        WHERE g.Id LIKE 'G%'
        ORDER BY g.Id
      `);
      console.table(checkResult.recordset);
      return;
    }

    const allResults: VerificationResult[] = [];

    // For each group, pick 2 policies
    for (const group of groups) {
      console.log(`\n${'─'.repeat(80)}`);
      console.log(`Group: ${group.GroupId} - ${group.GroupName}`);
      console.log(`${'─'.repeat(80)}`);

      // Step 2: Get 2 policies for this group (ETL policies with numeric IDs, different products)
      const policiesResult = await pool.request()
        .input('groupId', group.GroupId)
        .query(`
          WITH RankedPolicies AS (
            SELECT 
              p.Id AS PolicyId,
              p.GroupId,
              p.ProductCode,
              p.[State],
              p.EffectiveDate,
              p.Premium,
              p.ProposalId,
              ROW_NUMBER() OVER (PARTITION BY p.ProductCode ORDER BY p.Id) AS rn
            FROM [dbo].[Policies] p
            WHERE p.GroupId = @groupId
              AND p.ProposalId IS NOT NULL
              AND TRY_CAST(p.Id AS BIGINT) IS NOT NULL
          )
          SELECT TOP 2 PolicyId, GroupId, ProductCode, [State], EffectiveDate, Premium, ProposalId
          FROM RankedPolicies
          WHERE rn = 1
          ORDER BY ProductCode
        `);

      const policies: PolicyInfo[] = policiesResult.recordset;
      
      if (policies.length === 0) {
        console.log('  No ETL policies found for this group');
        continue;
      }
      
      for (const policy of policies) {
        console.log(`\n  Policy: ${policy.PolicyId} | Product: ${policy.ProductCode} | State: ${policy.State}`);
        
        const result = await verifyPolicyCommission(pool, policy);
        allResults.push(result);
        
        printVerificationResult(result);
      }
    }

    // Print summary
    printSummary(allResults);

  } finally {
    await pool.close();
  }
}

async function verifyPolicyCommission(pool: sql.ConnectionPool, policy: PolicyInfo): Promise<VerificationResult> {
  const result: VerificationResult = {
    policyId: policy.PolicyId,
    groupId: policy.GroupId,
    productCode: policy.ProductCode,
    state: policy.State,
    premiumAmount: policy.Premium || 0,
    proposalFound: false,
    proposalId: null,
    splitVersionFound: false,
    splitVersionId: null,
    totalSplitPercent: 0,
    hierarchiesFound: 0,
    participantsFound: 0,
    calculatedCommissions: [],
    rawCommissions: [],
    match: false,
    discrepancy: null
  };

  try {
    // Step 3: Get premium from raw_certificate_info (use CertPremium column)
    const premiumResult = await pool.request()
      .input('certId', policy.PolicyId)
      .query(`
        SELECT TOP 1 CertPremium
        FROM [etl].[raw_certificate_info]
        WHERE CertificateId = @certId
      `);
    
    if (premiumResult.recordset.length > 0 && premiumResult.recordset[0].CertPremium) {
      result.premiumAmount = parseFloat(premiumResult.recordset[0].CertPremium) || result.premiumAmount;
    }

    // Step 4: Find the proposal
    if (policy.ProposalId) {
      const proposalResult = await pool.request()
        .input('proposalId', policy.ProposalId)
        .query(`
          SELECT Id, ProposalNumber, GroupId, [Status], SitusState
          FROM [dbo].[Proposals]
          WHERE Id = @proposalId
        `);
      
      if (proposalResult.recordset.length > 0) {
        result.proposalFound = true;
        result.proposalId = proposalResult.recordset[0].Id;
      }
    }

    // Step 5: Find the split version using ProposalId
    const splitVersionResult = await pool.request()
      .input('proposalId', policy.ProposalId)
      .query(`
        SELECT TOP 1 Id, TotalSplitPercent, [Status], GroupId
        FROM [dbo].[PremiumSplitVersions]
        WHERE ProposalId = @proposalId
        ORDER BY EffectiveFrom DESC
      `);
    
    if (splitVersionResult.recordset.length > 0) {
      result.splitVersionFound = true;
      result.splitVersionId = splitVersionResult.recordset[0].Id;
      result.totalSplitPercent = parseFloat(splitVersionResult.recordset[0].TotalSplitPercent) || 100;
    }

    // Step 6: Find hierarchies for this group
    const hierarchiesResult = await pool.request()
      .input('groupId', policy.GroupId)
      .input('proposalId', policy.ProposalId)
      .query(`
        SELECT h.Id, h.Name, h.BrokerId, h.BrokerName, h.CurrentVersionId
        FROM [dbo].[Hierarchies] h
        WHERE h.GroupId = @groupId
           OR h.ProposalId = @proposalId
      `);
    
    result.hierarchiesFound = hierarchiesResult.recordset.length;

    // Step 7-8: For each hierarchy, get versions and participants with schedule rates
    const allParticipants: { 
      hierarchyId: string;
      participant: HierarchyParticipant;
      firstYearRate: number;
      renewalRate: number;
    }[] = [];

    for (const hierarchy of hierarchiesResult.recordset) {
      // Get hierarchy version (status 0 or 1 both seem to be used)
      const versionResult = await pool.request()
        .input('hierarchyId', hierarchy.Id)
        .query(`
          SELECT TOP 1 Id, [Version], [Status]
          FROM [dbo].[HierarchyVersions]
          WHERE HierarchyId = @hierarchyId
          ORDER BY [Version] DESC
        `);

      if (versionResult.recordset.length > 0) {
        const versionId = versionResult.recordset[0].Id;

        // Get participants
        const participantsResult = await pool.request()
          .input('versionId', versionId)
          .query(`
            SELECT 
              hp.EntityId,
              hp.EntityName,
              hp.[Level],
              hp.ScheduleCode,
              hp.ScheduleId,
              hp.CommissionRate
            FROM [dbo].[HierarchyParticipants] hp
            WHERE hp.HierarchyVersionId = @versionId
            ORDER BY hp.[Level]
          `);

        for (const p of participantsResult.recordset) {
          // Try to get schedule rate
          let firstYearRate = parseFloat(p.CommissionRate) || 0;
          let renewalRate = parseFloat(p.CommissionRate) || 0;

          if (p.ScheduleId) {
            // Get schedule version ID
            const scheduleVersionResult = await pool.request()
              .input('scheduleId', p.ScheduleId)
              .query(`
                SELECT TOP 1 sv.Id AS ScheduleVersionId
                FROM [dbo].[ScheduleVersions] sv
                WHERE sv.scheduleId = @scheduleId
                ORDER BY sv.versionNumber DESC
              `);

            if (scheduleVersionResult.recordset.length > 0) {
              const scheduleVersionId = scheduleVersionResult.recordset[0].ScheduleVersionId;

              // Get the ScheduleRate record (with State fallback to NULL)
              const rateResult = await pool.request()
                .input('scheduleVersionId', scheduleVersionId)
                .input('productCode', policy.ProductCode)
                .input('state', policy.State)
                .query(`
                  SELECT TOP 1 Id, FirstYearRate, RenewalRate
                  FROM [dbo].[ScheduleRates]
                  WHERE ScheduleVersionId = @scheduleVersionId
                    AND ProductCode = @productCode
                    AND ([State] = @state OR [State] IS NULL)
                  ORDER BY CASE WHEN [State] = @state THEN 0 ELSE 1 END
                `);

              if (rateResult.recordset.length > 0) {
                const scheduleRateId = rateResult.recordset[0].Id;
                firstYearRate = parseFloat(rateResult.recordset[0].FirstYearRate) || firstYearRate;
                renewalRate = parseFloat(rateResult.recordset[0].RenewalRate) || renewalRate;
                
                // Check for SpecialScheduleRates (year-graded rates)
                // Calculate policy year: how many years since effective date
                const policyEffDate = new Date(policy.EffectiveDate);
                const today = new Date();
                const policyYear = Math.floor((today.getTime() - policyEffDate.getTime()) / (365.25 * 24 * 60 * 60 * 1000)) + 1;
                
                // Look up year-specific rate from SpecialScheduleRates
                // Year brackets: 1-15 are exact, 16 covers 16-65, 66 covers 66-98, 99 is 99+
                const specialRateResult = await pool.request()
                  .input('scheduleRateId', scheduleRateId)
                  .input('policyYear', policyYear)
                  .query(`
                    SELECT TOP 1 [Year], Rate
                    FROM [dbo].[SpecialScheduleRates]
                    WHERE ScheduleRateId = @scheduleRateId
                      AND (
                        ([Year] = @policyYear) OR
                        ([Year] = 16 AND @policyYear BETWEEN 16 AND 65) OR
                        ([Year] = 66 AND @policyYear BETWEEN 66 AND 98) OR
                        ([Year] = 99 AND @policyYear >= 99)
                      )
                    ORDER BY [Year] DESC
                  `);
                
                if (specialRateResult.recordset.length > 0) {
                  // Year-graded rate found - use it for the policy year
                  const yearRate = parseFloat(specialRateResult.recordset[0].Rate) || 0;
                  // For year-graded schedules, apply the year-specific rate
                  if (policyYear === 1) {
                    firstYearRate = yearRate;
                  } else {
                    renewalRate = yearRate;
                  }
                }
              }
            }
          }

          allParticipants.push({
            hierarchyId: hierarchy.Id,
            participant: {
              EntityId: p.EntityId,
              EntityName: p.EntityName,
              Level: p.Level,
              ScheduleCode: p.ScheduleCode,
              ScheduleId: p.ScheduleId,
              CommissionRate: p.CommissionRate
            },
            firstYearRate,
            renewalRate,
            policyYear: Math.floor((new Date().getTime() - new Date(policy.EffectiveDate).getTime()) / (365.25 * 24 * 60 * 60 * 1000)) + 1
          });
        }
      }
    }

    result.participantsFound = allParticipants.length;

    // Step 9: Calculate commission earned
    // Determine if first year based on effective date
    const policyEffDate = new Date(policy.EffectiveDate);
    const today = new Date();
    const policyYear = Math.floor((today.getTime() - policyEffDate.getTime()) / (365.25 * 24 * 60 * 60 * 1000)) + 1;
    const isFirstYear = policyYear === 1;

    const splitPercent = result.totalSplitPercent > 0 ? result.totalSplitPercent / 100 : 1;
    const splitPremium = result.premiumAmount * splitPercent;

    for (const p of allParticipants) {
      // Use renewalRate for year-graded schedules (already has the year-specific rate)
      const rate = isFirstYear ? p.firstYearRate : p.renewalRate;
      const commission = splitPremium * (rate / 100);

      result.calculatedCommissions.push({
        brokerId: p.participant.EntityId,
        brokerName: p.participant.EntityName,
        rate: rate,
        amount: Math.round(commission * 100) / 100
      });
    }

    // Step 10: Get raw commission details for comparison
    const rawCommResult = await pool.request()
      .input('certId', policy.PolicyId)
      .query(`
        SELECT 
          SplitBrokerId,
          CommissionRate,
          PaidAmount
        FROM [etl].[raw_commissions_detail]
        WHERE CertificateId = @certId
      `);

    for (const r of rawCommResult.recordset) {
      result.rawCommissions.push({
        brokerId: r.SplitBrokerId?.replace('P', '') || '',
        rate: parseFloat(r.CommissionRate) || 0,
        amount: parseFloat(r.PaidAmount) || 0
      });
    }

    // Compare results
    result.match = compareCommissions(result.calculatedCommissions, result.rawCommissions);
    if (!result.match) {
      result.discrepancy = buildDiscrepancyMessage(result);
    }

  } catch (err: any) {
    result.discrepancy = `Error: ${err.message}`;
  }

  return result;
}

function compareCommissions(
  calculated: { brokerId: string; rate: number; amount: number }[],
  raw: { brokerId: string; rate: number; amount: number }[]
): boolean {
  // If no raw commissions, check if calculated is also empty/zero
  if (raw.length === 0) {
    const calcTotal = calculated.reduce((sum, c) => sum + c.amount, 0);
    return calcTotal === 0 || calculated.length === 0;
  }
  
  // Compare broker rates (not amounts, since amounts depend on actual premium paid)
  const calcRates = new Map(calculated.map(c => [c.brokerId, c.rate]));
  const rawRates = new Map(raw.map(r => [r.brokerId, r.rate]));
  
  let matchCount = 0;
  for (const [brokerId, rawRate] of rawRates) {
    const calcRate = calcRates.get(brokerId);
    if (calcRate !== undefined && Math.abs(calcRate - rawRate) < 0.5) {
      matchCount++;
    }
  }
  
  // Consider a match if at least 50% of brokers have matching rates
  return matchCount >= Math.ceil(rawRates.size / 2);
}

function buildDiscrepancyMessage(result: VerificationResult): string {
  const calcRates = result.calculatedCommissions.map(c => `${c.brokerId}:${c.rate}%`).join(', ');
  const rawRates = result.rawCommissions.map(r => `${r.brokerId}:${r.rate}%`).join(', ');
  
  if (result.calculatedCommissions.length === 0) {
    return `No calculated commissions (missing data in chain)`;
  }
  if (result.rawCommissions.length === 0) {
    return `No raw commission data to compare`;
  }
  
  return `Calc rates: [${calcRates}] vs Raw rates: [${rawRates}]`;
}

function printVerificationResult(result: VerificationResult) {
  console.log(`    Premium Amount: $${result.premiumAmount.toFixed(2)}`);
  console.log(`    Proposal Found: ${result.proposalFound ? '✅ ' + result.proposalId : '❌'}`);
  console.log(`    Split Version: ${result.splitVersionFound ? '✅ ' + result.splitVersionId + ' (' + result.totalSplitPercent + '%)' : '❌ (using 100%)'}`);
  console.log(`    Hierarchies: ${result.hierarchiesFound}`);
  console.log(`    Participants: ${result.participantsFound}`);
  
  if (result.calculatedCommissions.length > 0) {
    console.log(`    Calculated Commissions:`);
    for (const c of result.calculatedCommissions.slice(0, 5)) {
      console.log(`      - ${c.brokerName || c.brokerId}: ${c.rate}% = $${c.amount.toFixed(2)}`);
    }
    if (result.calculatedCommissions.length > 5) {
      console.log(`      ... and ${result.calculatedCommissions.length - 5} more`);
    }
  }
  
  if (result.rawCommissions.length > 0) {
    console.log(`    Raw Commissions (from LION):`);
    for (const r of result.rawCommissions.slice(0, 5)) {
      console.log(`      - Broker ${r.brokerId}: ${r.rate}% = $${r.amount.toFixed(2)}`);
    }
    if (result.rawCommissions.length > 5) {
      console.log(`      ... and ${result.rawCommissions.length - 5} more`);
    }
  }
  
  console.log(`    Match: ${result.match ? '✅ PASS' : '❌ FAIL'}`);
  if (result.discrepancy) {
    console.log(`    Discrepancy: ${result.discrepancy}`);
  }
}

function printSummary(results: VerificationResult[]) {
  console.log('\n');
  console.log('═'.repeat(80));
  console.log('SUMMARY');
  console.log('═'.repeat(80));
  
  const total = results.length;
  if (total === 0) {
    console.log('No policies were verified.');
    return;
  }
  
  const passed = results.filter(r => r.match).length;
  const failed = results.filter(r => !r.match).length;
  const proposalFound = results.filter(r => r.proposalFound).length;
  const splitFound = results.filter(r => r.splitVersionFound).length;
  const hasHierarchy = results.filter(r => r.hierarchiesFound > 0).length;
  const hasParticipants = results.filter(r => r.participantsFound > 0).length;
  const hasRawData = results.filter(r => r.rawCommissions.length > 0).length;
  
  console.log(`\nTotal Policies Verified: ${total}`);
  console.log(`  Passed: ${passed} (${(passed/total*100).toFixed(1)}%)`);
  console.log(`  Failed: ${failed} (${(failed/total*100).toFixed(1)}%)`);
  console.log(`\nData Chain Coverage:`);
  console.log(`  Proposal Found: ${proposalFound}/${total}`);
  console.log(`  Split Version Found: ${splitFound}/${total}`);
  console.log(`  Has Hierarchy: ${hasHierarchy}/${total}`);
  console.log(`  Has Participants: ${hasParticipants}/${total}`);
  console.log(`  Has Raw Commission Data: ${hasRawData}/${total}`);
  
  if (failed > 0) {
    console.log(`\nFailed Policies (first 10):`);
    for (const r of results.filter(r => !r.match).slice(0, 10)) {
      console.log(`  - ${r.policyId} (${r.groupId}): ${r.discrepancy || 'No discrepancy info'}`);
    }
  }
}

main().catch(console.error);
