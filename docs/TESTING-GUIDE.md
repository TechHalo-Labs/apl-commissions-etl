# TypeScript Proposal Builder - Testing Guide

## Overview

This guide provides step-by-step instructions for testing the TypeScript proposal builder implementation to ensure it correctly generates all staging entities before production rollout.

## Test Phases

### Phase 1: Small Dataset Testing (100 certificates)
**Goal:** Verify basic functionality and correctness with manageable dataset  
**Duration:** ~5 minutes

### Phase 2: Medium Dataset Testing (10K certificates)
**Goal:** Test batching, performance, and edge cases  
**Duration:** ~15-20 minutes

### Phase 3: Full Dataset Testing (~150K active certificates)
**Goal:** Validate production-scale performance and memory usage  
**Duration:** ~15-20 minutes

**Note:** Active certificates (CertStatus='A' AND RecStatus='A') are ~150K, not the full ~400K in the database

### Phase 4: Edge Case Testing
**Goal:** Validate handling of special scenarios  
**Duration:** ~10-15 minutes

---

## Prerequisites

1. **Database access** - SQL Server with ETL schema
2. **Environment variables** - `SQLSERVER` connection string set
3. **Node.js** - Version 18+ with TypeScript support
4. **Baseline data** - ETL pipeline run with SQL approach for comparison (optional)

```bash
# Verify environment
echo $SQLSERVER  # Should output connection string
node --version   # Should be v18+
```

---

## Phase 1: Small Dataset Testing

### Step 1.1: Run TypeScript Builder with Limit

```bash
cd /path/to/apl-commissions-etl

# Run builder with 100 certificate limit
npx tsx scripts/proposal-builder.ts --limit 100 --verbose

# Expected output:
# - Certificates processed: 100
# - Proposals created: 10-30 (varies by data)
# - PHA records: 0-5 (for invalid groups)
# - Unique hierarchies: 10-30
# - Hash collisions: 0 (MUST be zero)
```

**Success Criteria:**
- ✅ No errors or exceptions
- ✅ Hash collisions = 0
- ✅ Proposals > 0
- ✅ All 9 staging tables populated

### Step 1.2: Validate Certificate Resolution

```bash
# Run small sample validation (20 certificates)
npm run validate-certificates -- --sample small

# Expected output:
# - Pass rate >= 95%
# - All checks passing:
#   ✅ proposalFound
#   ✅ proposalCorrect
#   ✅ splitConfigCorrect
#   ✅ hierarchyFound
#   ✅ hierarchyCorrect
#   ✅ foreignKeysIntact
#   ✅ configHashValid
```

**Success Criteria:**
- ✅ Pass rate >= 95%
- ✅ No critical errors
- ✅ Warnings acceptable (< 10%)

### Step 1.3: Manual Spot Check

```sql
-- Check proposal distribution
SELECT 
  COUNT(*) as proposal_count,
  COUNT(DISTINCT GroupId) as unique_groups,
  AVG(LEN(ProductCodes) - LEN(REPLACE(ProductCodes, ',', '')) + 1) as avg_products_per_proposal
FROM etl.stg_proposals;

-- Check split completeness
SELECT 
  COUNT(*) as split_count,
  AVG(TotalSplitPercent) as avg_split_pct
FROM etl.stg_premium_split_versions;
-- avg_split_pct should be close to 100.0

-- Check hierarchy linkage
SELECT 
  COUNT(DISTINCT h.Id) as unique_hierarchies,
  COUNT(DISTINCT psp.HierarchyId) as linked_hierarchies
FROM etl.stg_hierarchies h
LEFT JOIN etl.stg_premium_split_participants psp ON psp.HierarchyId = h.Id;
-- unique_hierarchies should equal linked_hierarchies

-- Check ConfigHash uniqueness
SELECT 
  SplitConfigHash, 
  COUNT(*) as proposal_count
FROM etl.stg_proposals
GROUP BY SplitConfigHash
HAVING COUNT(*) > 1;
-- Should return 0 rows if no hash collisions
```

**Success Criteria:**
- ✅ Proposals distributed across multiple groups
- ✅ Average split percent ≈ 100%
- ✅ All hierarchies linked to split participants
- ✅ No ConfigHash duplicates

---

## Phase 2: Medium Dataset Testing

### Step 2.1: Run with 10K Certificates

```bash
# Run builder with 10K certificate limit
npx tsx scripts/proposal-builder.ts --limit 10000 --verbose

# Monitor output for:
# - Memory usage (should stay < 2GB)
# - Execution time (should be < 5 minutes)
# - Hash collisions (MUST be zero)
```

**Success Criteria:**
- ✅ Completes in < 5 minutes
- ✅ Memory < 2GB
- ✅ Hash collisions = 0
- ✅ No errors

### Step 2.2: Run Medium Sample Validation

```bash
# Run medium sample validation (200 certificates)
npm run validate-certificates -- --sample medium

# Expected:
# - Pass rate >= 95%
# - Edge cases covered (DTC, multi-split, complex hierarchies)
```

**Success Criteria:**
- ✅ Pass rate >= 95%
- ✅ Edge cases handled correctly
- ✅ Performance acceptable

### Step 2.3: Test Batched Mode

```bash
# Test batching with 2K batch size
npx tsx scripts/proposal-builder.ts --limit 10000 --batch-size 2000 --verbose

# Verify:
# - Batches processed: 5
# - Output consistent with single-pass mode
```

**Success Criteria:**
- ✅ All batches complete
- ✅ Results consistent with single-pass

---

## Phase 3: Full Dataset Testing

### Step 3.1: Run Full Pipeline with TypeScript Builder

```bash
# Run FULL pipeline with TypeScript builder (no limit)
npm run pipeline:ts -- --skip-export

# Monitor:
# - Execution time (~10-15 minutes)
# - Memory usage (should stay < 4GB with batching)
# - Final counts
```

**Success Criteria:**
- ✅ Completes without errors
- ✅ Memory < 4GB
- ✅ Time < 20 minutes
- ✅ Hash collisions = 0

### Step 3.2: Run Large Sample Validation

```bash
# Run large sample validation (1000 certificates)
npm run validate-certificates -- --sample large

# This will take ~10-15 minutes
```

**Success Criteria:**
- ✅ Pass rate >= 95%
- ✅ Statistical confidence achieved

### Step 3.3: Compare Entity Counts

```sql
-- Get TypeScript builder counts
SELECT 
  'Proposals' as entity,
  COUNT(*) as ts_count,
  (SELECT COUNT(*) FROM etl.stg_proposals_sql) as sql_count,
  COUNT(*) - (SELECT COUNT(*) FROM etl.stg_proposals_sql) as diff
FROM etl.stg_proposals
UNION ALL
SELECT 
  'Hierarchies',
  COUNT(*),
  (SELECT COUNT(*) FROM etl.stg_hierarchies_sql),
  COUNT(*) - (SELECT COUNT(*) FROM etl.stg_hierarchies_sql)
FROM etl.stg_hierarchies
UNION ALL
SELECT 
  'Split Versions',
  COUNT(*),
  (SELECT COUNT(*) FROM etl.stg_premium_split_versions_sql),
  COUNT(*) - (SELECT COUNT(*) FROM etl.stg_premium_split_versions_sql)
FROM etl.stg_premium_split_versions;

-- Differences within ±5% are acceptable
-- TypeScript should generate FEWER hierarchies (better deduplication)
```

**Success Criteria:**
- ✅ Counts within ±5% tolerance
- ✅ Hierarchy count <= SQL count (better deduplication)
- ✅ No missing entities

---

## Phase 4: Edge Case Testing

### Test Case 1: DTC Policies (No GroupId)

```sql
-- Find DTC certificate
SELECT TOP 1 CertificateId
FROM etl.input_certificate_info
WHERE GroupId IS NULL OR LTRIM(RTRIM(GroupId)) = ''
  AND CertStatus IN ('A', 'Active');

-- Run builder
npx tsx scripts/proposal-builder.ts --limit 100 --verbose

-- Verify in PHA table
SELECT *
FROM etl.stg_policy_hierarchy_assignments
WHERE NonConformantReason LIKE '%Invalid GroupId%';
```

**Expected:** DTC policies routed to PHA, not proposals

### Test Case 2: Multi-Split Configuration

```sql
-- Find multi-split certificate
SELECT TOP 1 CertificateId
FROM etl.input_certificate_info
WHERE CertSplitSeq > 1
GROUP BY CertificateId
HAVING COUNT(DISTINCT CertSplitSeq) > 1;

-- Verify split configuration
SELECT 
  p.Id as ProposalId,
  psv.TotalSplitPercent,
  COUNT(psp.Id) as split_count
FROM etl.stg_proposals p
JOIN etl.stg_premium_split_versions psv ON psv.ProposalId = p.Id
JOIN etl.stg_premium_split_participants psp ON psp.VersionId = psv.Id
WHERE p.GroupId = (
  SELECT GroupId FROM etl.input_certificate_info WHERE CertificateId = '<cert-id>'
)
GROUP BY p.Id, psv.TotalSplitPercent;
```

**Expected:** Multiple splits with correct percentages

### Test Case 3: Complex Hierarchy (3+ Tiers)

```sql
-- Find complex hierarchy
SELECT TOP 1 CertificateId, SplitBrokerId
FROM etl.input_certificate_info
WHERE SplitBrokerSeq >= 3
GROUP BY CertificateId, SplitBrokerId;

-- Verify hierarchy participants
SELECT 
  hp.Level,
  hp.EntityId,
  hp.EntityName,
  hp.ScheduleCode
FROM etl.stg_hierarchy_participants hp
JOIN etl.stg_hierarchy_versions hv ON hv.Id = hp.HierarchyVersionId
JOIN etl.stg_hierarchies h ON h.Id = hv.HierarchyId
WHERE h.BrokerId = (
  SELECT CAST(SplitBrokerId AS BIGINT) 
  FROM etl.input_certificate_info 
  WHERE CertificateId = '<cert-id>' AND SplitBrokerSeq = 1
)
ORDER BY hp.Level;
```

**Expected:** All tiers present in correct order

### Test Case 4: Date Range Expansion

```sql
-- Find group with wide date range
SELECT 
  GroupId,
  MIN(CertEffectiveDate) as min_date,
  MAX(CertEffectiveDate) as max_date,
  COUNT(DISTINCT YEAR(CertEffectiveDate)) as year_count
FROM etl.input_certificate_info
WHERE GroupId IS NOT NULL
GROUP BY GroupId
HAVING COUNT(DISTINCT YEAR(CertEffectiveDate)) > 3
ORDER BY year_count DESC;

-- Verify date range expansion
SELECT 
  p.GroupId,
  p.DateRangeFrom,
  p.DateRangeTo,
  p.EffectiveDateFrom,
  p.EffectiveDateTo,
  p.DateRangeTo - p.DateRangeFrom + 1 as year_span
FROM etl.stg_proposals p
WHERE p.GroupId = '<group-id>';
```

**Expected:** Proposal date range spans all certificate years

---

## Troubleshooting

### Issue: Hash Collisions Detected

```
❌ Hash collision detected for config-CERT123: ABC123...
```

**Solution:** This is a CRITICAL error. Full SHA256 should never collide. Investigate the input data for duplicates.

### Issue: Memory Usage Exceeds 4GB

**Solution:** Enable batched processing mode:
```bash
npx tsx scripts/proposal-builder.ts --batch-size 5000
```

### Issue: Validation Pass Rate < 95%

**Solution:** Review failed certificates:
```bash
npm run validate-certificates -- --sample medium | tee validation-report.txt
```

Analyze common failure patterns in the report.

### Issue: Foreign Key Violations

**Solution:** Check staging table order and ensure all parent entities created before children.

---

## Performance Benchmarks

| Dataset Size | Expected Time | Expected Memory |
|--------------|---------------|-----------------|
| 100 certs    | < 5 seconds   | < 500MB         |
| 10K certs    | < 2 minutes   | < 1GB           |
| 100K certs   | < 10 minutes  | < 2GB           |
| 150K certs (full active) | < 15 minutes  | < 2.5GB     |

**Note:** Active certificates (CertStatus='A' AND RecStatus='A') are ~150K. Times may vary based on server performance and data complexity.

---

## Reporting Results

After completing all test phases, generate a summary report:

```bash
# Run all validations and collect results
echo "=== Small Sample ===" > test-report.txt
npm run validate-certificates -- --sample small >> test-report.txt 2>&1

echo "=== Medium Sample ===" >> test-report.txt
npm run validate-certificates -- --sample medium >> test-report.txt 2>&1

echo "=== Large Sample ===" >> test-report.txt
npm run validate-certificates -- --sample large >> test-report.txt 2>&1

# Review report
cat test-report.txt
```

Share `test-report.txt` with the team for approval before production rollout.

---

## Next Steps After Testing

1. **If all tests pass (>= 95% pass rate):**
   - Document test results
   - Get team approval
   - Plan production rollout (shadow mode → validation mode → staged rollout)

2. **If tests fail (< 95% pass rate):**
   - Review failure patterns
   - Fix identified issues
   - Re-run failed test phase
   - Repeat until pass rate >= 95%

3. **For production rollout:**
   - Start with shadow mode (run both SQL and TS, compare)
   - Move to validation mode (TS active, SQL for comparison)
   - Staged rollout (dev → staging → production)
   - Monitor metrics and rollback if needed

---

## Support

For issues or questions:
- Check logs: `[AUDIT]` entries in console output
- Review validation reports
- Check `.cursorrules` for troubleshooting guidance
