# Group Conformance Filtering

## Overview

The ETL pipeline now includes **automatic conformance analysis** that measures data quality and filters exports to ensure only high-quality data reaches production.

## Problem Solved

Previously, the ETL would export **all** groups, including those with missing or incorrect proposal mappings. This led to:
- Commission calculation failures
- Manual data cleanup
- Production data quality issues

## Solution

**Automated conformance analysis** that:
1. Analyzes certificate-to-proposal mapping accuracy
2. Classifies groups by conformance level
3. Filters exports to only include conformant/nearly conformant groups

## Conformance Levels

| Level | Criteria | Typical % | Export Decision |
|-------|----------|-----------|-----------------|
| **Conformant** | 100% of certificates map to exactly one proposal | ~72% of groups | ✅ **EXPORT** |
| **Nearly Conformant** | >=95% of certificates map correctly | ~1% of groups | ✅ **EXPORT** |
| **Non-Conformant** | <95% conformance | ~27% of groups | ❌ **SKIP** |

## System Performance

Based on real data analysis:

- **Total Groups Analyzed:** 1,407
- **Conformant (100%):** 1,012 groups (72%)
- **Nearly Conformant (>=95%):** 8 groups (0.6%)
- **Non-Conformant (<95%):** 387 groups (27%)
- **Overall Certificate Conformance:** 94.62%

### Export Impact

- **WILL EXPORT:** 1,020 groups (72.6%)
- **WILL SKIP:** 387 groups (27.4%)

## How It Works

### 1. Schema Setup (`03b-conformance-table.sql`)

Creates `GroupConformanceStatistics` table to store analysis results:

```sql
CREATE TABLE [etl].[GroupConformanceStatistics] (
    GroupId NVARCHAR(100) NOT NULL PRIMARY KEY,
    GroupName NVARCHAR(500),
    SitusState NVARCHAR(10),
    TotalCertificates INT NOT NULL,
    ConformantCertificates INT NOT NULL,
    NonConformantCertificates INT NOT NULL,
    ConformancePercentage DECIMAL(5,2) NOT NULL,
    GroupClassification NVARCHAR(50) NOT NULL,
    AnalysisDate DATETIME2
);
```

### 2. Analysis Phase (`08-analyze-conformance.sql`)

Runs after transform phase, before export:

1. **Deduplicate Certificates** - Uses `UNION` (not `UNION ALL`) to remove duplicates across `cert_split_configs_remainder2` and `remainder3`
2. **Map to Proposals** - Joins to `stg_proposal_key_mapping` using (GroupId, EffectiveYear, ProductCode, PlanCode)
3. **Classify Certificates** - Each certificate is:
   - **Conformant**: Maps to exactly 1 proposal
   - **Non-Conformant (No Match)**: Maps to 0 proposals
   - **Non-Conformant (Multiple Matches)**: Maps to >1 proposals
4. **Aggregate by Group** - Calculates conformance percentage
5. **Store Results** - Inserts into `GroupConformanceStatistics`

### 3. Export Filtering

All export scripts filter using conformance:

**Groups Export** (`05-export-groups.sql`):
```sql
FROM [etl].[stg_groups] sg
INNER JOIN [etl].[GroupConformanceStatistics] gcs
    ON gcs.GroupId = sg.Id
    AND gcs.GroupClassification IN ('Conformant', 'Nearly Conformant (>=95%)')
```

**Proposals Export** (`07-export-proposals.sql`):
```sql
FROM [etl].[stg_proposals] sp
INNER JOIN [etl].[GroupConformanceStatistics] gcs
    ON gcs.GroupId = sp.GroupId
    AND gcs.GroupClassification IN ('Conformant', 'Nearly Conformant (>=95%)')
```

**Policies Export** (`09-export-policies.sql`):
```sql
WHERE (
    sp.GroupId IN (
      SELECT GroupId 
      FROM [etl].[GroupConformanceStatistics]
      WHERE GroupClassification IN ('Conformant', 'Nearly Conformant (>=95%)')
    )
    OR sp.GroupId IS NULL  -- Always export DTC (Direct-to-Consumer) policies
    OR sp.GroupId = ''
  )
```

**Hierarchies Export** (`08-export-hierarchies.sql`):
```sql
FROM [etl].[stg_hierarchies] sh
INNER JOIN [etl].[GroupConformanceStatistics] gcs
    ON gcs.GroupId = sh.GroupId
    AND gcs.GroupClassification IN ('Conformant', 'Nearly Conformant (>=95%)')
```

## Usage

### Run Pipeline with Conformance Analysis

```bash
cd tools/v5-etl
npx tsx scripts/run-pipeline.ts
```

Conformance analysis runs automatically during transform phase.

### View Conformance Results

```sql
-- Summary statistics
SELECT 
    GroupClassification,
    COUNT(*) AS GroupCount,
    SUM(TotalCertificates) AS TotalCerts,
    AVG(ConformancePercentage) AS AvgConformance
FROM [etl].[GroupConformanceStatistics]
GROUP BY GroupClassification
ORDER BY GroupClassification;

-- Top non-conformant groups (for investigation)
SELECT TOP 20
    GroupId,
    GroupName,
    TotalCertificates,
    ConformantCertificates,
    NonConformantCertificates,
    ConformancePercentage
FROM [etl].[GroupConformanceStatistics]
WHERE GroupClassification = 'Non-Conformant'
ORDER BY TotalCertificates DESC;

-- Specific group detail
SELECT * 
FROM [etl].[GroupConformanceStatistics]
WHERE GroupId = 'G21254';
```

### Example: G21254 Analysis

**Before Fix (UNION ALL - with duplicates):**
- Classification: ❌ Non-Conformant
- Total Certificates: 443
- Conformant: 339 (76.52%)
- Issue: 104 certificates duplicated across source tables

**After Fix (UNION - deduplicated):**
- Classification: ✅ Conformant
- Total Certificates: 443
- Conformant: 443 (100.00%)
- Result: All certificates map correctly to one proposal

## Pipeline Integration

### Transform Scripts Order

```typescript
const transformScripts = [
  '00-references.sql',
  '01-brokers.sql',
  '02-groups.sql',
  '03-products.sql',
  '04-schedules.sql',
  '06a-proposals-simple-groups.sql',
  '06b-proposals-non-conformant.sql',
  '06c-proposals-plan-differentiated.sql',
  '06d-proposals-year-differentiated.sql',
  '06e-proposals-granular.sql',
  '06f-populate-prestage-split-configs.sql',
  '06g-normalize-proposal-date-ranges.sql',
  '06z-update-proposal-broker-names.sql',
  '07-hierarchies.sql',
  '08-analyze-conformance.sql',  // ← NEW: Conformance analysis
  '08-hierarchy-splits.sql',
  '09-policies.sql',
  '10-premium-transactions.sql',
  '11-policy-hierarchy-assignments.sql',
  '99-audit-and-cleanup.sql',
];
```

## Benefits

1. **Data Quality Gate** - Only high-quality data reaches production
2. **Automatic Filtering** - No manual intervention required
3. **Audit Trail** - `GroupConformanceStatistics` table preserves analysis
4. **Transparency** - Clear classification and metrics per group
5. **Performance** - Reduces production data volume by ~27%
6. **Reliability** - Commission calculations work on conformant data

## Files Modified

| File | Purpose | Change |
|------|---------|--------|
| `sql/03b-conformance-table.sql` | Schema | Created `GroupConformanceStatistics` table |
| `sql/transforms/08-analyze-conformance.sql` | Transform | Performs conformance analysis |
| `scripts/run-pipeline.ts` | Pipeline | Added conformance table schema and analysis transform |
| `sql/export/05-export-groups.sql` | Export | Filter by conformance |
| `sql/export/07-export-proposals.sql` | Export | Filter by conformance |
| `sql/export/08-export-hierarchies.sql` | Export | Filter by conformance |
| `sql/export/09-export-policies.sql` | Export | Filter by conformance (+ DTC) |
| `sql/analysis/group-conformance-analysis.sql` | Analysis | Fixed UNION ALL → UNION bug |
| `README.md` | Documentation | Added conformance section |

## Troubleshooting

### "Why is my group not exporting?"

Check conformance status:

```sql
SELECT * 
FROM [etl].[GroupConformanceStatistics]
WHERE GroupId = 'GYourGroupId';
```

If `Non-Conformant`, investigate why certificates don't map to proposals.

### "How do I export non-conformant groups anyway?"

Temporarily remove conformance filter from export scripts (not recommended for production):

```sql
-- Comment out the conformance filter
-- INNER JOIN [etl].[GroupConformanceStatistics] gcs
--     ON gcs.GroupId = sg.Id
--     AND gcs.GroupClassification IN ('Conformant', 'Nearly Conformant (>=95%)')
```

### "Conformance percentage dropped unexpectedly"

Check for:
1. Missing proposals in `stg_proposal_key_mapping`
2. Certificate duplicates across source tables
3. Product/plan code mismatches

## Future Enhancements

Potential improvements:
- Email alerts for conformance drops
- Per-product conformance metrics
- Automatic proposal creation for non-conformant groups
- Conformance trending over time
