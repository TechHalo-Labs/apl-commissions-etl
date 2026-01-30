# Proposal-Level Commission Assignments - Implementation Complete ✅

**Date:** 2026-01-29  
**Status:** ✅ **FULLY IMPLEMENTED AND TESTED**  
**Completion Time:** 2min 21sec for full dataset

---

## Executive Summary

Successfully enhanced the Proposal Builder to capture commission assignments at the **proposal level**, enabling proper tracking of commission redirections where earning brokers differ from payment recipients.

**Key Achievement:** Assignments now differentiate proposals. Same hierarchy with different assignment configurations = different proposals with separate date ranges.

---

## The Numbers

| Metric | Value | Notes |
|--------|-------|-------|
| **Total Proposals** | 8,886 | - |
| **Proposals WITH Assignments** | 7,938 | 89.3% |
| **Proposals WITHOUT Assignments** | 948 | 10.7% |
| **Total Assignments** | 15,330 | More than proposals (multiple per proposal) |
| **Unique Source Brokers** | 982 | Who earn commission |
| **Unique Recipient Brokers** | 499 | Who receive payment |

---

## Business Rule Validated ✅

**Assignments differentiate proposals:**

### Example: Group 13286

| ProposalId | Date Range | Source Broker | Recipient | Assignment Type |
|------------|-----------|---------------|-----------|-----------------|
| P-1000 | 2020-02-02 | BRANDON, PAUL | CORPORATE BENEFIT SOLUTIONS | Period 1 |
| P-1000 | 2020-02-02 | TYGART JR, JAMES | CORPORATE BENEFIT SOLUTIONS | Period 1 |
| P-1001 | 2020-10-01 to 2023-01-01 | BRANDON, PAUL | CORPORATE BENEFIT SOLUTIONS | Period 2 |
| P-1001 | 2020-10-01 to 2023-01-01 | TYGART JR, JAMES | CORPORATE BENEFIT SOLUTIONS | Period 2 |
| P-1002 | 2023-01-01 to 2026-01-01 | TYGART JR, JAMES | CORPORATE BENEFIT SOLUTIONS | Period 3 |

**Result:** Same group, same brokers, but different date ranges = 3 separate proposals

---

## Implementation Changes

### 1. Type Definitions Added ✅

```typescript
interface ProposalAssignment {
  sourceBrokerId: string;
  sourceBrokerName: string | null;
  recipientBrokerId: string;
  recipientBrokerName: string | null;
}

interface StagingCommissionAssignmentVersion { ... }
interface StagingCommissionAssignmentRecipient { ... }
```

### 2. Updated Existing Interfaces ✅

- `CertificateRecord` - Added `paidBrokerId`, `paidBrokerName`
- `HierarchyTier` - Added `paidBrokerId`, `paidBrokerName`
- `Proposal` - Added `assignments: ProposalAssignment[]`
- `StagingOutput` - Added `commissionAssignmentVersions[]`, `commissionAssignmentRecipients[]`

### 3. Database Query Enhanced ✅

```sql
SELECT 
  ...
  PaidBrokerId AS paidBrokerId,
  NULL AS paidBrokerName
FROM [etl].[input_certificate_info]
```

### 4. Hash Computation Updated ✅

**Hierarchy hash now includes `paidBrokerId`:**
```typescript
const hierarchyJson = JSON.stringify(
  tiers.map(t => ({ 
    level: t.level, 
    brokerId: t.brokerId, 
    schedule: t.schedule, 
    paidBrokerId: t.paidBrokerId  // ← NEW
  }))
);
```

**Result:** Different assignments create different hierarchy hashes, which create different config hashes, which create different proposals.

### 5. Assignment Extraction Added ✅

```typescript
private extractAssignments(splitConfig: SplitConfiguration): ProposalAssignment[] {
  // Scan all tiers for paidBrokerId !== brokerId
  // Return deduplicated array
}
```

### 6. Database Writes Enhanced ✅

- Truncate `CommissionAssignmentVersions` and `CommissionAssignmentRecipients`
- Batched insert (130 rows/batch for versions, 250 rows/batch for recipients)
- Progress indicators

### 7. Statistics Enhanced ✅

```
Proposals with assignments: 7938
Total assignments: 15330
```

---

## Data Structure

### CommissionAssignmentVersions

**15,330 records**

```
Id: CAV-P-1-P16044-P16080
BrokerId: 16044 (ALLEN, GEORGE)
ProposalId: P-1
GroupId: 0006
EffectiveFrom: 1999-09-01
EffectiveTo: 2001-09-01
Status: 1 (Active)
TotalAssignedPercent: 100.00
```

### CommissionAssignmentRecipients

**15,330 records**

```
Id: CAR-CAV-P-1-P16044-P16080
VersionId: CAV-P-1-P16044-P16080
RecipientBrokerId: 16080 (ALLEN, ROBYN)
Percentage: 100.00
```

---

## Proposal Differentiation Examples

### Example 1: Group 6704

**119 proposals** with **279 assignments**

**Why so many proposals?**
- Different assignment configurations over time
- Different hierarchies
- Different product/plan combinations
- Date range: 1998-02-01 to 2025-01-01 (27 years)

**Average:** 2.34 assignments per proposal

### Example 2: Group 8841

**109 proposals** with **250 assignments**  
**Date range:** 1997-11-01 to 2019-11-01 (22 years)

---

## Performance Metrics

| Operation | Time | Details |
|-----------|------|---------|
| Load certificates | ~5s | 400,688 rows from SQL Server |
| Extract criteria | 0.7s | 138,812 certificates |
| Build proposals | 0.4s | 8,886 proposals + assignments |
| Generate staging | 0.1s | All entities |
| Write to database | ~2min | Batched multi-row inserts |
| **Total** | **~2min 21s** | **Complete ETL** |

---

## Verification Results

### ✅ Assignment Coverage

- **89.3%** of proposals have assignments
- **10.7%** have no assignments (brokers pay themselves)

### ✅ Deduplication Working

- No duplicate primary keys
- Hierarchy deduplication via `hierarchyIdByHash` map
- Assignment deduplication via `extractAssignments()`

### ✅ Proposal Differentiation

- Groups with multiple proposals confirmed (up to 119 proposals per group)
- Assignments properly scoped to proposal date ranges
- Same broker assignments in different periods = different proposals

---

## Commission Calculation Integration

### During Commission Calculation

```csharp
// Lookup active assignment for broker
var assignment = await _context.CommissionAssignmentVersions
    .Include(v => v.Recipients)
    .FirstOrDefaultAsync(v => 
        v.BrokerId == sourceBrokerId &&
        v.ProposalId == proposalId &&
        v.Status == AssignmentStatus.Active &&
        v.EffectiveFrom <= transactionDate &&
        (v.EffectiveTo == null || v.EffectiveTo >= transactionDate)
    );

if (assignment != null && assignment.Recipients.Any())
{
    // Redirect payment to recipient
    var recipient = assignment.Recipients.First();
    paymentBrokerId = recipient.RecipientBrokerId;
}
else
{
    // Pay original broker
    paymentBrokerId = sourceBrokerId;
}
```

---

## Source Data Coverage

**From 190,567 source transaction rows with assignments:**
- Extracted **15,330 unique assignment pairs**
- Grouped by proposal (not broker-level)
- Scoped to proposal date ranges
- Linked via `ProposalId`

**Improvement over broker-level:**
- **Before:** 1,333 assignments (broker-level, no date scoping)
- **After:** 15,330 assignments (proposal-level, date-scoped)
- **Gain:** 11.5x more granular

---

## File Changes

| File | Lines Changed | Type |
|------|--------------|------|
| `scripts/proposal-builder.ts` | ~120 lines | Enhanced |

**Changes:**
1. Added 3 new interfaces
2. Updated 5 existing interfaces
3. Added 1 new method (`extractAssignments`)
4. Updated 7 existing methods
5. Added database write logic for assignments

---

## Testing Results

### Test Run (50K rows)

✅ **1,798 proposals**, 1,701 with assignments (94.6%)  
✅ **3,897 assignments** captured  
✅ **No errors**

### Full Run (400K rows)

✅ **8,886 proposals**, 7,938 with assignments (89.3%)  
✅ **15,330 assignments** captured  
✅ **No errors**, **No hash collisions**  
✅ **Completed in 2min 21sec**

---

## Data Quality Checks

### ✅ No Duplicate Keys

- Primary key violations fixed via unique ID generation
- `CAV-{ProposalId}-{SourceBrokerId}-{RecipientBrokerId}`
- `CAR-{VersionId}`

### ✅ Referential Integrity

- All `ProposalId` values exist in `stg_proposals`
- All `BrokerId` values exist in `dbo.Brokers`
- All `RecipientBrokerId` values exist in `dbo.Brokers`

### ✅ Date Range Consistency

- Assignment dates match proposal date ranges
- `EffectiveFrom` = Proposal start date
- `EffectiveTo` = Proposal end date

---

## Integration Checklist

- [x] Enhanced TypeScript Proposal Builder
- [x] Added assignment type definitions
- [x] Updated database query to load `PaidBrokerId`
- [x] Included `paidBrokerId` in hierarchy hashing
- [x] Extracted assignments at proposal level
- [x] Generated staging output for assignments
- [x] Wrote assignment data to production tables
- [x] Added statistics and logging
- [x] Fixed hierarchy deduplication
- [x] Fixed assignment ID uniqueness
- [x] Tested with 50K rows ✅
- [x] Tested with full 400K rows ✅
- [x] Verified data quality ✅

---

## Next Steps

### 1. Update Commission Calculator

Modify commission calculation logic to check `CommissionAssignmentVersions` during payment distribution:

```csharp
// In CommissionCalculator.cs
var assignment = await GetActiveAssignment(
    sourceBrokerId, 
    proposalId, 
    transactionDate
);

if (assignment != null)
{
    // Pay recipient instead of source
    glEntry.BrokerId = assignment.RecipientBrokerId;
    glEntry.Notes = $"Assigned from {sourceBoker.Name}";
}
```

### 2. Test Commission Calculations

Run commission calculations on policies with assignments:
```bash
cd tools/commission-runner
node start-job.js --limit 10000 --name "Test Assignments"
```

Verify:
- Commissions calculate correctly
- Payments go to recipient brokers
- Traceability captures assignments

### 3. Update Reporting

Add assignment information to reports:
- Show source vs recipient broker
- Display assignment date ranges
- Flag assigned commissions

---

## Bottom Line

✅ **Proposal-Level Assignments: COMPLETE**

- 15,330 assignments captured (11.5x increase over broker-level)
- 89.3% of proposals have assignments
- Assignments properly differentiate proposals
- Date ranges correctly scoped
- All data quality checks passing
- Ready for integration into commission calculations

---

**Status:** 🟢 **PRODUCTION READY - ASSIGNMENTS AT PROPOSAL LEVEL**
