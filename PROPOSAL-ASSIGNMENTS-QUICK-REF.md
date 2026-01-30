# Proposal-Level Assignments - Quick Reference

**Status:** ✅ **COMPLETE**  
**Performance:** 2min 21sec for 400K rows  
**Coverage:** 89.3% of proposals

---

## What Changed

✅ **Before:** Broker-level assignments (1,333 global assignments)  
✅ **After:** Proposal-level assignments (15,330 scoped to proposals)  
✅ **Benefit:** 11.5x more granular, historically accurate

---

## Key Numbers

| Metric | Value |
|--------|-------|
| Proposals | 8,886 |
| With Assignments | 7,938 (89.3%) |
| Total Assignments | 15,330 |
| Source Brokers | 982 |
| Recipients | 499 |

---

## How It Works

### 1. Assignments Included in Hash

```typescript
// Hierarchy hash NOW includes paidBrokerId
const hierarchyJson = JSON.stringify(
  tiers.map(t => ({ 
    level, brokerId, schedule,
    paidBrokerId  // ← NEW!
  }))
);
```

**Result:** Different assignments = Different hash = Different proposal

---

### 2. Assignment Detection

```typescript
// Extract assignments from hierarchy
if (tier.paidBrokerId && tier.paidBrokerId !== tier.brokerId) {
  assignments.push({
    sourceBrokerId: tier.brokerId,    // Who earns
    recipientBrokerId: tier.paidBrokerId  // Who receives
  });
}
```

---

### 3. Output Generation

```typescript
// For each proposal with assignments:
for (const assignment of proposal.assignments) {
  // Create CommissionAssignmentVersion
  Id: `CAV-${proposalId}-${sourceBrokerId}-${recipientBrokerId}`
  ProposalId: proposalId
  BrokerId: sourceBrokerId
  EffectiveFrom: proposal.effectiveDateFrom
  EffectiveTo: proposal.effectiveDateTo
  
  // Create CommissionAssignmentRecipient
  VersionId: above CAV Id
  RecipientBrokerId: recipientBrokerId
  Percentage: 100.00
}
```

---

## Real Example: Group 13286

**19 proposals over 18 years** (2008-2026)

### Why so many?

1. **Assignment changes:**
   - 2008-2020: O'KEEFE + TYGART
   - 2020+: BRANDON + TYGART
   - 2023+: Only TYGART

2. **Product changes:**
   - Different product mixes over time

3. **Date-specific configurations:**
   - Some proposals cover just one day
   - Others span multiple years

### Sample Timeline

```
2008-2020  │ P-987  │ O'KEEFE→GALLAGHER + TYGART→CORPORATE
2008-2021  │ P-988  │ BRANDON→CORPORATE + TYGART→CORPORATE
2020-02-02 │ P-1000 │ BRANDON→CORPORATE + TYGART→CORPORATE  (1 day!)
2020-2023  │ P-1001 │ BRANDON→CORPORATE + TYGART→CORPORATE
2023-2026  │ P-1002 │ TYGART→CORPORATE (only)
2023-2026  │ P-1003 │ COSTA→GALLAGHER + TYGART→CORPORATE
```

**Each has different:**
- ConfigHash
- Date range
- Assignment configuration

---

## Commission Calculation

### Query Pattern

```csharp
// During commission calculation for a transaction:
var assignment = await _context.CommissionAssignmentVersions
    .Include(v => v.Recipients)
    .FirstOrDefaultAsync(v => 
        v.ProposalId == proposalId &&        // ← Scoped to proposal
        v.BrokerId == sourceBrokerId &&
        v.Status == 1 &&
        v.EffectiveFrom <= transactionDate &&
        (v.EffectiveTo == null || v.EffectiveTo >= transactionDate)
    );

if (assignment != null)
{
    // Redirect payment
    glEntry.BrokerId = assignment.Recipients.First().RecipientBrokerId;
    glEntry.OriginalBrokerId = sourceBrokerId;
    glEntry.IsAssigned = true;
}
```

---

## Verification Queries

### Check Proposal Coverage

```sql
SELECT 
    COUNT(*) as total_proposals,
    COUNT(DISTINCT v.ProposalId) as with_assignments,
    CAST(COUNT(DISTINCT v.ProposalId) AS FLOAT) / COUNT(*) * 100 as percent
FROM etl.stg_proposals p
LEFT JOIN dbo.CommissionAssignmentVersions v ON v.ProposalId = p.Id;
```

**Expected:** ~89% coverage

---

### Find Groups with Most Proposals

```sql
SELECT TOP 10
    p.GroupId,
    COUNT(DISTINCT p.Id) as proposal_count,
    COUNT(v.Id) as assignment_count
FROM etl.stg_proposals p
LEFT JOIN dbo.CommissionAssignmentVersions v ON v.ProposalId = p.Id
GROUP BY p.GroupId
ORDER BY proposal_count DESC;
```

**Top groups:** 100+ proposals due to assignment changes

---

### Check Assignment for Specific Proposal

```sql
SELECT 
    bs.Name as SourceBroker,
    br.Name as RecipientBroker,
    v.EffectiveFrom,
    v.EffectiveTo
FROM dbo.CommissionAssignmentVersions v
JOIN dbo.CommissionAssignmentRecipients r ON r.VersionId = v.Id
LEFT JOIN dbo.Brokers bs ON bs.Id = v.BrokerId
LEFT JOIN dbo.Brokers br ON br.Id = r.RecipientBrokerId
WHERE v.ProposalId = 'P-1001';
```

---

## Files Modified

| File | Purpose |
|------|---------|
| `scripts/proposal-builder.ts` | Enhanced with assignment support |
| `PROPOSAL-LEVEL-ASSIGNMENTS-SUCCESS.md` | Technical documentation |
| `PROPOSAL-LEVEL-ASSIGNMENTS-EXAMPLE.md` | Real-world example |
| `PROPOSAL-ASSIGNMENTS-QUICK-REF.md` | This file |

---

## Run Commands

### Test Run (50K rows)
```bash
cd /Users/kennpalm/Downloads/source/APL/apl-commissions-etl
npx tsx scripts/proposal-builder.ts --limit 50000 --verbose
```

### Full Run (400K rows)
```bash
npx tsx scripts/proposal-builder.ts --verbose
```

**Expected Output:**
```
Proposals created: ~8,886
Proposals with assignments: ~7,938 (89%)
Total assignments: ~15,330
Completed in ~2min 21sec
```

---

## Next Steps

1. **Update Commission Calculator**
   - Add assignment lookup logic
   - Redirect payments to recipients
   - Log assignments in traceability

2. **Test Commissions**
   ```bash
   cd tools/commission-runner
   node start-job.js --limit 10000
   ```

3. **Verify**
   - Commissions calculate correctly
   - Payments go to right brokers
   - Traceability shows assignments

---

**Status:** 🟢 **READY FOR COMMISSION CALCULATIONS**
