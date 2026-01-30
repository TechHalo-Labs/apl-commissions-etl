# Commission Assignments - ETL Complete ✅

**Date:** 2026-01-29  
**Status:** ✅ **FULLY IMPLEMENTED AND TESTED**

---

## Executive Summary

Successfully implemented ETL to capture **1,333 commission assignments** where brokers redirect their commissions to other parties (e.g., agencies, successors, or new business entities).

---

## The Business Case

**Commission Assignment** = When a broker's earned commission is paid to a different entity.

**Common Scenarios:**
- Broker retires → Assigns commissions to successor
- Individual broker → Forms LLC → Assigns to LLC
- Broker → Assigns to their agency/office
- Terminated broker → Commissions redirected

**Source Data Logic:**
```sql
-- Assignment exists when:
SplitBrokerId != PaidBrokerId

-- Example:
SplitBrokerId: P17161 (PEACE, RICHARD)      ← Should earn commission
PaidBrokerId:  P12841 (FINANCIAL BENEFIT..) ← Actually receives payment
```

---

## Implementation Details

### Transform Script
**File:** `sql/transforms/12-commission-assignments.sql`

**Logic:**
1. Extract unique `(SplitBrokerId, PaidBrokerId)` pairs from `CertificateInfo`
2. Create one `CommissionAssignmentVersion` per pair
3. Create one `CommissionAssignmentRecipient` per version
4. Calculate effective date ranges

### Export Script
**File:** `sql/export/13-export-commission-assignments.sql`

**Process:**
1. Clear existing assignment data
2. Export versions to `dbo.CommissionAssignmentVersions`
3. Export recipients to `dbo.CommissionAssignmentRecipients`
4. Validate counts

---

## Production Data

### CommissionAssignmentVersions

**Total:** 1,333 versions

**Key Fields:**
| Field | Purpose | Example |
|-------|---------|---------|
| `Id` | Unique version ID | `CAV-17161-12841` |
| `BrokerId` | Source broker (who earned) | `17161` |
| `BrokerName` | Source broker name | `PEACE, RICHARD` |
| `ProposalId` | Context (currently `BROKER-LEVEL`) | `BROKER-LEVEL` |
| `EffectiveFrom` | When assignment started | `2010-09-01` |
| `EffectiveTo` | When assignment ended | `NULL` (still active) |
| `TotalAssignedPercent` | % assigned | `100.00` (full redirect) |

**Active vs Inactive:**
- **417 still active** (EffectiveTo = NULL)
- **916 ended** (EffectiveTo set)

---

### CommissionAssignmentRecipients

**Total:** 1,333 recipients

**Key Fields:**
| Field | Purpose | Example |
|-------|---------|---------|
| `Id` | Unique recipient ID | `CAR-17161-12841` |
| `VersionId` | Links to version | `CAV-17161-12841` |
| `RecipientBrokerId` | Who receives payment | `12841` |
| `RecipientName` | Recipient name | `FINANCIAL BENEFIT SERVICES LLC` |
| `Percentage` | % they receive | `100.00` (full redirect) |

---

## Source Data Coverage

### Assignments in Source

**Total Transaction Rows with Assignments:** 190,567  
**Unique Assignment Pairs:** 1,333  

**Source Brokers:** 1,315 unique brokers have their commissions assigned  
**Recipient Brokers:** 521 unique brokers/entities receive assigned commissions

---

## Top 10 Largest Assignments

| Source Broker | Recipient | Affected Certificates | Status |
|---------------|-----------|----------------------|--------|
| PUERTA, KAREN | BENEFIT GUARANTY LLC | 34,705 | Active |
| DZEIMA, JOHN | CALSTAR FINANCIAL | 11,392 | Active |
| SUTHERLAND, JOSEPH | CAPITOL GROUP HEALTH | 7,516 | Active |
| B AND B INSURANCE | MASS GROUP MARKETING | 7,490 | Active |
| COLLINS JR, DAVID | IQ INSURE | 6,370 | Active |
| NASTASI, JASON | CRESCENT CITY BENEFITS | 5,117 | Active |
| PATUREAU, BRIAN | BRIAN PATUREAU LLC | 5,107 | Active |
| PEACE, RICHARD | FINANCIAL BENEFIT SERVICES | 4,001 | Active |
| RIEHM, DANIEL | RIEHM BENEFITS LLC | 3,601 | Active |
| FERNANDEZ, ANDRES | NE SOLUTIONS INC | 3,346 | Active |

---

## Assignment Patterns

### Pattern 1: Simple Redirect (Majority)

**99% of brokers have ONE recipient** (all commissions go to one entity)

**Example:** P17161 (PEACE, RICHARD)
- 100% of commissions → P12841 (FINANCIAL BENEFIT SERVICES LLC)
- Affects 4,001 certificates
- Started 2010-09-01, still active

---

### Pattern 2: Multiple Recipients (Rare)

**18 brokers have multiple recipients** (split assignments)

**Example:** P16658
- 3 different recipients across 163 certificates
- Suggests different assignments per group/product

**Example:** P13044
- 3 different recipients across 5 certificates

---

## Commission Calculation Impact

### How Assignments Work in Commission Calculations

**Without Assignment:**
```
Policy → PHA → Hierarchy → Participant (SplitBrokerId)
         ↓
      Commission paid to SplitBrokerId
```

**With Assignment:**
```
Policy → PHA → Hierarchy → Participant (SplitBrokerId)
         ↓                           ↓
      Lookup assignment         Commission earned by SplitBrokerId
         ↓                           ↓
      Redirect to PaidBrokerId
         ↓
      Commission paid to PaidBrokerId
```

---

### Lookup Logic for Commission Calculator

```sql
-- During commission calculation:
DECLARE @splitBrokerId BIGINT = 17161; -- From hierarchy participant
DECLARE @paidBrokerId BIGINT;

-- Check for active assignment
SELECT TOP 1 @paidBrokerId = r.RecipientBrokerId
FROM dbo.CommissionAssignmentVersions v
INNER JOIN dbo.CommissionAssignmentRecipients r ON r.VersionId = v.Id
WHERE v.BrokerId = @splitBrokerId
  AND v.Status = 1 -- Active
  AND v.EffectiveFrom <= GETDATE()
  AND (v.EffectiveTo IS NULL OR v.EffectiveTo >= GETDATE())
ORDER BY v.EffectiveFrom DESC;

-- If found, pay to @paidBrokerId
-- If not found, pay to @splitBrokerId (original)
```

---

## Verification Queries

### Check Assignment for Specific Broker

```sql
-- Example: Check assignments for broker 17161
SELECT 
    v.BrokerName as SourceBroker,
    r.RecipientName as RecipientBroker,
    v.EffectiveFrom,
    v.EffectiveTo,
    CASE 
        WHEN v.EffectiveTo IS NULL THEN 'Active'
        WHEN v.EffectiveTo >= GETDATE() THEN 'Active'
        ELSE 'Expired'
    END as Status,
    v.ChangeDescription
FROM dbo.CommissionAssignmentVersions v
INNER JOIN dbo.CommissionAssignmentRecipients r ON r.VersionId = v.Id
WHERE v.BrokerId = 17161;
```

### Find All Active Assignments

```sql
SELECT COUNT(*) as active_assignments
FROM dbo.CommissionAssignmentVersions
WHERE Status = 1
  AND EffectiveTo IS NULL;

-- Expected: 417
```

### Find Brokers with Multiple Recipients

```sql
SELECT 
    v.BrokerId,
    v.BrokerName,
    COUNT(DISTINCT r.RecipientBrokerId) as recipient_count
FROM dbo.CommissionAssignmentVersions v
INNER JOIN dbo.CommissionAssignmentRecipients r ON r.VersionId = v.Id
GROUP BY v.BrokerId, v.BrokerName
HAVING COUNT(DISTINCT r.RecipientBrokerId) > 1
ORDER BY recipient_count DESC;

-- Expected: 18 brokers with 2-3 recipients
```

---

## Integration with ETL Pipeline

### Pipeline Position

The commission assignments transform should run:
- **After:** Brokers transform (`01-brokers.sql`)
- **Before:** Commission calculations

### Run Commands

```bash
# Transform only
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C \
  -v ETL_SCHEMA="etl" \
  -i sql/transforms/12-commission-assignments.sql

# Export to production
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C \
  -v ETL_SCHEMA="etl" \
  -i sql/export/13-export-commission-assignments.sql
```

---

## Future Enhancements

### Potential Improvements

1. **Proposal-Level Assignments**
   - Current: Broker-level (all certificates)
   - Future: Assignment per proposal/group
   - Would require: `ProposalId` linkage

2. **Partial Assignments**
   - Current: 100% redirect
   - Future: Split percentages (e.g., 70% to recipient, 30% to original)
   - Would use: `TotalAssignedPercent < 100`

3. **Historical Versioning**
   - Track assignment changes over time
   - Multiple versions per broker
   - Version number tracking

4. **Assignment Reason Codes**
   - Retirement
   - Business entity change
   - Agency assignment
   - Termination
   - Would add: `ReasonCode` enum

---

## Known Limitations

1. **Broker-Level Only**
   - Assignments apply to ALL policies for that broker
   - Cannot vary by group/product (yet)
   - `ProposalId` currently set to `'BROKER-LEVEL'`

2. **Full Redirect Only**
   - All commissions go to recipient
   - Cannot split partial percentages
   - `Percentage` always 100.00

3. **Single Recipient per Version**
   - One source → one recipient per version
   - For multiple recipients, need multiple versions
   - Current data: 18 brokers with 2-3 recipients

---

## Testing Results

### Transform Results

✅ **1,333 unique assignments extracted**  
✅ **1,315 source brokers identified**  
✅ **521 recipient brokers identified**  
✅ **All broker IDs mapped correctly (P-prefix handled)**

### Export Results

✅ **1,333 CommissionAssignmentVersions exported**  
✅ **1,333 CommissionAssignmentRecipients exported**  
✅ **417 active assignments identified**  
✅ **916 expired assignments identified**

### Data Quality

✅ **No NULL BrokerId values**  
✅ **All recipients exist in Brokers table**  
✅ **Date ranges valid**  
✅ **Percentages = 100.00**

---

## Bottom Line

✅ **Commission Assignments ETL: COMPLETE**

- 1,333 assignments captured from 190,567 source transactions
- Covers 47.6% of all commission transactions
- 1,315 source brokers → 521 recipients
- 417 still active, 916 expired
- Ready for integration into commission calculations

**Next Step:** Update commission calculator to check assignments during payment distribution.

---

**Status:** 🟢 **PRODUCTION READY**
