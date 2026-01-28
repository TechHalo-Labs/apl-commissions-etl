# Split Configuration & Hierarchy Structure - Detailed Explanation

## 📋 Overview

This document explains the JSON structure of a complete split configuration with hierarchies, showing how premium splits are distributed across multiple commission structures with tiered participants and schedules.

---

## 🎯 Complete Data Structure

```
Split Configuration
    └── Premium Split Version (metadata)
    └── Split Participants (who gets what % of premium)
            │
            ├── Participant 1 (60%) → Hierarchy 1
            │                              └── Tier 1: Writing Agent (50% of split)
            │                              └── Tier 2: Supervisor (30% of split)
            │                              └── Tier 3: Regional Manager (15% of split)
            │                              └── Tier 4: MGA (5% of split)
            │
            └── Participant 2 (40%) → Hierarchy 2
                                       └── Tier 1: Partner Agent (60% of split)
                                       └── Tier 2: Supervisor (30% of split)
                                       └── Tier 3: MGA (10% of split)
```

---

## 📊 1. Split Configuration

The top-level structure that defines how premium is divided.

### Premium Split Version

```json
{
  "id": "PSV-G25992-V1",
  "groupId": "G25992",
  "groupName": "ABC Manufacturing Co.",
  "proposalId": "P-G25992-DENTAL",
  "proposalNumber": "PRO-2025-0123",
  "productId": "DENTAL",
  "versionNumber": "1.0",
  "effectiveFrom": "2025-01-01T00:00:00Z",
  "effectiveTo": null,
  "totalSplitPercent": 100.0,
  "status": 0
}
```

**Key Fields:**
- `totalSplitPercent`: Must equal 100 (all splits combined)
- `versionNumber`: Allows for changes over time
- `effectiveFrom/To`: Date range for this configuration

---

## 👥 2. Split Participants

Defines WHO gets a portion of the premium and HOW MUCH.

### Split Participant Example

```json
{
  "id": "SP-1",
  "versionId": "PSV-G25992-V1",
  "brokerId": 12345,
  "brokerName": "Smith, John (Writing Agent)",
  "splitPercent": 60.0,
  "isWritingAgent": true,
  "hierarchyId": "H-G25992-1",
  "hierarchyName": "Smith's Team - Dental Specialists",
  "sequence": 1
}
```

**Key Concepts:**
1. **Split Percent** (60%): This participant gets 60% of the premium
2. **Hierarchy Link**: Points to a hierarchy that defines how that 60% is distributed
3. **Writing Agent Flag**: Identifies the primary sales agent
4. **Sequence**: Order of splits (important for processing)

---

## 🏗️ 3. Hierarchy Structure

Each hierarchy defines a multi-tier commission structure.

### Hierarchy Metadata

```json
{
  "id": "H-G25992-1",
  "name": "Smith's Team - Dental Specialists",
  "type": 0,
  "typeName": "Commission",
  "status": 0,
  "statusName": "Active",
  "proposalId": "P-G25992-DENTAL",
  "groupId": "G25992",
  "currentVersionId": "HV-G25992-1-V2",
  "currentVersionNumber": "2.0"
}
```

**Versioning:**
- Hierarchies can have multiple versions over time
- `currentVersionId`: Points to the active version
- Allows for historical tracking and changes

---

## 🪜 4. Hierarchy Tiers

Each hierarchy has multiple tiers (levels) of participants.

### Tier Structure

```
Tier 1: Writing Agent    (50% of the split premium)
Tier 2: Supervisor       (30% of the split premium)
Tier 3: Regional Manager (15% of the split premium)
Tier 4: MGA              (5% of the split premium)
─────────────────────────────────────────────────────
Total:                   (100% of the split premium)
```

**Important:** Tier percentages are OF THE SPLIT, not the original premium!

---

## 👤 5. Hierarchy Participants

Each tier has one or more participants (brokers/organizations).

### Participant Detail

```json
{
  "id": "HP-1-1",
  "hierarchyVersionId": "HV-G25992-1-V2",
  "entityId": 12345,
  "entityName": "Smith, John",
  "entityType": "Broker",
  "npn": "NPN123456789",
  "level": 1,
  "sortOrder": 1,
  "splitPercent": 50.0,
  "scheduleCode": "SCH-DENTAL-TIER1-2025",
  "scheduleId": 1001,
  "commissionRate": 15.0,
  "firstYearRate": 18.0,
  "renewalRate": 15.0,
  "bonusRate": 2.0,
  "paidBrokerId": 12345
}
```

**Key Fields:**
- `level`: Tier number (1, 2, 3, 4...)
- `splitPercent`: Percentage of split premium this tier gets
- `scheduleCode/scheduleId`: Links to commission rate schedule
- `commissionRate`: Base rate (can vary by product, state, group size)
- `firstYearRate` vs `renewalRate`: Different rates for year 1 vs renewals
- `paidBrokerId`: Who receives the payment (may differ due to assignments)

---

## 📅 6. Schedule & Rates

Each participant has a schedule that defines their commission rates.

### Schedule Structure

```json
{
  "id": 1001,
  "externalId": "SCH-DENTAL-TIER1-2025",
  "name": "Dental Tier 1 - 2025",
  "description": "Writing agent rates for dental products",
  "status": "Active",
  "commissionType": "Tiered",
  "rateStructure": "GroupSize",
  "effectiveDate": "2025-01-01",
  "productLines": ["Dental", "Vision"],
  "productCodes": ["DENTAL", "VISION", "DENT_PLUS"]
}
```

### Rate Details by Group Size

```json
{
  "ratesByGroupSize": [
    {
      "groupSizeTier": "Small (1-25)",
      "groupSizeFrom": 1,
      "groupSizeTo": 25,
      "firstYearRate": 20.0,
      "renewalRate": 17.0,
      "bonusRate": 2.0
    },
    {
      "groupSizeTier": "Medium (26-100)",
      "groupSizeFrom": 26,
      "groupSizeTo": 100,
      "firstYearRate": 18.0,
      "renewalRate": 15.0,
      "bonusRate": 2.0
    },
    {
      "groupSizeTier": "Large (101-500)",
      "groupSizeFrom": 101,
      "groupSizeTo": 500,
      "firstYearRate": 15.0,
      "renewalRate": 12.0,
      "bonusRate": 1.5
    }
  ]
}
```

**Rate Lookup Logic:**
1. Match product code
2. Match state (if state-specific)
3. Match group size tier
4. Select first-year vs renewal rate based on policy age
5. Add bonus rate if applicable

### Rate Details by State (MAC Limits)

```json
{
  "ratesByState": [
    {
      "state": "TX",
      "maxAllowableCommission": 25.0,
      "firstYearRate": 18.0,
      "renewalRate": 15.0
    },
    {
      "state": "CA",
      "maxAllowableCommission": 20.0,
      "firstYearRate": 16.0,
      "renewalRate": 13.0
    }
  ]
}
```

**State-Specific Rules:**
- Each state has a Maximum Allowable Commission (MAC)
- Rates may be adjusted for state regulatory requirements
- System caps commission at MAC limit

---

## 🔄 7. Assignment Overrides

Sometimes commission is redirected to a different broker.

### Assignment Example

```json
{
  "assignmentOverrides": [
    {
      "assignmentId": "ASG-001",
      "assignedToBrokerId": 78901,
      "assignedToBrokerName": "Anderson, Tom",
      "assignmentPercent": 50.0,
      "effectiveFrom": "2025-01-01T00:00:00Z",
      "effectiveTo": null,
      "reason": "Temporary coverage during leave"
    }
  ]
}
```

**Use Cases:**
- Broker on leave/vacation
- Territory transfers
- Mentorship arrangements
- Book of business sales

**How it Works:**
- Original broker (Johnson): Earns $56.00 commission
- Assignment: 50% redirected
- Anderson receives: $28.00 (50% of $56)
- Johnson receives: $28.00 (remaining 50%)

---

## 💰 8. Complete Calculation Example

### Scenario:
- **Premium:** $1,000.00
- **Policy:** ABC Manufacturing (Group G25992)
- **Product:** Dental
- **Policy Age:** Renewal (not first year)
- **Group Size:** 75 employees (Medium tier)
- **State:** Texas

### Split 1 (60% = $600.00)

**Hierarchy 1: Smith's Team (4 tiers)**

| Tier | Name | Split % | Split Premium | Rate | Commission |
|------|------|---------|---------------|------|------------|
| 1 | Smith, John | 50% | $600.00 | 15% | $90.00 |
| 2 | Williams, Robert | 30% | $600.00 | 8% | $48.00 |
| 3 | Davis, Jennifer | 15% | $600.00 | 4% | $24.00 |
| 4 | Elite MGA | 5% | $600.00 | 2% | $12.00 |
| | **Split 1 Total** | | | | **$174.00** |

**Calculation Formula:**
```
Commission = Split Premium × (Rate / 100)

Tier 1: $600 × (15 / 100) = $90.00
Tier 2: $600 × (8 / 100) = $48.00
Tier 3: $600 × (4 / 100) = $24.00
Tier 4: $600 × (2 / 100) = $12.00
```

---

### Split 2 (40% = $400.00)

**Hierarchy 2: Johnson's Regional Team (3 tiers)**

| Tier | Name | Split % | Split Premium | Rate | Commission | Assignment |
|------|------|---------|---------------|------|------------|------------|
| 1 | Johnson, Mary | 60% | $400.00 | 14% | $56.00 | 50% → Anderson ($28) |
| | Anderson, Tom | | | | | Receives $28.00 |
| | Johnson (net) | | | | | Keeps $28.00 |
| 2 | Martinez, Carlos | 30% | $400.00 | 8% | $32.00 | None |
| 3 | Elite MGA | 10% | $400.00 | 2% | $8.00 | None |
| | **Split 2 Total** | | | | **$96.00** | |

**With Assignment:**
```
Johnson's Commission: $400 × (14 / 100) = $56.00
Assignment (50%): $56.00 × 0.50 = $28.00 → Anderson
Johnson Keeps: $56.00 - $28.00 = $28.00
```

---

### Grand Total

| Split | Premium | Commission | Effective Rate |
|-------|---------|------------|----------------|
| Split 1 | $600.00 | $174.00 | 29.0% |
| Split 2 | $400.00 | $96.00 | 24.0% |
| **Total** | **$1,000.00** | **$270.00** | **27.0%** |

---

## 🔍 Key Concepts Summary

### 1. **Two-Level Splitting**
- **First Level**: Premium split between hierarchies (60%/40%)
- **Second Level**: Split premium distributed across tiers within each hierarchy

### 2. **Waterfall (Cascading) Calculation**
- Each tier gets its rate percentage OF THE SPLIT PREMIUM
- Not cumulative - tier 2 doesn't subtract tier 1's commission
- All tiers operate on the same base (the split premium)

### 3. **Rate Variability**
Rates can vary by:
- **Product** (Dental vs Vision vs Life)
- **State** (regulatory requirements)
- **Group Size** (1-25, 26-100, 101-500, etc.)
- **Policy Age** (First year vs renewal)
- **Tier Level** (Agent vs Supervisor vs Manager)

### 4. **Versioning**
- Split configurations can change over time
- Hierarchies can change over time
- System uses effective dates to select correct version
- Historical calculations use version active on payment date

### 5. **Assignments**
- Redirect commission from one broker to another
- Can be partial (50%) or full (100%)
- Have effective date ranges
- Common for leaves, transfers, mentorship

---

## 📝 Data Model Relationships

```
PremiumSplitVersion (1)
    ├── PremiumSplitParticipants (N)
    │       └── Hierarchy (1)
    │               ├── HierarchyVersion (current)
    │               │       └── HierarchyParticipants (N)
    │               │               ├── Broker/Organization
    │               │               └── Schedule (1)
    │               │                       └── ScheduleRates (N)
    │               │                               └── RatesByGroupSize (N)
    │               │                               └── RatesByState (N)
    │               └── Assignments (N)
    │                       └── AssignedToBroker
    └── Proposal (1)
            └── Group (1)
            └── Product (1)
```

---

## 🎯 Business Rules

### Split Percentages Must Equal 100%
```
Split 1: 60%
Split 2: 40%
─────────────
Total:  100% ✅
```

### Tier Percentages Within Split (Flexible)
```
Hierarchy 1:
  Tier 1: 50%
  Tier 2: 30%
  Tier 3: 15%
  Tier 4: 5%
  ─────────────
  Total:  100% ✅

Hierarchy 2:
  Tier 1: 60%
  Tier 2: 30%
  Tier 3: 10%
  ─────────────
  Total:  100% ✅
```

### MAC (Maximum Allowable Commission) Enforcement
- Each state has a regulatory cap
- System never pays more than MAC
- Commission is capped BEFORE distribution
- Example: If calculated rate is 28% but MAC is 25%, use 25%

### First Year vs Renewal
```
Policy Effective: 2024-01-01
Payment Date:     2024-06-15  → First Year Rate (18%)
Payment Date:     2025-02-10  → Renewal Rate (15%)
```

---

## 📊 JSON File Location

**File:** `/Users/kennpalm/Downloads/source/APL/apl-commissions-etl/SPLIT-HIERARCHY-EXAMPLE.json`

This file contains:
- Complete split configuration
- Two hierarchies with multiple tiers
- Detailed schedule information
- Rate tables by group size and state
- Assignment override example
- Complete calculation example with breakdown

---

## 🚀 Usage

This JSON structure represents:
1. **Database Export**: How data looks when exported from staging tables
2. **API Response**: Format for commission configuration endpoints
3. **Traceability Report**: Detailed breakdown in commission reports
4. **Testing Data**: Template for creating test scenarios

---

## ✅ Validation Rules

When validating this structure:

1. ✅ `totalSplitPercent` = 100.0
2. ✅ All `splitPercent` values sum to `totalSplitPercent`
3. ✅ Each hierarchy's tier `splitPercent` values sum to 100.0
4. ✅ All `scheduleId` references exist in schedules table
5. ✅ All `brokerId` references exist in brokers table
6. ✅ Effective dates are logical (from < to, or to is null)
7. ✅ Rate percentages are positive and reasonable (0-100%)
8. ✅ Assignment percentages are between 0-100%
9. ✅ `paidBrokerId` matches `entityId` unless assignment override exists
10. ✅ Schedule `productCodes` include the policy's product code
