# Portable Documentation: Proposal Builder & Database Schemas

**Generated:** 2026-01-29  
**Purpose:** Complete portable reference for proposal builder logic and all related database structures

---

# Table of Contents

1. [Proposal Builder Logic (TypeScript)](#proposal-builder-logic)
2. [Database Schemas - Staging Tables](#database-schemas-staging)
3. [Database Schemas - Commission Assignments](#database-schemas-commission-assignments)
4. [Entity Relationships](#entity-relationships)
5. [Key Algorithms](#key-algorithms)

---

# Proposal Builder Logic

## Overview

The Proposal Builder is a TypeScript application that generates commission agreements and hierarchy structures from certificate data.

**Input:** Certificate records with split/hierarchy information  
**Output:** 9 staging tables with proposals, splits, hierarchies, and policy assignments

## Core Algorithm

```
1. Load certificates from input_certificate_info
2. Extract selection criteria (group certificates by config)
3. Build proposals (consolidate by GroupId + ConfigHash)
4. Generate staging output (9 tables)
5. Write to database (batched multi-row inserts)
```

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Proposal** | Commission agreement for a group with specific split configuration |
| **ConfigHash** | SHA256 hash of split configuration (identifies unique arrangements) |
| **Split** | Premium percentage allocated to a specific hierarchy |
| **Hierarchy** | Chain of brokers (Agent → Supervisor → Manager) |
| **PHA** | Policy Hierarchy Assignment (for non-conformant policies) |

---

## TypeScript Implementation

### File: `scripts/proposal-builder.ts`

**Key Classes:**
- `ProposalBuilder` - Main builder class
- `CertificateRecord` - Input certificate interface
- `Proposal` - Output proposal interface
- `SplitConfiguration` - Split arrangement structure

### Core Methods

#### 1. Load Certificates
```typescript
async function loadCertificatesFromDatabase(
  config: DatabaseConfig,
  options: BuilderOptions
): Promise<CertificateRecord[]>
```

**Query:**
```sql
SELECT 
  CertificateId, GroupId, CertEffectiveDate, Product, PlanCode,
  CertSplitSeq, CertSplitPercent, SplitBrokerSeq, SplitBrokerId, CommissionsSchedule
FROM [etl].[input_certificate_info]
WHERE CertStatus = 'A' AND RecStatus = 'A' AND CertEffectiveDate IS NOT NULL
```

#### 2. Extract Selection Criteria

**Logic:**
```typescript
extractSelectionCriteria(): void {
  // Group certificates by (GroupId, CertificateId)
  // For each certificate:
  //   - Build splits (by CertSplitSeq)
  //   - Build hierarchy tiers (by SplitBrokerSeq)
  //   - Compute hierarchy hash (SHA256 of tier structure)
  //   - Compute config hash (SHA256 of all splits)
}
```

**Hierarchy Hash Input:**
```json
[
  {"level": 1, "brokerId": "P13178", "schedule": "5010T"},
  {"level": 2, "brokerId": "P13179", "schedule": "5020T"}
]
```

**Config Hash Input:**
```json
[
  {"seq": 1, "pct": 100, "hierarchyHash": "ABC123..."},
  {"seq": 2, "pct": 100, "hierarchyHash": "DEF456..."}
]
```

#### 3. Build Proposals

**Logic:**
```typescript
buildProposals(): void {
  // Group selection criteria by (GroupId, ConfigHash)
  // For each group:
  //   - Create one proposal
  //   - Expand ProductCodes, PlanCodes, DateRange
  //   - Collect certificate IDs
  
  // Route to PHA if:
  //   - GroupId is null/empty/all-zeros
  //   - (Future: Multiple hierarchies per split)
}
```

**Proposal Key:** `GroupId + ConfigHash`

**Example:**
- Group `0001` with 2 splits (100% each) → Proposal `P-1`
- Group `0001` with different split configuration → Proposal `P-2`
- Group `0002` with same split as `P-1` → Proposal `P-3` (different group)

#### 4. Generate Staging Output

**Deduplication:**
- Hierarchies: Deduplicated by `hierarchyHash`
- Key Mappings: Deduplicated by `(GroupId, EffectiveYear, ProductCode, PlanCode)`

**Entity ID Patterns:**
- Proposals: `P-{counter}`
- Split Versions: `PSV-{counter}`
- Split Participants: `PSP-{counter}`
- Hierarchies: `H-{counter}`
- Hierarchy Versions: `HV-{counter}`
- Hierarchy Participants: `HP-{counter}`
- PHA Assignments: `PHA-{counter}`
- PHA Participants: `PHP-{counter}`

#### 5. Write to Database

**Batching Strategy:**
```
SQL Server: Max 2100 parameters per query

Proposals: 16 params/row → 100 rows/batch
Key Mappings: 6 params/row → 300 rows/batch
Hierarchies: 11 params/row → 180 rows/batch
Hierarchy Participants: 10 params/row → 200 rows/batch
```

**Multi-Row INSERT:**
```sql
INSERT INTO [etl].[stg_proposals] (...) 
VALUES 
  (@Id0, @ProposalNumber0, ...),
  (@Id1, @ProposalNumber1, ...),
  ...
  (@Id99, @ProposalNumber99, ...)
```

---

## Broker ID Mapping

**Source Format:** `P13178` (with "P" prefix)  
**Internal Format:** `13178` (numeric, no prefix)

**Conversion Function:**
```typescript
function brokerExternalToInternal(externalId: string): number {
  const numStr = externalId.replace(/^P/, '');
  return parseInt(numStr, 10);
}
```

**Database Storage:**
- `Brokers.Id` = `13178` (Primary Key)
- `Brokers.ExternalPartyId` = `P13178` (for lookup)

---

## Hash Collision Detection

**Original Design:** Truncated 16-character hash (high collision risk)  
**Current Design:** Full 64-character SHA256 hash with collision detection

```typescript
private computeHashWithCollisionCheck(input: string, context: string): string {
  const hash = crypto.createHash('sha256').update(input).digest('hex').toUpperCase();
  
  if (this.hashCollisions.has(hash)) {
    const existing = this.hashCollisions.get(hash);
    if (existing !== input) {
      throw new Error(`Hash collision detected for ${context}`);
    }
  }
  
  this.hashCollisions.set(hash, input);
  return hash;
}
```

---

## Invalid Group Detection

**Routes to PHA when:**
```typescript
private isInvalidGroup(groupId: string): boolean {
  if (!groupId) return true;                     // NULL
  const trimmed = groupId.trim();
  if (trimmed === '') return true;               // Empty string
  if (/^0+$/.test(trimmed)) return true;         // All zeros (e.g., "0000")
  return false;
}
```

**Examples:**
- `NULL` → PHA
- `""` → PHA
- `"0000"` → PHA
- `"00000"` → PHA (DTC - Direct-to-Consumer)
- `"0001"` → Proposal (valid group)

---

## CLI Usage

```bash
# Full ETL (all certificates)
npx tsx scripts/proposal-builder.ts --verbose

# Test with limit
npx tsx scripts/proposal-builder.ts --limit 1000 --verbose

# Dry run (no database writes)
npx tsx scripts/proposal-builder.ts --dry-run --verbose

# Different schema
npx tsx scripts/proposal-builder.ts --schema staging --verbose
```

**Environment Variable:**
```bash
export SQLSERVER="Server=halo-sql.database.windows.net;Database=halo-sqldb;User Id=azadmin;Password=..."
```

---

# Database Schemas - Staging

## Overview

**Schema:** `etl`  
**Purpose:** Staging tables for ETL pipeline  
**Lifecycle:** Cleared and repopulated on each ETL run

---

## 1. stg_proposals

**Purpose:** Commission agreements (proposals) per group

```sql
CREATE TABLE [etl].[stg_proposals] (
    Id nvarchar(200) NOT NULL PRIMARY KEY,
    ProposalNumber nvarchar(200) NOT NULL,
    Status int NULL,
    SubmittedDate datetime2 NOT NULL,
    ProposedEffectiveDate datetime2 NOT NULL,
    SpecialCase bit NULL,
    SpecialCaseCode int NULL,
    SitusState nvarchar(20) NULL,
    ProductId nvarchar(200) NULL,
    ProductName nvarchar(1000) NULL,
    BrokerId bigint NULL,
    BrokerUniquePartyId nvarchar(100) NULL,
    BrokerName nvarchar(1000) NULL,
    GroupId nvarchar(200) NULL,
    GroupName nvarchar(1000) NULL,
    ContractId nvarchar(200) NULL,
    RejectionReason nvarchar(4000) NULL,
    Notes nvarchar(MAX) NULL,
    ProductCodes nvarchar(MAX) NULL,           -- CSV list
    PlanCodes nvarchar(MAX) NULL,              -- CSV list
    SplitConfigHash nvarchar(128) NULL,        -- SHA256 hash
    DateRangeFrom int NULL,                    -- Year
    DateRangeTo int NULL,                      -- Year
    PlanCodeConstraints nvarchar(MAX) NULL,
    EnablePlanCodeFiltering bit NULL,
    EffectiveDateFrom datetime2 NULL,
    EffectiveDateTo datetime2 NULL,
    EnableEffectiveDateFiltering bit NULL,
    ConstrainingEffectiveDateFrom datetime2 NULL,
    ConstrainingEffectiveDateTo datetime2 NULL,
    DisplayName nvarchar(200) NULL,
    CreationTime datetime2 NULL,
    IsDeleted bit NULL
);
```

**Key Fields:**
- `Id` - Unique proposal ID (e.g., `P-1`, `P-2`)
- `GroupId` - Employer group ID
- `SplitConfigHash` - Identifies unique split configuration
- `ProductCodes` - Comma-separated (e.g., `"DENTAL,VISION"`)
- `PlanCodes` - Comma-separated (e.g., `"PLAN_A,PLAN_B"`)
- `DateRangeFrom/To` - Year range covered by proposal

**Example Row:**
```
Id: P-1
ProposalNumber: PROP-0001-1
GroupId: 0001
ProductCodes: DENTAL,VISION,LIFE
PlanCodes: PLAN_A,PLAN_B
SplitConfigHash: ABC123DEF456...
DateRangeFrom: 2020
DateRangeTo: 2025
```

---

## 2. stg_proposal_key_mapping

**Purpose:** Fast lookup table for proposal resolution

```sql
CREATE TABLE [etl].[stg_proposal_key_mapping] (
    GroupId nvarchar(200) NOT NULL,
    EffectiveYear int NOT NULL,
    ProductCode nvarchar(200) NOT NULL,
    PlanCode nvarchar(200) NOT NULL,
    ProposalId nvarchar(200) NOT NULL,
    SplitConfigHash nvarchar(128) NULL,
    CreationTime datetime2 NULL,
    PRIMARY KEY (GroupId, EffectiveYear, ProductCode, PlanCode)
);
```

**Key Fields:**
- Composite key: `(GroupId, EffectiveYear, ProductCode, PlanCode)`
- `ProposalId` - Points to `stg_proposals.Id`

**Purpose:** 
During commission calculation, quickly find proposal by:
```sql
SELECT ProposalId 
FROM stg_proposal_key_mapping
WHERE GroupId = @GroupId
  AND EffectiveYear = YEAR(@TransactionDate)
  AND ProductCode = @ProductCode
  AND PlanCode = @PlanCode
```

**Example Rows:**
```
GroupId | EffectiveYear | ProductCode | PlanCode | ProposalId | SplitConfigHash
--------|---------------|-------------|----------|------------|----------------
0001    | 2020          | DENTAL      | PLAN_A   | P-1        | ABC123...
0001    | 2021          | DENTAL      | PLAN_A   | P-1        | ABC123...
0001    | 2020          | VISION      | PLAN_B   | P-1        | ABC123...
```

---

## 3. stg_premium_split_versions

**Purpose:** Premium split configuration versions

```sql
CREATE TABLE [etl].[stg_premium_split_versions] (
    Id nvarchar(200) NOT NULL PRIMARY KEY,
    GroupId nvarchar(200) NOT NULL,
    GroupName nvarchar(1000) NULL,
    ProposalId nvarchar(200) NOT NULL,
    ProposalNumber nvarchar(200) NULL,
    ProductId nvarchar(200) NULL,
    VersionNumber nvarchar(100) NULL,
    EffectiveFrom datetime2 NOT NULL,
    EffectiveTo datetime2 NULL,
    ChangeDescription nvarchar(4000) NULL,
    TotalSplitPercent decimal(18,4) NOT NULL,  -- Sum of all split percentages
    Status int NULL,
    Source int NULL,
    HubspotDealId nvarchar(200) NULL,
    CreationTime datetime2 NULL,
    IsDeleted bit NULL
);
```

**Key Fields:**
- `TotalSplitPercent` - Sum of all splits (e.g., 100.00 for single split, 200.00 for two 100% splits)

**Example Row:**
```
Id: PSV-1
ProposalId: P-1
TotalSplitPercent: 200.00    -- Two 100% splits
VersionNumber: V1
EffectiveFrom: 2020-01-01
EffectiveTo: 9999-12-31
```

---

## 4. stg_premium_split_participants

**Purpose:** Individual splits (links to hierarchies)

```sql
CREATE TABLE [etl].[stg_premium_split_participants] (
    Id nvarchar(200) NOT NULL PRIMARY KEY,
    VersionId nvarchar(200) NOT NULL,         -- FK to stg_premium_split_versions
    BrokerId bigint NOT NULL,
    BrokerUniquePartyId nvarchar(100) NULL,
    BrokerName nvarchar(1000) NULL,
    BrokerNPN nvarchar(100) NULL,
    SplitPercent decimal(18,4) NOT NULL,      -- This split's percentage
    IsWritingAgent bit NULL,
    HierarchyId nvarchar(200) NULL,           -- FK to stg_hierarchies
    HierarchyName nvarchar(1000) NULL,
    TemplateId nvarchar(200) NULL,
    TemplateName nvarchar(1000) NULL,
    EffectiveFrom datetime2 NOT NULL,
    EffectiveTo datetime2 NULL,
    Notes nvarchar(MAX) NULL,
    Sequence int NULL,                        -- Split sequence number
    WritingBrokerId bigint NULL,              -- Writing broker for this split
    GroupId nvarchar(200) NULL,
    CreationTime datetime2 NULL,
    IsDeleted bit NULL
);
```

**Key Fields:**
- `VersionId` - Links to split version
- `HierarchyId` - Links to hierarchy (broker chain)
- `SplitPercent` - This split's percentage (e.g., 100.00)
- `Sequence` - Split number (1, 2, 3...)

**Example Rows:**
```
Id       | VersionId | BrokerId | SplitPercent | HierarchyId | Sequence
---------|-----------|----------|--------------|-------------|----------
PSP-1    | PSV-1     | 13178    | 100.00       | H-1         | 1
PSP-2    | PSV-1     | 14522    | 100.00       | H-2         | 2
```

---

## 5. stg_hierarchies

**Purpose:** Hierarchy containers (broker chains)

```sql
CREATE TABLE [etl].[stg_hierarchies] (
    Id nvarchar(200) NOT NULL PRIMARY KEY,
    Name nvarchar(1000) NULL,
    Description nvarchar(4000) NULL,
    Type int NULL,
    Status int NULL,
    ProposalId nvarchar(200) NULL,            -- FK to stg_proposals
    ProposalNumber nvarchar(200) NULL,
    GroupId nvarchar(200) NULL,
    GroupName nvarchar(1000) NULL,
    GroupNumber nvarchar(200) NULL,
    BrokerId bigint NULL,                     -- Writing broker
    BrokerName nvarchar(1000) NULL,
    BrokerLevel int NULL,
    ContractId nvarchar(200) NULL,
    ContractNumber nvarchar(200) NULL,
    ContractType nvarchar(200) NULL,
    ContractStatus nvarchar(100) NULL,
    SourceType nvarchar(200) NULL,            -- 'NonConformant-Historical' for PHA
    HasOverrides bit NULL,
    DeviationCount int NULL,
    SitusState nvarchar(20) NULL,
    EffectiveDate date NULL,
    CurrentVersionId nvarchar(200) NULL,      -- FK to stg_hierarchy_versions
    CurrentVersionNumber int NULL,
    TemplateId nvarchar(200) NULL,
    TemplateVersion nvarchar(100) NULL,
    TemplateSyncStatus int NULL,
    CreationTime datetime2 NULL,
    IsDeleted bit NULL
);
```

**Key Fields:**
- `ProposalId` - Links to proposal (for conformant policies)
- `BrokerId` - Writing broker (top of hierarchy)
- `CurrentVersionId` - Points to current hierarchy version
- `SourceType` - `'NonConformant-Historical'` for PHA hierarchies

**Example Row:**
```
Id: H-1
Name: Hierarchy for 0001
ProposalId: P-1
BrokerId: 13178
BrokerName: SMITH, JOHN
CurrentVersionId: HV-1
EffectiveDate: 2020-01-01
```

---

## 6. stg_hierarchy_versions

**Purpose:** Time-versioned hierarchy structures

```sql
CREATE TABLE [etl].[stg_hierarchy_versions] (
    Id nvarchar(200) NOT NULL PRIMARY KEY,
    HierarchyId nvarchar(200) NULL,           -- FK to stg_hierarchies
    Version int NULL,
    Status int NULL,
    EffectiveFrom datetime2 NULL,
    EffectiveTo datetime2 NULL,
    ChangeReason nvarchar(4000) NULL,
    CreationTime datetime2 NULL,
    IsDeleted bit NULL
);
```

**Key Fields:**
- `HierarchyId` - Links to hierarchy
- `EffectiveFrom/To` - Version date range
- `Version` - Version number (1, 2, 3...)

**Example Row:**
```
Id: HV-1
HierarchyId: H-1
Version: 1
EffectiveFrom: 2020-01-01
EffectiveTo: 9999-12-31
```

---

## 7. stg_hierarchy_participants

**Purpose:** Brokers in hierarchy chains (tiers)

```sql
CREATE TABLE [etl].[stg_hierarchy_participants] (
    Id nvarchar(200) NOT NULL PRIMARY KEY,
    HierarchyVersionId nvarchar(200) NOT NULL,  -- FK to stg_hierarchy_versions
    EntityId bigint NOT NULL,                   -- Broker ID
    EntityName nvarchar(1000) NULL,             -- Broker name
    Level int NULL,                             -- Tier level (1, 2, 3...)
    SortOrder int NULL,
    SplitPercent decimal(18,4) NULL,
    ScheduleCode nvarchar(400) NULL,            -- Commission schedule
    ScheduleId bigint NULL,                     -- FK to Schedules
    CommissionRate decimal(18,4) NULL,
    PaidBrokerId bigint NULL,                   -- For assignments
    CreationTime datetime2 NULL,
    IsDeleted bit NULL
);
```

**Key Fields:**
- `HierarchyVersionId` - Links to hierarchy version
- `EntityId` - Broker ID (numeric, no P-prefix)
- `Level` - Tier level (1 = Agent, 2 = Supervisor, 3 = Manager, etc.)
- `ScheduleCode` - Commission schedule (e.g., `"5010T"`, `"SR-AGT"`)
- `ScheduleId` - Resolved schedule ID (populated during export)

**Example Rows:**
```
Id    | HierarchyVersionId | EntityId | Level | ScheduleCode
------|-------------------|----------|-------|-------------
HP-1  | HV-1              | 13178    | 1     | 5010T
HP-2  | HV-1              | 13179    | 2     | 5020T
HP-3  | HV-1              | 13180    | 3     | 5030T
```

---

## 8. stg_policy_hierarchy_assignments

**Purpose:** Non-conformant policy assignments (PHA)

```sql
CREATE TABLE [etl].[stg_policy_hierarchy_assignments] (
    Id nvarchar(200) NOT NULL PRIMARY KEY,
    PolicyId nvarchar(200) NOT NULL,            -- Certificate ID
    CertificateId bigint NULL,
    HierarchyId nvarchar(200) NULL,             -- Synthetic hierarchy ID
    SplitPercent decimal(5,2) NOT NULL,         -- Split percentage
    WritingBrokerId bigint NOT NULL,            -- Writing broker
    SplitSequence int NOT NULL,                 -- Split number
    IsNonConforming bit NULL,
    NonConformantReason nvarchar(1000) NULL,    -- Reason for PHA
    SourceTraceabilityReportId bigint NULL,
    CreationTime datetime2 NULL,
    IsDeleted bit NULL
);
```

**Purpose:** Used when policies don't conform to standard proposal structure

**Reasons for PHA:**
- `"Invalid GroupId (null/empty/zeros)"`
- `"No matching proposal"` (future)
- `"Multiple hierarchies per split"` (future)

**Example Rows:**
```
Id       | PolicyId | WritingBrokerId | SplitSequence | SplitPercent | NonConformantReason
---------|----------|-----------------|---------------|--------------|--------------------
PHA-1    | 441304   | 14694           | 1             | 40.00        | Invalid GroupId
PHA-2    | 441304   | 13722           | 2             | 50.00        | Invalid GroupId
PHA-3    | 441304   | 13702           | 3             | 10.00        | Invalid GroupId
```

---

## 9. stg_policy_hierarchy_participants

**Purpose:** Embedded participants for PHA

```sql
CREATE TABLE [etl].[stg_policy_hierarchy_participants] (
    Id nvarchar(200) NOT NULL PRIMARY KEY,
    PolicyHierarchyAssignmentId nvarchar(200) NOT NULL,  -- FK to stg_policy_hierarchy_assignments
    BrokerId bigint NOT NULL,                            -- Broker ID
    BrokerName nvarchar(1000) NULL,
    Level int NOT NULL,                                  -- Tier level
    CommissionRate decimal(18,4) NULL,
    ScheduleCode nvarchar(400) NULL,                     -- Commission schedule
    ScheduleId bigint NULL,
    ReassignedType nvarchar(100) NULL,
    PaidBrokerId bigint NULL,
    CreationTime datetime2 NULL,
    IsDeleted bit NULL
);
```

**Key Fields:**
- `PolicyHierarchyAssignmentId` - Links to PHA
- `BrokerId` - Broker in hierarchy
- `Level` - Tier level
- `ScheduleCode` - Commission schedule

**Example Rows:**
```
Id     | PolicyHierarchyAssignmentId | BrokerId | Level | ScheduleCode
-------|----------------------------|----------|-------|-------------
PHP-1  | PHA-1                      | 14694    | 1     | D55125
PHP-2  | PHA-1                      | 13722    | 2     | 6015
```

---

# Database Schemas - Commission Assignments

## Overview

**Schema:** `dbo` (production)  
**Purpose:** Track commission redirections (assignments)  
**Records:** 1,333 assignments

---

## 1. CommissionAssignmentVersions

**Purpose:** Assignment versions (who assigns to whom)

```sql
CREATE TABLE [dbo].[CommissionAssignmentVersions] (
    Id nvarchar(100) NOT NULL PRIMARY KEY,
    BrokerId bigint NOT NULL,                   -- Source broker (who earns)
    BrokerName nvarchar(510) NULL,
    ProposalId nvarchar(100) NOT NULL,          -- 'BROKER-LEVEL' for broker-level assignments
    GroupId nvarchar(100) NULL,
    HierarchyId nvarchar(100) NULL,
    HierarchyVersionId nvarchar(100) NULL,
    HierarchyParticipantId nvarchar(100) NULL,
    VersionNumber nvarchar(40) NULL,
    EffectiveFrom datetime2 NOT NULL,           -- When assignment started
    EffectiveTo datetime2 NULL,                 -- NULL = still active
    Status int NOT NULL,                        -- 1 = Active
    Type int NOT NULL,                          -- 1 = Full assignment
    ChangeDescription nvarchar(1000) NULL,
    TotalAssignedPercent decimal(5,2) NOT NULL, -- 100.00 = full redirect
    CreationTime datetime2 NOT NULL,
    CreatorUserId bigint NULL,
    LastModificationTime datetime2 NULL,
    LastModifierUserId bigint NULL,
    IsDeleted bit NOT NULL,
    DeleterUserId bigint NULL,
    DeletionTime datetime2 NULL
);
```

**Key Fields:**
- `BrokerId` - Broker who earns the commission
- `ProposalId` - Currently `'BROKER-LEVEL'` (applies to all policies)
- `EffectiveFrom/To` - Date range for assignment
- `TotalAssignedPercent` - 100.00 = full redirect
- `Status` - 1 = Active, 0 = Inactive

**Example Row:**
```
Id: CAV-17161-12841
BrokerId: 17161
BrokerName: PEACE, RICHARD
ProposalId: BROKER-LEVEL
EffectiveFrom: 2010-09-01
EffectiveTo: NULL           -- Still active
Status: 1
TotalAssignedPercent: 100.00
ChangeDescription: Commission assignment from PEACE, RICHARD to FINANCIAL BENEFIT SERVICES LLC affecting 4001 certificates
```

---

## 2. CommissionAssignmentRecipients

**Purpose:** Recipients of assigned commissions

```sql
CREATE TABLE [dbo].[CommissionAssignmentRecipients] (
    Id nvarchar(900) NOT NULL PRIMARY KEY,
    VersionId nvarchar(100) NOT NULL,         -- FK to CommissionAssignmentVersions
    RecipientBrokerId bigint NOT NULL,        -- Who receives payment
    RecipientName nvarchar(510) NULL,
    RecipientNPN nvarchar(40) NULL,
    Percentage decimal(5,2) NOT NULL,         -- 100.00 = receives all
    RecipientHierarchyId nvarchar(100) NULL,
    Notes nvarchar(1000) NULL
);
```

**Key Fields:**
- `VersionId` - Links to assignment version
- `RecipientBrokerId` - Broker who receives payment
- `Percentage` - 100.00 = receives all (full redirect)

**Example Row:**
```
Id: CAR-17161-12841
VersionId: CAV-17161-12841
RecipientBrokerId: 12841
RecipientName: FINANCIAL BENEFIT SERVICES LLC
Percentage: 100.00
Notes: Receives commissions from PEACE, RICHARD for 4001 certificates
```

---

## Assignment Lookup Logic

During commission calculation:

```sql
-- Check for active assignment
DECLARE @sourceBrokerId BIGINT = 17161;  -- From hierarchy participant
DECLARE @recipientBrokerId BIGINT;
DECLARE @transactionDate DATE = GETDATE();

SELECT TOP 1 @recipientBrokerId = r.RecipientBrokerId
FROM dbo.CommissionAssignmentVersions v
INNER JOIN dbo.CommissionAssignmentRecipients r ON r.VersionId = v.Id
WHERE v.BrokerId = @sourceBrokerId
  AND v.Status = 1                                    -- Active
  AND v.EffectiveFrom <= @transactionDate
  AND (v.EffectiveTo IS NULL OR v.EffectiveTo >= @transactionDate)
ORDER BY v.EffectiveFrom DESC;

-- If @recipientBrokerId found, pay to recipient
-- Otherwise, pay to @sourceBrokerId (original broker)
```

---

# Entity Relationships

## Conformant Policy Chain

```
Certificate
    ↓
stg_proposal_key_mapping → stg_proposals
    ↓
stg_premium_split_versions
    ↓
stg_premium_split_participants → stg_hierarchies
    ↓
stg_hierarchy_versions
    ↓
stg_hierarchy_participants → Schedules → ScheduleRates
    ↓
Commission Calculation
    ↓
CommissionAssignmentVersions → CommissionAssignmentRecipients (if redirect)
    ↓
Payment to Broker (original or recipient)
```

## Non-Conformant Policy Chain (PHA)

```
Certificate
    ↓
stg_policy_hierarchy_assignments
    ↓
stg_policy_hierarchy_participants → Schedules → ScheduleRates
    ↓
Commission Calculation
    ↓
CommissionAssignmentVersions → CommissionAssignmentRecipients (if redirect)
    ↓
Payment to Broker (original or recipient)
```

---

# Key Algorithms

## 1. Proposal Resolution

**Input:** `(GroupId, EffectiveYear, ProductCode, PlanCode)`  
**Output:** `ProposalId` and `SplitConfigHash`

```sql
SELECT ProposalId, SplitConfigHash
FROM stg_proposal_key_mapping
WHERE GroupId = @GroupId
  AND EffectiveYear = YEAR(@TransactionDate)
  AND ProductCode = @ProductCode
  AND PlanCode = @PlanCode
```

**Fallback:** If not found, check PHA:
```sql
SELECT Id FROM stg_policy_hierarchy_assignments
WHERE PolicyId = @CertificateId
```

---

## 2. Hierarchy Selection

**Input:** `ProposalId` or `PHAId`  
**Output:** Hierarchy participants with schedules

**For Proposals:**
```sql
SELECT 
    hp.EntityId as BrokerId,
    hp.Level,
    hp.ScheduleCode,
    s.Id as ScheduleId
FROM stg_premium_split_participants psp
INNER JOIN stg_hierarchies h ON h.Id = psp.HierarchyId
INNER JOIN stg_hierarchy_versions hv ON hv.HierarchyId = h.Id
INNER JOIN stg_hierarchy_participants hp ON hp.HierarchyVersionId = hv.Id
LEFT JOIN Schedules s ON s.ExternalId = hp.ScheduleCode OR s.Name = hp.ScheduleCode
WHERE psp.VersionId IN (
    SELECT Id FROM stg_premium_split_versions WHERE ProposalId = @ProposalId
)
ORDER BY psp.Sequence, hp.Level
```

**For PHA:**
```sql
SELECT 
    php.BrokerId,
    php.Level,
    php.ScheduleCode,
    s.Id as ScheduleId
FROM stg_policy_hierarchy_participants php
LEFT JOIN Schedules s ON s.ExternalId = php.ScheduleCode OR s.Name = php.ScheduleCode
WHERE php.PolicyHierarchyAssignmentId = @PHAId
ORDER BY php.Level
```

---

## 3. Commission Calculation

**Formula:**
```
Split Premium = Premium × (SplitPercent / 100)
Tier Commission = Split Premium × (Schedule Rate / 100)
```

**With Assignment Redirect:**
```
1. Calculate commission for SourceBroker (from hierarchy)
2. Check CommissionAssignmentVersions for active assignment
3. If assignment exists:
   - Redirect payment to RecipientBroker
   - Original broker earns, but recipient receives payment
4. Else:
   - Pay to original SourceBroker
```

---

## 4. Rate Lookup Priority

```
1. Certificate-level rates (highest priority)
2. ETL migrated rates
3. Schedule lookup (by product, state, group size, first-year flag)
4. Default rate (if none found)
```

**Schedule Rate Query:**
```sql
SELECT 
    CASE 
        WHEN @IsFirstYear = 1 THEN sr.FirstYearRate
        ELSE sr.RenewalRate
    END as CommissionRate
FROM ScheduleRates sr
INNER JOIN Schedules s ON s.Id = sr.ScheduleId
WHERE s.Id = @ScheduleId
  AND sr.ProductCode = @ProductCode
  AND sr.State = @State
  AND sr.GroupSizeTier = @GroupSizeTier
```

---

## 5. Hash-Based Deduplication

**Purpose:** Avoid creating duplicate hierarchies

**Logic:**
```typescript
// Compute hierarchy hash
const hierarchyJson = JSON.stringify(
  tiers.map(t => ({ level: t.level, brokerId: t.brokerId, schedule: t.schedule }))
);
const hierarchyHash = crypto.createHash('sha256').update(hierarchyJson).digest('hex');

// Check if hierarchy already exists
if (!hierarchyByHash.has(hierarchyHash)) {
  hierarchyByHash.set(hierarchyHash, { writingBrokerId, tiers });
  // Create new hierarchy
} else {
  // Reuse existing hierarchy
}
```

**Result:** Only unique hierarchies are created, even if used by multiple proposals.

---

# Performance Optimizations

## 1. Certificate Grouping (O(n) vs O(n²))

**Before (O(n²)):**
```typescript
for (const cert of certificates) {
  const relatedCerts = certificates.filter(c => c.certificateId === cert.certificateId);
  // Process...
}
```

**After (O(n)):**
```typescript
const certGroups = new Map<string, CertificateRecord[]>();
for (const cert of certificates) {
  const key = cert.certificateId;
  if (!certGroups.has(key)) certGroups.set(key, []);
  certGroups.get(key)!.push(cert);
}

for (const [certId, certs] of certGroups) {
  // Process group...
}
```

---

## 2. Batched Multi-Row Inserts

**Before (N queries):**
```sql
INSERT INTO table VALUES (@val1, @val2, @val3);
INSERT INTO table VALUES (@val1, @val2, @val3);
INSERT INTO table VALUES (@val1, @val2, @val3);
-- ... N times
```

**After (1 query per batch):**
```sql
INSERT INTO table VALUES 
  (@val1_0, @val2_0, @val3_0),
  (@val1_1, @val2_1, @val3_1),
  (@val1_2, @val2_2, @val3_2),
  -- ... up to batch size
```

**Result:** ~100x faster for large datasets

---

## 3. Progress Indicators

```typescript
if (processed % 10000 === 0) {
  const pct = ((processed / total) * 100).toFixed(1);
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`Progress: ${processed}/${total} (${pct}%) - ${elapsed}s elapsed`);
}
```

---

# Testing & Validation

## Quick Validation Queries

**1. Check proposal count:**
```sql
SELECT COUNT(*) as total_proposals FROM etl.stg_proposals;
```

**2. Check hierarchy coverage:**
```sql
SELECT COUNT(*) as total_hierarchies FROM etl.stg_hierarchies;
```

**3. Check PHA records:**
```sql
SELECT COUNT(*) as total_pha FROM etl.stg_policy_hierarchy_assignments;
```

**4. Check assignment coverage:**
```sql
SELECT COUNT(*) as total_assignments FROM dbo.CommissionAssignmentVersions;
SELECT COUNT(*) as active_assignments FROM dbo.CommissionAssignmentVersions WHERE Status = 1 AND EffectiveTo IS NULL;
```

**5. Verify hierarchy participants have schedules:**
```sql
SELECT 
    COUNT(*) as total,
    COUNT(ScheduleId) as with_schedule,
    COUNT(*) - COUNT(ScheduleId) as without_schedule
FROM etl.stg_hierarchy_participants;
```

---

## Common Issues & Solutions

### Issue 1: Empty Staging Tables

**Symptom:** `stg_proposals` has 0 rows  
**Cause:** GroupId format mismatch (e.g., `0006` vs `G0006`)  
**Solution:** Check `input_certificate_info.GroupId` format

### Issue 2: Hash Collisions

**Symptom:** `Hash collision detected` error  
**Cause:** Two different hierarchies produce same SHA256 hash  
**Solution:** This is extremely rare with 64-char SHA256 (2^256 space)

### Issue 3: Missing Schedules

**Symptom:** Hierarchy participants with `ScheduleId = NULL`  
**Cause:** Schedule not found in `Schedules` table  
**Solution:** Import missing schedules from legacy data

### Issue 4: Duplicate Key Violations

**Symptom:** `Cannot insert duplicate key` during PHA generation  
**Cause:** Multiple rows with same `(PolicyId, HierarchyId, WritingBrokerId)`  
**Solution:** Aggregate source data by `(PolicyId, CertSplitSeq)` before insert

---

# Summary Statistics

## Current Production Data

| Metric | Count |
|--------|-------|
| **Certificates (Active)** | 138,812 unique |
| **Certificate Rows (with splits)** | 400,688 rows |
| **Proposals** | 8,871 |
| **Hierarchies** | 81,098 (15K conformant + 66K PHA) |
| **Hierarchy Participants** | 161,924 |
| **Policy Hierarchy Assignments** | 65,771 |
| **Commission Assignments** | 1,333 |
| **Active Assignments** | 417 |

## Data Completeness

| Category | Count | % |
|----------|-------|---|
| **Conformant Policies** | 378,265 | 89.5% |
| **Non-Conformant with PHA** | 37,433 | 8.9% |
| **Without Source Data** | 6,828 | 1.6% |
| **Commission-Ready** | **415,698** | **98.4%** |

---

**END OF PORTABLE DOCUMENTATION**
