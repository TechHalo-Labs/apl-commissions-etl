# Proposal Builder Regeneration Guide

**Purpose:** Re-run the TypeScript proposal builder to incorporate broker assignments

---

## ✅ YES - You Understand Correctly!

The proposal builder can be run **separately** from the full ETL, and you can export **only** the proposal-related data.

---

## Step-by-Step Workflow

### Step 1: Backup Current Staging Data (Recommended)

```bash
cd /Users/kennpalm/Downloads/source/APL/apl-commissions-etl

TIMESTAMP=$(date +%d%H%M)
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" \
  -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C \
  -Q "EXEC('CREATE SCHEMA [backup_proposals_${TIMESTAMP}]');"

# Backup proposal-related staging tables
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" \
  -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C -Q "
SELECT * INTO [backup_proposals_${TIMESTAMP}].[stg_proposals] 
FROM etl.stg_proposals;

SELECT * INTO [backup_proposals_${TIMESTAMP}].[stg_hierarchies] 
FROM etl.stg_hierarchies;

SELECT * INTO [backup_proposals_${TIMESTAMP}].[stg_premium_split_versions] 
FROM etl.stg_premium_split_versions;

SELECT * INTO [backup_proposals_${TIMESTAMP}].[stg_policy_hierarchy_assignments] 
FROM etl.stg_policy_hierarchy_assignments;

PRINT 'Backup complete: backup_proposals_${TIMESTAMP}';
"
```

---

### Step 2: Clear Existing Staging Data

```bash
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" \
  -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C -Q "
-- Clear in reverse dependency order
DELETE FROM etl.stg_policy_hierarchy_assignments;
DELETE FROM etl.stg_hierarchy_participants;
DELETE FROM etl.stg_hierarchy_versions;
DELETE FROM etl.stg_hierarchies;
DELETE FROM etl.stg_premium_split_participants;
DELETE FROM etl.stg_premium_split_versions;
DELETE FROM etl.stg_proposal_key_mapping;
DELETE FROM etl.stg_proposals;

PRINT 'Staging tables cleared';
"
```

---

### Step 3: Run TypeScript Proposal Builder

**Option A: Standalone (Fastest)**
```bash
cd /Users/kennpalm/Downloads/source/APL/apl-commissions-etl
time npx tsx scripts/proposal-builder.ts --verbose
```

**Option B: Via Pipeline (if you need fresh input data)**
```bash
npx tsx scripts/run-pipeline.ts --use-ts-builder --skip-export
```

**Expected Output:**
```
✓ Generated all staging entities
  Proposals: ~8,871
  Hierarchies: ~81,098
  Key Mappings: ~56,767
  PHA Records: ~65,771

Writing staging output to database...
✓ All staging data written successfully
```

---

### Step 4: Verify Staging Output

```bash
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" \
  -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C -Q "
SELECT 'stg_proposals' as entity, COUNT(*) as count 
FROM etl.stg_proposals
UNION ALL
SELECT 'stg_hierarchies', COUNT(*) FROM etl.stg_hierarchies
UNION ALL
SELECT 'stg_hierarchy_versions', COUNT(*) FROM etl.stg_hierarchy_versions
UNION ALL
SELECT 'stg_hierarchy_participants', COUNT(*) FROM etl.stg_hierarchy_participants
UNION ALL
SELECT 'stg_premium_split_versions', COUNT(*) FROM etl.stg_premium_split_versions
UNION ALL
SELECT 'stg_premium_split_participants', COUNT(*) FROM etl.stg_premium_split_participants
UNION ALL
SELECT 'stg_policy_hierarchy_assignments', COUNT(*) FROM etl.stg_policy_hierarchy_assignments
UNION ALL
SELECT 'stg_commission_assignment_versions', COUNT(*) FROM etl.stg_commission_assignment_versions
UNION ALL
SELECT 'stg_commission_assignment_recipients', COUNT(*) FROM etl.stg_commission_assignment_recipients
ORDER BY entity;
"
```

**Expected Counts:**
```
stg_proposals: ~8,871
stg_hierarchies: ~81,098
stg_hierarchy_versions: ~81,098
stg_hierarchy_participants: ~161,924
stg_premium_split_versions: ~8,871
stg_premium_split_participants: ~15,327
stg_policy_hierarchy_assignments: ~65,771
stg_commission_assignment_versions: TBD (new)
stg_commission_assignment_recipients: TBD (new)
```

---

### Step 5: Clear Production Tables (Be Careful!)

```bash
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" \
  -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C -Q "
-- Clear in reverse dependency order
DELETE FROM dbo.PolicyHierarchyAssignments;
DELETE FROM dbo.HierarchyParticipants;
DELETE FROM dbo.HierarchyVersions;
DELETE FROM dbo.Hierarchies;
DELETE FROM dbo.PremiumSplitParticipants;
DELETE FROM dbo.PremiumSplitVersions;
DELETE FROM dbo.Proposals;
DELETE FROM dbo.CommissionAssignmentRecipients;
DELETE FROM dbo.CommissionAssignmentVersions;

PRINT 'Production tables cleared';
"
```

---

### Step 6: Export Proposal-Related Data Only

**Option A: Export All at Once via Pipeline**
```bash
npx tsx scripts/run-pipeline.ts --export-only
```

**Option B: Export Selectively (Individual Scripts)**
```bash
# Export in dependency order

# 1. Proposals
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" \
  -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C \
  -v ETL_SCHEMA=etl -v PRODUCTION_SCHEMA=dbo \
  -i sql/export/07-export-proposals.sql

# 2. Hierarchies (includes versions, participants)
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" \
  -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C \
  -v ETL_SCHEMA=etl -v PRODUCTION_SCHEMA=dbo \
  -i sql/export/08-export-hierarchies.sql

# 3. Premium Splits (versions & participants)
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" \
  -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C \
  -v ETL_SCHEMA=etl -v PRODUCTION_SCHEMA=dbo \
  -i sql/export/11-export-splits.sql

# 4. Policy Hierarchy Assignments
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" \
  -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C \
  -v ETL_SCHEMA=etl -v PRODUCTION_SCHEMA=dbo \
  -i sql/export/14-export-policy-hierarchy-assignments.sql

# 5. Commission Assignments (NEW)
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" \
  -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C \
  -v ETL_SCHEMA=etl -v PRODUCTION_SCHEMA=dbo \
  -i sql/export/12-export-assignments.sql
```

---

### Step 7: Verify Production Output

```bash
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" \
  -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C -Q "
SELECT 'Proposals' as entity, COUNT(*) as count 
FROM dbo.Proposals
UNION ALL
SELECT 'Hierarchies', COUNT(*) FROM dbo.Hierarchies
UNION ALL
SELECT 'HierarchyVersions', COUNT(*) FROM dbo.HierarchyVersions
UNION ALL
SELECT 'HierarchyParticipants', COUNT(*) FROM dbo.HierarchyParticipants
UNION ALL
SELECT 'PremiumSplitVersions', COUNT(*) FROM dbo.PremiumSplitVersions
UNION ALL
SELECT 'PremiumSplitParticipants', COUNT(*) FROM dbo.PremiumSplitParticipants
UNION ALL
SELECT 'PolicyHierarchyAssignments', COUNT(*) FROM dbo.PolicyHierarchyAssignments
UNION ALL
SELECT 'CommissionAssignmentVersions', COUNT(*) FROM dbo.CommissionAssignmentVersions
UNION ALL
SELECT 'CommissionAssignmentRecipients', COUNT(*) FROM dbo.CommissionAssignmentRecipients
ORDER BY entity;
"
```

---

## What Gets Regenerated

### Staging Tables (Cleared & Regenerated)
- ✅ `stg_proposals` - Proposal entities
- ✅ `stg_proposal_key_mapping` - Certificate→Proposal lookup
- ✅ `stg_premium_split_versions` - Split configurations
- ✅ `stg_premium_split_participants` - Brokers in splits
- ✅ `stg_hierarchies` - Hierarchy entities
- ✅ `stg_hierarchy_versions` - Hierarchy versions
- ✅ `stg_hierarchy_participants` - Brokers in hierarchies
- ✅ `stg_policy_hierarchy_assignments` - Policy→Hierarchy links (PHA)
- ✅ `stg_commission_assignment_versions` - Assignment metadata **NEW**
- ✅ `stg_commission_assignment_recipients` - Assignment recipients **NEW**

### Production Tables (Cleared & Re-Exported)
- ✅ `dbo.Proposals`
- ✅ `dbo.Hierarchies`
- ✅ `dbo.HierarchyVersions`
- ✅ `dbo.HierarchyParticipants`
- ✅ `dbo.PremiumSplitVersions`
- ✅ `dbo.PremiumSplitParticipants`
- ✅ `dbo.PolicyHierarchyAssignments`
- ✅ `dbo.CommissionAssignmentVersions` **NEW**
- ✅ `dbo.CommissionAssignmentRecipients` **NEW**

### Tables NOT Affected
- ✅ `dbo.Policies` - Left intact
- ✅ `dbo.EmployerGroups` - Left intact
- ✅ `dbo.Brokers` - Left intact
- ✅ `dbo.Schedules` - Left intact
- ✅ `dbo.ScheduleRates` - Left intact

---

## Broker Assignment Capture Status

### Current State: ⚠️ NOT CAPTURED

**Source Data Available:**
- 45.63% of certificate records have `ReassignedType` (277,126 out of 607,355)
  - `Assigned`: 251,681 records (41.44%)
  - `Transferred`: 25,445 records (4.19%)
- 13.22% of "Assigned" records have `WritingBrokerId ≠ SplitBrokerId`
- 52.72% of "Transferred" records have different brokers

**Not Yet Implemented:**
- TypeScript proposal builder does NOT currently extract assignments
- Staging tables `stg_commission_assignment_*` are empty (0 rows)
- No export of assignment data to production

### Required Changes to Capture Assignments

The proposal builder needs to be modified to:
1. Extract `ReassignedType` from `CertificateInfo`
2. Identify assignment relationships (WritingBrokerId vs SplitBrokerId)
3. Generate `CommissionAssignmentVersions` for each assignment
4. Generate `CommissionAssignmentRecipients` for each recipient broker
5. Link assignments to hierarchies/participants

---

## Key Benefits of Standalone Execution

✅ **Fast** - Only regenerates proposals (~3-5 minutes)  
✅ **Safe** - Doesn't touch other ETL data (policies, groups, brokers)  
✅ **Flexible** - Can export selectively or all at once  
✅ **Testable** - Easy to verify changes in staging before export

---

## When to Re-Run

You need to re-run the proposal builder when:
1. ✅ Source data changes (new certificates in `input_certificate_info`)
2. ✅ Business logic changes (grouping rules, split logic)
3. ✅ **NEW FEATURE** - To capture broker assignments (requires code changes)
4. ✅ Bug fixes in proposal generation logic

---

## Quick Commands Summary

```bash
# Backup current state
TIMESTAMP=$(date +%d%H%M)
sqlcmd ... -Q "EXEC('CREATE SCHEMA [backup_proposals_${TIMESTAMP}]');"

# Clear staging
sqlcmd ... -Q "DELETE FROM etl.stg_proposals; ..."

# Regenerate
npx tsx scripts/proposal-builder.ts --verbose

# Verify staging
sqlcmd ... -Q "SELECT 'stg_proposals' as entity, COUNT(*) ..."

# Clear production
sqlcmd ... -Q "DELETE FROM dbo.Proposals; ..."

# Export
npx tsx scripts/run-pipeline.ts --export-only

# Verify production
sqlcmd ... -Q "SELECT 'Proposals' as entity, COUNT(*) ..."
```

---

**Status:** Ready to execute when needed  
**Impact:** Proposal-related entities only  
**Safety:** Backup recommended before clearing production
