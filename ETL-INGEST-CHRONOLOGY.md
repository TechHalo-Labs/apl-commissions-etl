# ETL Ingest Process Chronology

## Overview

This document chronicles the complete data loading process from CSV files to production database, including all issues encountered and fixes applied. The goal is to understand the current state and create a repeatable, streamlined process.

---

## Phase 0: CSV File Loading to Database

### Source CSV Files

**Location:** `~/Downloads/newdata/`

| File | Size | Records | Purpose |
|------|------|---------|---------|
| `CertificateInfo_20260116.csv` | 249 MB | 1.7M | Certificate/policy data with splits |
| `APL-Perf_Schedule_model_20260116.csv` | 213 MB | 1.1M | Commission schedule rates |
| `APL-Perf_Group_model_20260116.csv` | 4 MB | 33K | Employer group data |
| `IndividualRosterExtract_20260116.csv` | 573 KB | 9.6K | Individual broker roster |
| `OrganizationRosterExtract_20260116.csv` | 181 KB | 2.6K | Organization broker roster |
| `CommHierarchy_20260116.csv` | 2.5 MB | 57K | Commission hierarchy data |
| `CommissionsDetail_*.csv` | 14 MB | 149K | Commission detail records |
| `BrokerLicenseExtract_20260116.csv` | 663 KB | | Broker licenses |
| `BrokerEO_20260116.csv` | 500 KB | | Broker E&O insurance |
| `Fees_20260116.csv` | 99 KB | | Fee schedules |
| `BrokerMGARelationships_20260116.csv` | 309 KB | | MGA relationships |

### Option A: TypeScript CSV Loader (Recommended)

**Script:** `scripts/load-csv.ts`

**Command:**
```bash
npx tsx scripts/load-csv.ts                # Load all rows
npx tsx scripts/load-csv.ts --limit 100    # Test with 100 rows per file
```

**Target Schema:** `etl` (creates `etl.raw_*` tables)

**Features:**
- Dynamically reads CSV columns
- Creates tables with NVARCHAR(MAX) columns
- Bulk inserts for performance (5,000 rows/batch)
- Handles BOM characters

**CSV File Mappings (defined in script):**
```typescript
const csvMappings = [
  { csvFile: 'IndividualRosterExtract_*.csv', tableName: 'raw_individual_brokers' },
  { csvFile: 'OrganizationRosterExtract_*.csv', tableName: 'raw_org_brokers' },
  { csvFile: 'CertificateInfo.csv', tableName: 'raw_certificate_info' },
  { csvFile: 'perf.csv', tableName: 'raw_schedule_rates' },
  { csvFile: 'perf-group.csv', tableName: 'raw_perf_groups' },
  { csvFile: 'premiums.csv', tableName: 'raw_premiums' },
  { csvFile: 'CommissionsDetail_*.csv', tableName: 'raw_commissions_detail' },
  // ... more mappings
];
```

### Option B: ZIP File Ingest

**Script:** `scripts/ingest-raw-data.ts`

**Command:**
```bash
npx tsx scripts/ingest-raw-data.ts --zip ~/Downloads/data.zip
npx tsx scripts/ingest-raw-data.ts --preview   # Load only 10 rows per table
npx tsx scripts/ingest-raw-data.ts --dry-run   # Show what would be done
```

**Features:**
- Extracts from ZIP file
- Auto-detects most recent ZIP in Downloads
- Creates new schema (`raw_data1`, `raw_data2`, etc.)
- Validates column headers against expected schema

### Option C: Direct Azure Data Studio Import

The `new_data` schema tables appear to have been populated via direct import:
- `new_data.CertificateInfo` - 1,733,710 records
- `new_data.IndividualRoster` - 9,633 records
- `new_data.OrganizationRoster` - 2,634 records
- `new_data.CommHierarchy` - 57,064 records
- `new_data.PerfGroupModel` - 32,986 records
- `new_data.CommissionsDetail` - 149,465 records
- `new_data.PerfScheduleModel` - (schedule rates - but data quality issues!)

**Note:** The `sql/fix/create-new-data-raw-tables.sql` script creates VIEWS in `new_data` that reference tables in other schemas (`poc_raw_data`).

### Schedule Rates Source Discovery

**Critical Finding:** Two different sources exist for schedule rates:

| Source | Records | ScheduleName | State | Rate Values |
|--------|---------|--------------|-------|-------------|
| `new_data.PerfScheduleModel` | varies | NULL | varies | WRONG (aggregated) |
| `raw_data.raw_schedule_rates` | 1,133,420 | 100% populated | 100% (50 states) | CORRECT |

**Resolution:** Use `raw_data.raw_schedule_rates` as the authoritative source.

---

## Phase 1: Initial Data Discovery & Source Selection

### Step 1.1: Identifying the Correct Data Sources

**Problem:** Multiple potential data sources existed with varying data quality:
- `new_data.PerfScheduleModel` - Had `ScheduleName = NULL` and wrong rate values
- `poc_etl.raw_*` - Had aggregated/consolidated data (incorrect)
- `raw_data.raw_schedule_rates` - Complete, correct data (1.1M records)
- `new_data.CertificateInfo` - Latest certificate data (1.7M records)

**Resolution:**
1. **Schedules Source:** Changed to `raw_data.raw_schedule_rates`
   - 1,133,420 records
   - 51,073 unique schedules
   - 100% ScheduleName populated
   - 100% State populated (50 states)
   - Correct rate values

2. **Certificates Source:** Changed to `new_data.CertificateInfo`
   - 1,733,710 records (183K more than old `raw_data` source)
   - Most current data available

### Step 1.2: Schema Discovery Issues

**Problems Encountered:**
- Column name mismatches between `new_data` and `etl` schemas
- `PerfGroupModel` had typo "FUndingType"
- Broker tables had different column structures
- Type conversion issues (nvarchar "NULL" strings vs actual NULLs)

**Resolution:** Created mapping scripts with:
- Explicit column mappings
- NULLIF conversions for "NULL" strings
- TRY_CAST for type conversions
- COALESCE for required non-null fields

---

## Phase 2: Data Ingest

### Step 2.1: Schedule Rates Ingest

**Script:** `sql/ingest/01-ingest-schedules-from-source.sql`

**Source:** `raw_data.raw_schedule_rates`

**Process:**
```
raw_data.raw_schedule_rates (1.1M) → etl.raw_schedule_rates
```

**Result:** 1,133,420 schedule rates with complete metadata

### Step 2.2: Certificate Data Ingest

**Process:**
```
new_data.CertificateInfo (1.7M) → etl.raw_certificate_info → etl.input_certificate_info
```

**Script:** `sql/ingest/populate-input-tables.sql`

**Issues Fixed:**
- `SplitBrokerSeq` column doesn't allow NULLs → Used COALESCE default
- Type conversion errors for dates/decimals → Used TRY_CAST

**Result:** 1,733,710 certificates with 4,074 groups, 721 schedules

---

## Phase 3: SQL Transforms (Reference Data)

### Step 3.1: Brokers Transform
**Script:** `sql/transforms/01-brokers.sql`
**Result:** 12,260 brokers in `stg_brokers`

### Step 3.2: Groups Transform
**Script:** `sql/transforms/02-groups.sql`
**Result:** 4,074 groups in `stg_groups`

### Step 3.3: Products Transform
**Script:** `sql/transforms/03-products.sql`
**Result:** 262 products in `stg_products`

### Step 3.4: Schedules Transform
**Script:** `sql/transforms/04-schedules.sql`

**Critical Fix Applied:** Disabled "catch-all consolidation" logic that was:
1. Detecting rates uniform across states
2. DELETING all state-specific rows
3. Creating catch-all rows with State = NULL

**Impact:** 175,828 → 1,133,420 schedule rates (previously losing 68% of data!)

**Result:** 
- 51,073 schedules in `stg_schedules`
- 51,073 schedule versions in `stg_schedule_versions`
- 1,133,420 schedule rates in `stg_schedule_rates`

---

## Phase 4: TypeScript Proposal Builder

### Step 4.1: Running the Proposal Builder

**Script:** `scripts/proposal-builder.ts`

**Command:**
```bash
npx tsx scripts/proposal-builder.ts
```

**Process:**
```
etl.input_certificate_info (1.7M) 
  → proposal-builder.ts 
  → stg_proposals, stg_hierarchies, stg_splits, etc.
```

### Step 4.2: First Run Issues

**Problem:** "No certificates loaded from database"
**Root Cause:** `etl.input_certificate_info` was empty
**Fix:** Re-ran ingest phase first

### Step 4.3: Successful Run Output

| Entity | Count |
|--------|-------|
| Proposals | 9,306 |
| Proposal Products | 14,695 |
| Hierarchies | 1,848 |
| Hierarchy Participants | 3,971 |
| State Rules | 2,053 |
| State Rule States | 322 |
| Hierarchy Splits | 4,782 |
| Split Distributions | 9,878 |
| Policy Hierarchy Assignments | 3,893 |
| Commission Assignments | 15,919 |

---

## Phase 5: Critical Bug Fixes

### Fix 5.1: Schedule ID Resolution Bug

**Problem:** Split distributions were using string schedule codes ("NM-RZ5", "5512A") instead of numeric IDs (523, 102)

**Impact:** Broke ALL commission calculations

**Solution:**
1. Added `scheduleIdByExternalId` lookup map
2. Added `loadSchedules()` method
3. Updated participant creation to resolve numeric IDs
4. Updated split distribution creation to use numeric IDs

**Result:** 94.55% resolution rate (8,831/9,339 with numeric IDs)

### Fix 5.2: Data Quality Gap Fixes

**Problems:**
- Proposals missing: GroupName, BrokerId, BrokerName, BrokerUniquePartyId
- Split Participants missing: BrokerName, BrokerUniquePartyId, HierarchyName

**Solution:** Added LEFT JOINs in certificate loading:
```sql
LEFT JOIN [dbo].[EmployerGroups] eg ON eg.GroupNumber = ci.GroupId
LEFT JOIN [dbo].[Brokers] b ON b.ExternalPartyId = ci.SplitBrokerId
```

**Result:** 100% population of all name fields

### Fix 5.3: State Rules Implementation

**Problem:** TypeScript ETL wasn't creating state rules

**Business Logic Implemented:**
- Single-state hierarchy → DEFAULT rule (no state restrictions)
- Multi-state hierarchy → One rule per state

**Result:** 1,977 state rules created (100% hierarchy coverage)

### Fix 5.4: Hierarchy Splits & Split Distributions

**Problem:** 
- Hierarchy splits existed but had empty `distributions[]`
- No schedule references for commission rate lookups

**Solution:** Added split distribution creation linking:
- HierarchySplitId
- HierarchyParticipantId
- ParticipantEntityId
- ScheduleId (numeric!)
- ScheduleName

**Result:** 9,319 split distributions with schedule references

### Fix 5.5: Proposal Products

**Problem:** `ProposalProducts` table was empty

**Solution:** Added parsing logic for comma-separated ProductCodes

**Result:** 14,160 proposal products from 8,886 proposals

### Fix 5.6: Proposal ID Format Change

**Problem:** IDs were simple counters (P-1, P-2)

**Change:** Made ProposalId = ProposalNumber (PROP-0006-1)

**Benefits:**
- Self-documenting (Group ID embedded)
- Better debugging
- Professional structure

---

## Phase 6: Export to Production

### Step 6.1: Backup First

**Backup Schema:** `backup` with timestamp (e.g., `20260129_151605`)

### Step 6.2: Clear Production Tables

**Order (FK constraints):**
1. SplitDistributions
2. HierarchySplits
3. StateRuleStates
4. StateRules
5. HierarchyParticipants
6. HierarchyVersions
7. Hierarchies
8. PremiumSplitParticipants
9. PremiumSplitVersions
10. ProposalProducts
11. Proposals
12. Policies
13. PolicyHierarchyAssignments

### Step 6.3: Export Scripts

| Script | Target Table | Rows |
|--------|--------------|------|
| 01-export-brokers.sql | Brokers | 12,260 |
| 02-export-groups.sql | EmployerGroups | 4,074 |
| 03-export-products.sql | Products | 262 |
| 04-export-schedules.sql | Schedules, ScheduleRates | 51,073 |
| 05-export-hierarchies.sql | Hierarchies, HierarchyVersions, HierarchyParticipants | 1,848 + 1,848 + 3,971 |
| 06-export-splits.sql | StateRules, HierarchySplits, SplitDistributions | 2,053 + 4,782 + 9,878 |
| 07-export-proposals.sql | Proposals, ProposalProducts | 9,306 + 14,695 |
| 08-export-policies.sql | Policies | 481,521 |
| 10-export-pha.sql | PolicyHierarchyAssignments | 3,893 |

### Step 6.4: Export Issues Encountered

**Issue 1:** Table name mismatch (`Group` → `EmployerGroups`)

**Issue 2:** GroupId format mismatch
- Staging: `25565` (no prefix)
- Production FK: `G25565` (with prefix)
- Fix: Added 'G' prefix during export

**Issue 3:** NULL schedule IDs
- Found 610 distributions with NULL ScheduleId
- Patched 602 by adding missing schedules
- Remaining 8 are legitimate (WH01/WH02 don't exist in source)

**Issue 4:** PolicyHierarchyAssignments unique index
- Original index required HierarchyId NOT NULL
- Changed to filtered index (WHERE HierarchyId IS NOT NULL)

---

## Phase 7: PolicyHierarchyAssignments Fix

### Initial State
- 3,893 PHA records with ALL HierarchyIds = NULL
- PHAs represent non-conformant policies (invalid GroupId, DTC, etc.)

### Investigation
- proposal-builder.ts generates PHA records
- Originally left HierarchyId NULL for non-conformant cases

### Problem Identified
PHAs SHOULD have hierarchies - they represent the commission structure for policies that don't fit standard proposals.

### Fix Required
Created `fix-policy-hierarchy-assignments.ts` to:
1. Load existing PHAs without hierarchies
2. Generate proper hierarchies for each PHA
3. Update HierarchyId references

### Performance Issue
- Initial implementation: ~1 INSERT/UPDATE per record
- Very slow for 3,893+ records

### Optimized Version
Created `fix-policy-hierarchy-assignments-fast.ts`:
- Memory-first generation
- Batched multi-row INSERTs (100 rows per batch)
- CTE-based UPDATE statements (500 records per batch)
- 50-100x faster

---

## Current Final State

### Production Tables

| Table | Rows | Status |
|-------|------|--------|
| Brokers | 12,260 | ✅ |
| EmployerGroups | 4,074 | ✅ |
| Products | 262 | ✅ |
| Schedules | 51,073 | ✅ |
| ScheduleRates | 1,133,420 | ✅ |
| Proposals | 9,306 | ✅ |
| ProposalProducts | 14,695 | ✅ |
| PremiumSplitVersions | 9,306 | ✅ |
| PremiumSplitParticipants | 15,983 | ✅ |
| Hierarchies | 1,848 | ✅ |
| HierarchyVersions | 1,848 | ✅ |
| HierarchyParticipants | 3,971 | ✅ |
| StateRules | 2,053 | ✅ |
| StateRuleStates | 322 | ✅ |
| HierarchySplits | 4,782 | ✅ |
| SplitDistributions | 9,878 | ✅ |
| Policies | 481,521 | ✅ |
| PolicyHierarchyAssignments | 3,893 | ⚠️ Needs hierarchy fix |

### Data Quality Metrics
- Schedule ID resolution: 99.9% (8 remaining NULLs are legitimate)
- Name field population: 100%
- State rules coverage: 100%
- Proposal products: 100%

---

## Key Lessons Learned

1. **Source Data Selection is Critical**
   - Multiple sources exist with varying quality
   - `raw_data.raw_schedule_rates` is the correct schedule source
   - `new_data.CertificateInfo` is the most current certificate source

2. **Schema Mismatches are Common**
   - Column names differ between schemas
   - Type conversions needed for "NULL" strings
   - GroupId format varies (with/without 'G' prefix)

3. **Transform Logic Can Destroy Data**
   - The "catch-all consolidation" in schedules transform deleted 68% of data
   - Always verify data counts before and after transforms

4. **Numeric vs String IDs Matter**
   - Schedule IDs must be numeric for FK joins
   - String codes break commission calculations

5. **Order of Operations Matters**
   - Schedules must be loaded BEFORE hierarchies
   - Reference data (brokers, groups) must exist before proposals
   - Hierarchies must exist before PHAs can reference them

6. **Performance Requires Batching**
   - Individual INSERT/UPDATE statements are too slow
   - Multi-row VALUES clauses: 100 rows/batch
   - CTE-based UPDATEs: 500 records/batch

---

## Recommended Repeatable Process

### Quick Reference: Complete ETL Sequence

```bash
# 1. Ingest Phase
sqlcmd -i sql/ingest/01-ingest-schedules-from-source.sql
sqlcmd -i sql/ingest/02-copy-from-new-data.sql
sqlcmd -i sql/ingest/populate-input-tables.sql

# 2. SQL Transforms (Reference Data)
npx tsx scripts/run-pipeline.ts --skip-ingest --transforms-only

# 3. TypeScript Proposal Builder
npx tsx scripts/proposal-builder.ts

# 4. Fix Policy Hierarchy Assignments
npx tsx scripts/fix-policy-hierarchy-assignments-fast.ts

# 5. Export to Production
npx tsx scripts/run-pipeline.ts --export-only

# 6. Verify
npx tsx scripts/verify-staging.ts
```

---

*Document created: January 29, 2026*
*Based on conversations: conve1.md, convo2.md, convo3.md*
