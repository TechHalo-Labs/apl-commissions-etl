# Proposal Builder Versions - Which Is Used?

**Answer:** `proposal-builder.ts` is integrated into the pipeline.

---

## File Overview

| File | Lines | Status | Purpose |
|------|-------|--------|---------|
| **`proposal-builder.ts`** | 1,735 | ✅ **ACTIVE** | Main version integrated into `run-pipeline.ts` |
| `proposal-builder-bulk.ts` | 344 | 📦 **MODULE** | Standalone bulk insert helper (not imported) |
| `proposal-builder-chunked.ts` | 325 | 🧪 **EXPERIMENTAL** | Memory-efficient chunked processing (not used) |

---

## Integration in Pipeline

**File:** `scripts/run-pipeline.ts` (line 479)

```typescript
const { runProposalBuilder } = require('./proposal-builder');
```

The pipeline **only imports from `proposal-builder.ts`** - the "simple" one you mentioned (though it's actually 1,735 lines).

---

## Relationship Between Files

### proposal-builder.ts (Main - ACTIVE)
- **1,735 lines** of full-featured implementation
- **Integrated** into `run-pipeline.ts`
- **Has bulk inserts built-in** (despite the comment mentioning the bulk file)
- Exports `runProposalBuilder()` function
- Generates all 9 staging tables:
  1. `stg_proposals`
  2. `stg_proposal_key_mapping`
  3. `stg_premium_split_versions`
  4. `stg_premium_split_participants`
  5. `stg_hierarchies`
  6. `stg_hierarchy_versions`
  7. `stg_hierarchy_participants`
  8. `stg_policy_hierarchy_assignments`
  9. `stg_policy_hierarchy_participants`

**Key Features:**
- Full SHA256 hash (64 chars) for collision safety
- Collision detection
- Batched processing
- Audit logging
- CLI flags for debugging and dry-run

**Misleading Comment:**
```typescript
// Line 4-5 in proposal-builder.ts:
* NOTE: This file now uses BULK INSERTS for optimal performance.
* See proposal-builder-bulk.ts for the optimized write implementation.
```
This comment is **outdated/misleading** - the bulk insert code is **already incorporated inline**, not imported from the bulk file.

---

### proposal-builder-bulk.ts (Module - NOT IMPORTED)
- **344 lines** of standalone module
- Exports `writeStagingOutputBulk()` function
- **Not imported** by the main proposal-builder.ts
- Appears to be an **earlier prototype** or **reference implementation**
- Shows bulk insert optimization approach

**Purpose:** Likely used during development to prototype the bulk insert optimization, which was then incorporated directly into the main file.

---

### proposal-builder-chunked.ts (Experimental - NOT USED)
- **325 lines** of memory-efficient processing
- **Imports** `runProposalBuilder` from the main file
- Processes groups in batches (default: 100 groups at a time)
- **Not integrated** into the pipeline
- Can be run standalone for memory-constrained scenarios

**Use Case:** 
```bash
# If you ever need memory-efficient processing:
npx tsx scripts/proposal-builder-chunked.ts --chunkSize 50
```

---

## How to Tell Which Is Active

### Method 1: Check Pipeline Import
```bash
grep "proposal-builder" scripts/run-pipeline.ts
```
**Result:** `require('./proposal-builder')` ← Uses the main file

### Method 2: Check Module Exports
```bash
grep "export.*runProposalBuilder" scripts/proposal-builder*.ts
```
**Result:**
- `proposal-builder.ts`: ✅ Exports `runProposalBuilder`
- `proposal-builder-bulk.ts`: ❌ Exports only `writeStagingOutputBulk`
- `proposal-builder-chunked.ts`: ❌ Imports, doesn't export

---

## Current Active Implementation

**File:** `scripts/proposal-builder.ts`

**Key Functions:**
1. `loadCertificates()` - Loads from `input_certificate_info`
2. `buildProposals()` - Main processing logic
3. `writeStagingOutput()` - **Built-in bulk insert implementation**
4. `runProposalBuilder()` - **Main entry point** (used by pipeline)

**Execution:**
```bash
# Standalone
npx tsx scripts/proposal-builder.ts --verbose

# Via pipeline
npx tsx scripts/run-pipeline.ts --use-ts-builder --skip-export
```

---

## Broker Assignment Support

**Current Status:** ⚠️ **NOT YET IMPLEMENTED**

The main `proposal-builder.ts` has:
- ✅ Interface defined: `ProposalAssignment` (lines 40-45)
- ✅ Type structure ready
- ❌ No extraction logic for `ReassignedType` from CertificateInfo
- ❌ No generation of `stg_commission_assignment_*` tables

**To Add Broker Assignments:**
You would modify **`proposal-builder.ts`** (the main file) to:
1. Extract `ReassignedType` from certificate data
2. Identify `WritingBrokerId ≠ SplitBrokerId` cases
3. Generate `CommissionAssignmentVersions` and `CommissionAssignmentRecipients`

---

## Summary

✅ **`proposal-builder.ts`** is the one integrated into the pipeline  
📦 **`proposal-builder-bulk.ts`** is a standalone module (not imported)  
🧪 **`proposal-builder-chunked.ts`** is an experimental alternative (not used)  

**Why it might look "simple":**
- The file name doesn't indicate it's the main one
- The comment references the bulk file (misleading)
- But it's actually the **most complete** implementation at 1,735 lines

**Bottom line:** When you want to modify the proposal builder for broker assignments, **edit `proposal-builder.ts`** - it's the active version.
