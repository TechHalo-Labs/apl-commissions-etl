# ETL Pipeline: Execution Flow Diagram

## Complete Pipeline Flow

```
┌────────────────────────────────────────────────────────────────────┐
│                    ETL PIPELINE ORCHESTRATOR                       │
│                  (scripts/run-pipeline.ts)                         │
└────────────────────────────────────────────────────────────────────┘
                              │
                              ├─ Flag: --step-by-step?
                              │  ├─ YES → Manual Mode (pauses between steps)
                              │  └─ NO  → Auto Mode (continuous)
                              │
        ┌─────────────────────┼─────────────────────┬─────────────────┐
        │                     │                     │                 │
        ▼                     ▼                     ▼                 ▼
┌───────────────┐   ┌──────────────────┐   ┌──────────────┐   ┌──────────┐
│  PHASE 1:     │   │  PHASE 2:        │   │  PHASE 3:    │   │ PHASE 4: │
│  Schema Setup │   │  Data Ingest     │   │  Transforms  │   │  Export  │
└───────────────┘   └──────────────────┘   └──────────────┘   └──────────┘
      │                     │                      │                │
      │ --skip-schema       │ --skip-ingest        │ --skip-        │ --skip-
      │                     │                      │ transform       │ export
      │                     │                      │                │
      ▼                     ▼                      ▼                ▼
 [Optional]          [New Feature!]         [Core Pipeline]    [Production]
```

---

## Phase 2: Data Ingest (NEW)

```
┌─────────────────────────────────────────────────────────────┐
│                    INGEST PHASE                             │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
            ┌─────────────────────────┐
            │  Step 1: Copy Raw Data  │
            │  (copy-from-poc-etl.sql)│
            └─────────────────────────┘
                          │
            poc_etl schema → etl.raw_* tables
                          │
            - raw_certificate_info: 1.5M rows
            - raw_schedule_rates: 1.1M rows
            - raw_perf_groups: 33K rows
            - raw_premiums: 138K rows
            - raw_brokers: 12K rows
                          │
                          ├─ --step-by-step?
                          │  ├─ YES → Pause for verification
                          │  └─ NO  → Continue
                          │
                          ▼
       ┌──────────────────────────────────────┐
       │  Step 2: Populate Input Tables       │
       │  (populate-input-tables.sql)         │
       └──────────────────────────────────────┘
                          │
         etl.raw_* → etl.input_certificate_info
                          │
                          ├─ --step-by-step?
                          │  ├─ YES → Pause for verification
                          │  └─ NO  → Continue to Transform
                          │
                          ▼
                   [Transform Phase]
```

---

## Phase 3: Transforms

```
┌─────────────────────────────────────────────────────────────┐
│                  TRANSFORM PHASE                            │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
        ┌──────────────────────────────────┐
        │  00-references.sql               │
        │  (States, Products)              │
        └──────────────────────────────────┘
                          │
                          ├─ Pause? → Continue
                          ▼
        ┌──────────────────────────────────┐
        │  01-brokers.sql                  │
        │  (Individual + Org Brokers)      │
        └──────────────────────────────────┘
                          │
                          ├─ Pause? → Continue
                          ▼
        ┌──────────────────────────────────┐
        │  02-groups.sql                   │
        │  (Employer Groups + PrimaryBrokerID)│
        └──────────────────────────────────┘
                          │
                          ├─ Pause? → Continue
                          ▼
        ┌──────────────────────────────────┐
        │  03-products.sql                 │
        │  (Product Catalog)               │
        └──────────────────────────────────┘
                          │
                          ├─ Pause? → Continue
                          ▼
        ┌──────────────────────────────────┐
        │  04-schedules.sql ⭐ CRITICAL     │
        │  (Schedule + Rates)              │
        │  Expected: 600+ schedules        │
        └──────────────────────────────────┘
                          │
                          ├─ Pause? → Verify schedules created!
                          ▼
        ┌──────────────────────────────────┐
        │  06a-06g: Proposals              │
        │  (Multi-tiered proposal creation)│
        └──────────────────────────────────┘
                          │
                          ├─ Pause? → Continue
                          ▼
        ┌──────────────────────────────────┐
        │  07-hierarchies.sql ⭐ CRITICAL   │
        │  (Hierarchies + Participants)    │
        │  Must link ScheduleId!           │
        └──────────────────────────────────┘
                          │
                          ├─ Pause? → Verify >95% ScheduleId linked!
                          ▼
        ┌──────────────────────────────────┐
        │  08-hierarchy-splits.sql         │
        │  (Premium Split Versions)        │
        └──────────────────────────────────┘
                          │
                          ▼
        ┌──────────────────────────────────┐
        │  09-policies.sql                 │
        │  (Policy/Certificate Transform)  │
        └──────────────────────────────────┘
                          │
                          ▼
        ┌──────────────────────────────────┐
        │  10-premium-transactions.sql     │
        │  (Premium Data)                  │
        └──────────────────────────────────┘
                          │
                          ▼
        ┌──────────────────────────────────┐
        │  11-policy-hierarchy-assignments │
        │  (PHA for non-conformant)        │
        └──────────────────────────────────┘
                          │
                          ├─ Pause? → Verify PHA has ScheduleId!
                          ▼
        ┌──────────────────────────────────┐
        │  99-audit-and-cleanup.sql ⭐     │
        │  (Final data quality checks)     │
        └──────────────────────────────────┘
                          │
                          ├─ Pause? → Final verification!
                          ▼
                   [Export Phase]
```

---

## Execution Mode Comparison

### Auto Mode (Default):
```
Step 1 ✅ → Step 2 ✅ → Step 3 ✅ → ... → Step N ✅
         (continuous execution)
```

### Manual Mode (--step-by-step):
```
Step 1 ✅ → [PAUSE: Verify] → User: 'y' → Step 2 ✅ → [PAUSE: Verify] → ...
         (wait for confirmation)
```

---

## Decision Tree: Which Command to Use?

```
START: What do you want to do?
    │
    ├─ First time running pipeline?
    │  └─ YES → npx tsx scripts/run-pipeline.ts --step-by-step
    │
    ├─ Testing transform changes?
    │  └─ YES → npx tsx scripts/run-pipeline.ts --step-by-step --transforms-only
    │
    ├─ Data already loaded (just re-transform)?
    │  └─ YES → npx tsx scripts/run-pipeline.ts --skip-ingest
    │
    ├─ Production/automated run?
    │  └─ YES → npx tsx scripts/run-pipeline.ts
    │
    ├─ Need detailed verification per step?
    │  ├─ Ingest? → npx tsx scripts/run-ingest-step-by-step.ts
    │  └─ Transform? → npx tsx scripts/run-transforms-step-by-step.ts
    │
    └─ Just need schedules fixed?
       └─ YES → See Issue 4 fix: sql/fix/copy-all-raw-from-poc-etl.sql
```

---

## Critical Checkpoints (Manual Mode)

When running in `--step-by-step` mode, **pay special attention** at these steps:

### ⭐ Checkpoint 1: After `04-schedules.sql`
**Verify:**
- Schedules created: Should be 600+ (expect ~686)
- Schedule rates: Should be 10K+ (expect ~10,090)

**If NOT:** 
- Check `raw_schedule_rates` is populated (1.1M rows)
- Check `input_certificate_info` has CommissionsSchedule values

---

### ⭐ Checkpoint 2: After `07-hierarchies.sql`
**Verify:**
- Hierarchy participants: >95% have ScheduleId
- If <95%: Check unmatched ScheduleCodes (should show in logs)

**If NOT:**
- Schedules may not be matching on ExternalId
- Audit script (99-audit-and-cleanup.sql) will attempt fallback matching

---

### ⭐ Checkpoint 3: After `11-policy-hierarchy-assignments.sql`
**Verify:**
- PHA participants have ScheduleId linked
- Multiple PHA per policy for multi-earning certificates

**If NOT:**
- Non-conformant policies may not be getting schedules
- Check if PHA participants reference valid HierarchyParticipants

---

### ⭐ Checkpoint 4: After `99-audit-and-cleanup.sql`
**Verify:**
- Broker ID population: >95%
- Schedule linking: >95%
- All staging tables populated

**If NOT:**
- Review audit script output for specific failures
- May need to fix source data

---

## Environment Variables Required

```bash
export SQLSERVER_HOST="halo-sql.database.windows.net"
export SQLSERVER_DATABASE="halo-sqldb"
export SQLSERVER_USER="azadmin"
export SQLSERVER_PASSWORD="AzureSQLWSXHjj!jks7600"

# Or use connection string
export SQLSERVER="Server=halo-sql.database.windows.net;Database=halo-sqldb;User Id=azadmin;Password=AzureSQLWSXHjj!jks7600;TrustServerCertificate=True;Encrypt=True;"
```

---

## One-Liner Commands

### Full pipeline with verification:
```bash
npx tsx scripts/run-pipeline.ts --step-by-step
```

### Re-test transforms after changes:
```bash
npx tsx scripts/run-pipeline.ts --step-by-step --transforms-only
```

### Production automated run:
```bash
npx tsx scripts/run-pipeline.ts
```

### Export staging to production (after manual testing):
```bash
npx tsx scripts/run-pipeline.ts --export-only
```
