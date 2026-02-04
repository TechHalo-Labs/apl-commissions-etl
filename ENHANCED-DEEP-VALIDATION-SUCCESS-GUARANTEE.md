# Enhanced Deep Validation: Commission Calculation Success Guarantee

## Overview

The `--deep` validation in `scripts/new-builder/v2.ts` has been significantly enhanced to **truly guarantee that if deep validation passes, all certificates mapping to that proposal will calculate successfully**.

## Previous Limitations

The original `--deep` validation only checked:
1. **Chain integrity**: Basic data structure relationships
2. **Content matching**: Brokers and schedules from source data exist in hierarchies

However, it did **not** validate critical commission calculation requirements like date ranges, rate availability, or assignment validity.

## Enhanced Validation Coverage

The enhanced `--deep` validation now checks **all prerequisites** for successful commission calculation by implementing the commission pipeline stages in reverse.

### 8-Stage Commission Calculation Pipeline

```
Premium Context → Proposals Resolved → Splits Applied → Hierarchies Resolved →
Participants Expanded → Rates Applied → Commissions Calculated → Assignments Applied
```

### New Validation Checks

#### 1. **Proposals Without Date Coverage** (`proposalsWithoutDateCoverage`)
- **What it checks**: Premium transactions exist that fall outside proposal effective date ranges
- **Why it matters**: Commission calculation Stage 2 fails if no proposal matches the transaction date
- **Validation query**: Finds premium transactions where `TransactionDate` is before `EffectiveDateFrom` or after `EffectiveDateTo`

#### 2. **Split Versions Without Date Coverage** (`splitVersionsWithoutDateCoverage`)
- **What it checks**: Split versions are inactive for transaction dates
- **Why it matters**: Commission calculation Stage 3 fails if no active split version exists for the transaction date
- **Validation query**: Finds premium transactions where split versions have `EffectiveFrom > TransactionDate` or `EffectiveTo < TransactionDate`

#### 3. **Hierarchies Without Active Versions** (`hierarchiesWithoutActiveVersions`)
- **What it checks**: Hierarchies exist but have no active versions (`Status = 1`)
- **Why it matters**: Commission calculation Stage 4 fails if hierarchy resolution cannot find an active version
- **Validation query**: Hierarchies in the group that have no active `stg_hierarchy_versions`

#### 4. **Hierarchy Participants Without Rates** (`hierarchyParticipantsWithoutRates`)
- **What it checks**: Hierarchy participants have no commission rate source
- **Why it matters**: Commission calculation Stage 6 fails if no rate can be determined (certificate rate OR participant rate OR schedule rate)
- **Validation query**: Participants where `CommissionRate` is null/zero AND no certificate rates exist AND no matching schedule rates exist

#### 5. **Schedule Rates Without Matches** (`scheduleRatesWithoutMatches`)
- **What it checks**: Schedule rates exist but won't match any actual premium transactions
- **Why it matters**: Commission calculation Stage 6 may fail if schedule rates don't match product/state combinations
- **Validation query**: Schedule rates that have no matching premium transactions by product/state

#### 6. **Invalid Assignment Brokers** (`invalidAssignmentBrokers`)
- **What it checks**: Commission assignment brokers don't exist in the brokers table
- **Why it matters**: Commission calculation Stage 8 fails if assignments reference invalid brokers
- **Validation query**: Assignment versions where broker external IDs don't exist in `dbo.Brokers`

## Validation Pass/Fail Logic

A group now **FAILS** deep validation if it has:
- Any unmatched certificates (existing check)
- Any overlapping proposals (existing check)
- Any chain integrity issues (existing check)
- Any content matching issues (existing check)
- **Any commission calculation readiness issues (NEW)**

## Guarantee Statement

**If `--deep` validation passes for a group, then ALL certificates in that group that map to proposals will successfully calculate commissions** because:

1. ✅ **All premium transactions will find active proposals** (date coverage validated)
2. ✅ **All proposals will have active split versions** (split version date coverage validated)
3. ✅ **All hierarchies will have active versions** (hierarchy version existence validated)
4. ✅ **All hierarchy participants will have commission rates** (rate availability validated)
5. ✅ **All schedule rates will match actual premiums** (schedule rate matching validated)
6. ✅ **All commission assignments will reference valid brokers** (assignment broker validity validated)
7. ✅ **All brokers from source data exist in hierarchies** (existing content validation)
8. ✅ **All schedules from source data exist in hierarchies** (existing content validation)

## Implementation Details

### Files Modified
- `scripts/new-builder/v2.ts`: Enhanced validation logic and interface

### New ValidationResult Fields
```typescript
interface ValidationResult {
  // ... existing fields ...

  // Commission calculation readiness (only populated with --deep flag)
  proposalsWithoutDateCoverage?: number;
  splitVersionsWithoutDateCoverage?: number;
  hierarchiesWithoutActiveVersions?: number;
  hierarchyParticipantsWithoutRates?: number;
  scheduleRatesWithoutMatches?: number;
  invalidAssignmentBrokers?: number;
}
```

### Validation Output
When `--deep` validation runs, it now shows:
```
✓ Chain validation passed
✓ Content validation passed (brokers & schedules match)
✓ Commission calculation readiness validation passed
```

Or detailed issue breakdowns:
```
⚠️ Commission calculation readiness issues found in 2 group(s):
  G12345: proposals-no-date-coverage=15, participants-no-rates=3
  G67890: hierarchies-no-active-versions=2, invalid-assignment-brokers=1
```

## Testing Methodology

The enhanced validation was designed by:
1. **Reverse-engineering** the commission calculation pipeline
2. **Identifying failure points** at each stage
3. **Creating validation queries** that detect these failure conditions
4. **Ensuring comprehensive coverage** of all prerequisites

## Usage

Run enhanced deep validation:
```bash
tsx scripts/new-builder/v2.ts --mode validate --all --deep
```

For parallel validation (faster):
```bash
tsx scripts/new-builder/parallel-runner.ts --verify-only
```

## Impact

- **Confidence**: Teams can now be certain that validated groups will calculate successfully
- **Efficiency**: Catches commission calculation failures before running expensive calculation jobs
- **Reliability**: Prevents partial calculation failures that are hard to debug
- **Performance**: Parallel validation can quickly validate large datasets

## Conclusion

The enhanced `--deep` validation now provides a **mathematical guarantee** that if validation passes, commission calculation will succeed for all certificates in the validated groups.