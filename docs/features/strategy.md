# Commission Processing Strategy (ETL Seeding)

## Purpose

This document explains how the ETL should set `CommissionProcessingStrategy`
when seeding EmployerGroups. The Bootstrap should default new groups to
`Disabled`, and this default must be configurable via appsettings. It also
captures the EmployerGroups export expectations that proposal-building relies on.

## Default Strategy

- **Default value:** `Disabled`
- **Reason:** Safe default that prevents accidental commission runs for newly
  seeded groups until they are reviewed.

## Enum Mapping

Backend enum values:

- `Standard` = `0`
- `Bypass` = `1`
- `Disabled` = `2`

When writing to the database, use the numeric value unless the ETL layer
explicitly stores string enums (use `2` for `Disabled`).

## AppSettings Configuration

Add a default strategy setting in the ETL appsettings and read it during
Bootstrap seeding:

```json
{
  "CommissionProcessingStrategyDefaults": {
    "EmployerGroups": "Disabled"
  }
}
```

## Seeding Rules

1. **Bootstrap import**: If no strategy is provided in the source data,
   set `CommissionProcessingStrategy` to the appsettings default.
2. **Explicit values**: If a source row provides a strategy, normalize it
   to one of `Standard`, `Bypass`, `Disabled` (or their numeric equivalents).
3. **Validation**: Reject or log any strategy value that is not one of the
   supported enum values.

## EmployerGroups Export Expectations

- **GroupNumber alignment**: The proposal builder loads certificates and
  resolves group names by joining `dbo.EmployerGroups.GroupNumber` to
  `input_certificate_info.GroupId` after trimming whitespace and stripping
  leading letters. The export should continue to populate `GroupNumber` with
  the numeric group code (no `G` prefix) so this join remains stable.
- **GroupName usage**: Proposal-building falls back to `GroupName` when available;
  ensure the export keeps `GroupName` populated from `stg_groups.Name`.

## Example (Pseudo-Logic)

```typescript
const defaultStrategy = config.CommissionProcessingStrategyDefaults.EmployerGroups;

const normalizedStrategy = normalizeStrategy(
  sourceRow.commissionProcessingStrategy ?? defaultStrategy
);

groupRecord.commissionProcessingStrategy = normalizedStrategy;
```

## Notes

- The API returns numeric values in some environments (e.g., `2` for `Disabled`).
- Keep the default centralized in appsettings to make future changes easy.

