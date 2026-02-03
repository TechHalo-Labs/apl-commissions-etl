# Proposal Builder v2

Proposal Builder v2 is a parallel entry point that adds entropy-based routing on top of existing hard-rule rejects. It writes to the same staging tables as v1 and uses the same export pipeline.

## Purpose
- Identify high-variability groups and route them to PHA.
- Keep conformant groups in proposals, while routing small outlier clusters to PHA.
- Preserve existing staging schema and export behavior.

## Configuration
All constants are configured in `appsettings.json` under `ProposalBuilderV2`:

```json
{
  "ProposalBuilderV2": {
    "highEntropyUniqueRatio": 0.2,
    "highEntropyShannon": 5.0,
    "dominantCoverageThreshold": 0.5,
    "phaClusterSizeThreshold": 3,
    "logEntropyByGroup": false
  }
}
```

## Usage
Run v2 with the same CLI pattern as v1:

```bash
npx tsx scripts/new-builder/v2.ts --mode transform --groups G0033
```

Process all groups in batches (default batch size = 200 groups):

```bash
npx tsx scripts/new-builder/v2.ts --mode transform --batch-size 200
```

Export only (no transform):

```bash
npx tsx scripts/new-builder/v2.ts --mode export --groups G0033
```

Full transform + export:

```bash
npx tsx scripts/new-builder/v2.ts --mode full --groups G0033
```

Validate specific groups after staging:

```bash
npx tsx scripts/new-builder/v2.ts --mode transform --validate-groups G0033,G00161
```

Validate all groups processed in this run:

```bash
npx tsx scripts/new-builder/v2.ts --mode transform --full-validation
```

## Routing Logic
1. **Hard rules**: invalid group IDs and split mismatch records are routed to PHA first.
2. **Entropy routing**:
   - If group entropy is high, route entire group to PHA (`BusinessDrivenEntropy`).
   - Otherwise, keep clusters above `phaClusterSizeThreshold` as proposals; smaller clusters go to PHA (`HumanErrorOutlier`).

## Notes
- v2 uses the same staging entity generation and export scripts as v1.
- Overlapping date range fixes, schedule ID resolution, and key mapping generation are preserved.
- `--batch-size` in v2 controls how many groups are processed per batch when no `--groups` filter is provided.
