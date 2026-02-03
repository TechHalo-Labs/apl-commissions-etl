#!/usr/bin/env bash
set -euo pipefail

out="$HOME/Downloads/newversion.txt"

{
  echo "===== /Users/kennpalm/Downloads/source/APL/apl-commissions-etl/scripts/new-builder/v2.ts ====="
  cat "/Users/kennpalm/Downloads/source/APL/apl-commissions-etl/scripts/new-builder/v2.ts"
  echo ""
  echo "===== /Users/kennpalm/Downloads/source/APL/apl-commissions-etl/scripts/proposal-builder.ts ====="
  cat "/Users/kennpalm/Downloads/source/APL/apl-commissions-etl/scripts/proposal-builder.ts"
  echo ""
  echo "===== /Users/kennpalm/Downloads/source/APL/apl-commissions-etl/appsettings.json ====="
  cat "/Users/kennpalm/Downloads/source/APL/apl-commissions-etl/appsettings.json"
  echo ""
  echo "===== /Users/kennpalm/Downloads/source/APL/apl-commissions-etl/docs/features/proposal-builder-v2.md ====="
  cat "/Users/kennpalm/Downloads/source/APL/apl-commissions-etl/docs/features/proposal-builder-v2.md"
} > "$out"

echo "Wrote $out"
