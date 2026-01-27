#!/bin/bash
# Fix hierarchy creation and re-export proposals
# This script:
# 1. Runs the fixed 07-hierarchies.sql transform
# 2. Runs 08-hierarchy-splits.sql to link hierarchies
# 3. Re-exports proposals

set -e

SQL_SERVER="${SQLSERVER_HOST:-halo-sql.database.windows.net}"
SQL_DATABASE="${SQLSERVER_DATABASE:-halo-sqldb}"
SQL_USER="${SQLSERVER_USER:-azadmin}"
SQL_PASSWORD="${SQLSERVER_PASSWORD:-AzureSQLWSXHjj!jks7600}"

echo "============================================================"
echo "STEP 1: Running hierarchy transform (07-hierarchies.sql)"
echo "============================================================"
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" -C \
    -i sql/transforms/07-hierarchies.sql

echo ""
echo "============================================================"
echo "STEP 2: Running hierarchy splits (08-hierarchy-splits.sql)"
echo "============================================================"
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" -C \
    -i sql/transforms/08-hierarchy-splits.sql

echo ""
echo "============================================================"
echo "STEP 3: Re-exporting proposals (07-export-proposals.sql)"
echo "============================================================"
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" -C \
    -i sql/export/07-export-proposals.sql

echo ""
echo "============================================================"
echo "✅ COMPLETE: Hierarchy fix and proposal re-export finished"
echo "============================================================"
