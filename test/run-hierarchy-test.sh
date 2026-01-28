#!/bin/bash
# =============================================================================
# Run Hierarchy Fix Test
# =============================================================================
# Quick test to validate the hierarchy consolidation fix
# =============================================================================

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SQL_SCRIPT="$SCRIPT_DIR/test-hierarchy-fix.sql"

# Database connection (use environment variables or defaults)
SQL_SERVER="${SQLSERVER_HOST:-halo-sql.database.windows.net}"
SQL_DATABASE="${SQLSERVER_DATABASE:-halo-sqldb}"
SQL_USERNAME="${SQLSERVER_USER:-azadmin}"
SQL_PASSWORD="${SQLSERVER_PASSWORD:-AzureSQLWSXHjj!jks7600}"

echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Hierarchy Fix Test Runner${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Database: $SQL_SERVER / $SQL_DATABASE"
echo "Test Script: $SQL_SCRIPT"
echo ""

# Check if sqlcmd is available
if ! command -v sqlcmd &> /dev/null; then
    echo -e "${RED}❌ sqlcmd not found!${NC}"
    echo "Please install sqlcmd to run this test."
    exit 1
fi

# Check if test script exists
if [ ! -f "$SQL_SCRIPT" ]; then
    echo -e "${RED}❌ Test script not found: $SQL_SCRIPT${NC}"
    exit 1
fi

echo -e "${YELLOW}⏳ Running test...${NC}"
echo ""

# Run the test
if sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -C -i "$SQL_SCRIPT"; then
    echo ""
    echo -e "${GREEN}════════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}✅ Test execution completed${NC}"
    echo -e "${GREEN}════════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "${BLUE}Next steps:${NC}"
    echo "  1. Review test results above"
    echo "  2. If PASSED: Run full ETL transform"
    echo "  3. If FAILED: Review validation errors"
    echo ""
    exit 0
else
    echo ""
    echo -e "${RED}════════════════════════════════════════════════════════════════${NC}"
    echo -e "${RED}❌ Test execution failed${NC}"
    echo -e "${RED}════════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "Check the error messages above for details."
    exit 1
fi
