#!/bin/bash
# Setup Trino connection in Apache Superset
# Run this after all services are up and Trino has registered tables

set -e

SUPERSET_URL="http://localhost:8088"
ADMIN_USER="admin"
ADMIN_PASS="admin"

echo "Setting up Trino connection in Superset..."

# Get access token
TOKEN=$(curl -sf -X POST "${SUPERSET_URL}/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\": \"${ADMIN_USER}\", \"password\": \"${ADMIN_PASS}\", \"provider\": \"db\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

if [ -z "$TOKEN" ]; then
  echo "ERROR: Failed to get Superset access token"
  exit 1
fi

echo "Authenticated with Superset"

# Get CSRF token
CSRF=$(curl -sf "${SUPERSET_URL}/api/v1/security/csrf_token/" \
  -H "Authorization: Bearer ${TOKEN}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['result'])")

# Create Trino database connection
echo "Creating Trino database connection..."
curl -sf -X POST "${SUPERSET_URL}/api/v1/database/" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -H "X-CSRFToken: ${CSRF}" \
  -d '{
    "database_name": "Trino Lakehouse",
    "engine": "trino",
    "sqlalchemy_uri": "trino://trino@trino:8080/delta/default",
    "expose_in_sqllab": true,
    "allow_ctas": false,
    "allow_cvas": false,
    "allow_dml": false,
    "allow_run_async": true,
    "extra": "{\"allows_virtual_table_explore\": true}"
  }' && echo " -> Created!" || echo " -> Already exists or failed"

echo ""
echo "Superset setup complete!"
echo "Access Superset at: ${SUPERSET_URL}"
echo "Login: admin / admin"
echo ""
echo "Available tables to explore in SQL Lab:"
echo "  - delta.default.dim_city"
echo "  - delta.default.dim_date"
echo "  - delta.default.fact_weather_daily_stats"
echo "  - delta.default.fact_weather_monthly_stats"
echo "  - delta.default.fact_weather_city_summary"
