#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "========================================"
echo "  Data Lakehouse Pipeline Tests"
echo "========================================"

# Install test dependencies if needed
if ! python -c "import pytest" 2>/dev/null; then
    echo "Installing test dependencies..."
    pip install -r "$PROJECT_DIR/tests/requirements.txt"
fi

echo ""
echo "--- Unit Tests (no Docker required) ---"
echo ""
python -m pytest "$PROJECT_DIR/tests/unit/" -v --timeout=120 -m "not e2e" "$@"

echo ""
echo "--- DAG Integrity Tests ---"
echo ""
python -m pytest "$PROJECT_DIR/tests/unit/test_dag_integrity.py" -v --timeout=30 "$@"

# Check if Docker services are running before E2E tests
if curl -sf http://localhost:8082/health > /dev/null 2>&1; then
    echo ""
    echo "--- E2E Tests (Docker services detected) ---"
    echo ""
    python -m pytest "$PROJECT_DIR/tests/e2e/" -v --timeout=600 -m e2e "$@"
else
    echo ""
    echo "--- Skipping E2E Tests (Docker services not running) ---"
    echo "  Start services with: docker compose up -d"
    echo ""
fi

echo ""
echo "========================================"
echo "  All tests completed!"
echo "========================================"
