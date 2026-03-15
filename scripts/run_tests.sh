#!/usr/bin/env bash
# ==============================================================================
# run_tests.sh — CI test runner with coverage enforcement
# ==============================================================================
set -euo pipefail

SUITE="${1:-all}"
COV_MIN="${COV_MIN:-80}"

echo "Running test suite: $SUITE"

case $SUITE in
  unit)
    pytest tests/unit -v -m unit \
      --cov=src --cov-report=term-missing --cov-fail-under="$COV_MIN"
    ;;
  integration)
    pytest tests/integration -v -m integration \
      --cov=src --cov-report=term-missing
    ;;
  all)
    pytest tests/ -v \
      --cov=src \
      --cov-report=term-missing \
      --cov-report=xml:coverage.xml \
      --cov-fail-under="$COV_MIN"
    ;;
  *)
    echo "Unknown suite: $SUITE. Use: unit | integration | all"
    exit 1
    ;;
esac

echo "Tests passed."
