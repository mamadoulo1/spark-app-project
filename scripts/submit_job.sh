#!/usr/bin/env bash
# ==============================================================================
# submit_job.sh — Spark submit wrapper for CI/CD and manual runs
#
# Usage:
#   ./scripts/submit_job.sh --env dev --job orders_etl
#   ./scripts/submit_job.sh --env prod --job orders_etl --dry-run
# ==============================================================================
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
ENV="dev"
JOB="orders_etl"
DRY_RUN=false
DEPLOY_MODE="client"
NUM_EXECUTORS=4
EXECUTOR_CORES=2
EXECUTOR_MEMORY="4g"
DRIVER_MEMORY="2g"

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --env)           ENV="$2";            shift 2 ;;
    --job)           JOB="$2";            shift 2 ;;
    --dry-run)       DRY_RUN=true;        shift   ;;
    --deploy-mode)   DEPLOY_MODE="$2";    shift 2 ;;
    --num-executors) NUM_EXECUTORS="$2";  shift 2 ;;
    --executor-mem)  EXECUTOR_MEMORY="$2"; shift 2 ;;
    --driver-mem)    DRIVER_MEMORY="$2";  shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

# ── Resolve environment-specific settings ─────────────────────────────────────
case $ENV in
  dev)
    MASTER="local[*]"
    DEPLOY_MODE="client"
    ;;
  staging|prod)
    MASTER="yarn"
    ;;
  *)
    echo "ERROR: Unknown environment '$ENV'. Use: dev, staging, prod"
    exit 1
    ;;
esac

JOB_MODULE="src/jobs/${JOB}.py"
if [[ ! -f "$JOB_MODULE" ]]; then
  echo "ERROR: Job module not found: $JOB_MODULE"
  exit 1
fi

# ── Build the command ──────────────────────────────────────────────────────────
SUBMIT_CMD=(
  spark-submit
  --master "$MASTER"
  --deploy-mode "$DEPLOY_MODE"
  --num-executors "$NUM_EXECUTORS"
  --executor-cores "$EXECUTOR_CORES"
  --executor-memory "$EXECUTOR_MEMORY"
  --driver-memory "$DRIVER_MEMORY"
  --conf "spark.sql.shuffle.partitions=200"
  --conf "spark.sql.adaptive.enabled=true"
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  --py-files "dist/spark_project-1.0.0-py3-none-any.whl"
  "$JOB_MODULE"
)

# ── Execute ───────────────────────────────────────────────────────────────────
echo "============================================================"
echo "  Environment : $ENV"
echo "  Job         : $JOB"
echo "  Master      : $MASTER"
echo "  Deploy mode : $DEPLOY_MODE"
echo "  Command     : ${SUBMIT_CMD[*]}"
echo "============================================================"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "[DRY RUN] Skipping execution."
  exit 0
fi

export ENV="$ENV"
"${SUBMIT_CMD[@]}"
