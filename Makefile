# ==============================================================================
# spark-project — Enterprise PySpark ETL Framework
# ==============================================================================

.PHONY: help install install-dev lint format type-check test test-unit \
        test-integration test-coverage clean build docker-build docker-up \
        docker-down spark-submit-dev spark-submit-prod

PYTHON     := python
PIP        := pip
PYTEST     := pytest
BLACK      := black
ISORT      := isort
FLAKE8     := flake8
MYPY       := mypy

SRC_DIR    := src
TEST_DIR   := tests
COV_MIN    := 80

# ── Help ──────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  spark-project — Makefile targets"
	@echo "  ──────────────────────────────────────────────────────"
	@echo "  install            Install production dependencies"
	@echo "  install-dev        Install dev + test dependencies"
	@echo "  lint               Run flake8 linter"
	@echo "  format             Format code with black + isort"
	@echo "  type-check         Run mypy type checking"
	@echo "  test               Run full test suite"
	@echo "  test-unit          Run unit tests only"
	@echo "  test-integration   Run integration tests only"
	@echo "  test-coverage      Run tests and open HTML coverage report"
	@echo "  clean              Remove build artefacts and caches"
	@echo "  docker-build       Build Docker image"
	@echo "  docker-up          Start docker-compose stack"
	@echo "  docker-down        Stop docker-compose stack"
	@echo "  spark-submit-dev   Submit ETL job to local Spark"
	@echo ""

# ── Dependencies ──────────────────────────────────────────────────────────────
install:
	$(PIP) install -r requirements.txt

install-dev:
	$(PIP) install -r requirements-dev.txt
	pre-commit install

# ── Code quality ──────────────────────────────────────────────────────────────
lint:
	$(FLAKE8) $(SRC_DIR) $(TEST_DIR) --max-line-length=100 --extend-ignore=E203,W503

format:
	$(BLACK) $(SRC_DIR) $(TEST_DIR) --line-length=100
	$(ISORT) $(SRC_DIR) $(TEST_DIR) --profile=black

type-check:
	$(MYPY) $(SRC_DIR) --ignore-missing-imports

quality: lint type-check

# ── Testing ───────────────────────────────────────────────────────────────────
test:
	$(PYTEST) $(TEST_DIR) -v

test-unit:
	$(PYTEST) $(TEST_DIR)/unit -v -m unit

test-integration:
	$(PYTEST) $(TEST_DIR)/integration -v -m integration

test-coverage:
	$(PYTEST) $(TEST_DIR) \
		--cov=$(SRC_DIR) \
		--cov-report=term-missing \
		--cov-report=html:htmlcov \
		--cov-fail-under=$(COV_MIN)
	@echo "Coverage report: htmlcov/index.html"

# ── Build ─────────────────────────────────────────────────────────────────────
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache htmlcov .coverage dist build
	rm -rf metastore_db derby.log spark-warehouse

build: clean
	$(PYTHON) -m build

# ── Docker ────────────────────────────────────────────────────────────────────
docker-build:
	docker build \
		--build-arg BUILD_DATE=$(shell date -u +%Y-%m-%dT%H:%M:%SZ) \
		--build-arg VCS_REF=$(shell git rev-parse --short HEAD 2>/dev/null || echo unknown) \
		-t spark-project:latest .

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down -v

# ── Spark submit ──────────────────────────────────────────────────────────────
spark-submit-dev:
	ENV=dev spark-submit \
		--master local[*] \
		--py-files dist/spark_project-*.whl \
		src/jobs/etl_job.py

spark-submit-prod:
	ENV=prod spark-submit \
		--master yarn \
		--deploy-mode cluster \
		--num-executors 20 \
		--executor-cores 4 \
		--executor-memory 8g \
		--driver-memory 4g \
		--py-files dist/spark_project-*.whl \
		src/jobs/etl_job.py
