.PHONY: help up down seed dbt-run dbt-test ge-docs precommit clean logs status

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Start all services
	docker compose up -d
	@echo "Services started. Access Airflow UI at http://localhost:8080"
	@echo "MinIO Console at http://localhost:9001 (minioadmin/minioadmin)"

down: ## Stop all services
	docker compose down

seed: ## Generate and seed demo data
	python scripts/seed_demo_data.py

dbt-run: ## Run dbt models
	cd transform/dbt && dbt deps && dbt debug && dbt run

dbt-test: ## Run dbt tests
	cd transform/dbt && dbt test

dbt-docs: ## Generate dbt documentation
	cd transform/dbt && dbt docs generate && dbt docs serve

ge-docs: ## Build Great Expectations data docs
	cd data_quality/great_expectations && great_expectations --v3-api docs build

precommit: ## Run pre-commit hooks
	pre-commit run --all-files

clean: ## Clean up containers and volumes
	docker compose down -v
	docker system prune -f

logs: ## Show logs for all services
	docker compose logs -f

status: ## Show status of all services
	docker compose ps

install-deps: ## Install Python dependencies
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

setup: install-deps ## Complete setup (install deps, start services, seed data)
	cp .env.example .env
	make up
	sleep 10
	make seed
	make dbt-run
	make ge-docs
	@echo "Setup complete! Access Airflow UI at http://localhost:8080"

test: ## Run all tests
	pytest orchestration/airflow/tests/
	pytest algae_lib/tests/

lint: ## Run linting
	flake8 algae_lib/ scripts/ orchestration/airflow/
	black --check algae_lib/ scripts/ orchestration/airflow/
	isort --check-only algae_lib/ scripts/ orchestration/airflow/

format: ## Format code
	black algae_lib/ scripts/ orchestration/airflow/
	isort algae_lib/ scripts/ orchestration/airflow/
