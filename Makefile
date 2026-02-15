SHELL := /bin/bash

PYTHON := .venv-management/bin/python
RUFF := .venv-management/bin/ruff
PYTEST := .venv-management/bin/pytest
UVICORN := .venv-management/bin/uvicorn
NPM := npm

API_DIR := apps/management_api
WEB_DIR := apps/web

.DEFAULT_GOAL := help

.PHONY: help fmt lint test build run docker-build docker-run clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "%-15s %s\n", $$1, $$2}'

fmt: ## Format Python sources
	$(RUFF) format $(API_DIR)

lint: ## Run static checks for API and web
	PYTHONPATH=$(API_DIR) $(RUFF) check $(API_DIR)
	cd $(WEB_DIR) && $(NPM) run lint

test: ## Run API and web tests
	PYTHONPATH=$(API_DIR) $(PYTEST) -q $(API_DIR)/tests
	cd $(WEB_DIR) && $(NPM) run test

build: ## Build web production assets
	cd $(WEB_DIR) && $(NPM) run build

run: ## Run API locally
	PYTHONPATH=$(API_DIR) $(UVICORN) app.main:app --reload --host 0.0.0.0 --port 8000

docker-build: ## Build Docker images for local stack
	docker compose build

docker-run: ## Run Docker Compose stack
	docker compose up --build

clean: ## Remove generated caches and local test db
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
	find . -name '.pytest_cache' -type d -prune -exec rm -rf {} +
	find . -name '.ruff_cache' -type d -prune -exec rm -rf {} +
	rm -f management_api_test.db
