PYTHON    ?= python3.13
TERRAFORM ?= terraform
VENV      := .venv
BIN       := $(VENV)/bin
TF        := $(TERRAFORM) -chdir=infra/terraform
PAYSIM    ?= data/paysim.csv
LIMIT     ?= 3000
RATE      ?= 5
KAFKA_LIMIT ?= 20000
KAFKA_RATE  ?= 500
SEED      ?= 42

export PYTHONPATH := src
export PYTHONDONTWRITEBYTECODE := 1

.PHONY: help setup test lint format demo demo-paysim tf-init plan deploy produce destroy \
	kafka-up kafka-produce kafka-detect kafka-report kafka-down clean

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-14s %s\n", $$1, $$2}'

setup: ## Create .venv and install the dev tools
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -r requirements-dev.txt

test: ## Run the unit tests
	$(BIN)/pytest

lint: ## Lint and check formatting (Python and Terraform)
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .
	$(TERRAFORM) fmt -check -recursive infra/terraform

format: ## Auto-format Python and Terraform
	$(BIN)/ruff check --fix .
	$(BIN)/ruff format .
	$(TERRAFORM) fmt -recursive infra/terraform

demo: ## Whole pipeline on your laptop (no AWS): synthetic data + detection quality report
	$(BIN)/python -m fraud_aml.producer --sink local

demo-paysim: ## Same with PaySim (download it to data/paysim.csv first)
	$(BIN)/python -m fraud_aml.producer --source paysim --paysim-path $(PAYSIM) --sink local --limit 200000 --scenario-every 2000

tf-init: ## terraform init
	$(TF) init

plan: ## Preview the AWS changes
	$(TF) plan

deploy: ## Create or update the AWS resources
	$(TF) apply

produce: ## Stream LIMIT transactions at RATE/second into the deployed pipeline
	$(BIN)/python -m fraud_aml.producer --sink dynamodb --limit $(LIMIT) --rate $(RATE) --scenario-every 100 \
	    --table "$$($(TF) output -raw transactions_table_name)" --region "$$($(TF) output -raw aws_region)"

destroy: ## Delete every AWS resource this project created
	$(TF) destroy

kafka-up: ## Start local Kafka + Kafka UI (http://localhost:8080) in Docker and create the topics
	docker compose up -d --wait
	$(BIN)/python -m fraud_aml.kafka.topics

kafka-produce: ## Stream KAFKA_LIMIT transactions into Kafka at KAFKA_RATE/second (SEED=n for new data)
	$(BIN)/python -m fraud_aml.producer --sink kafka --limit $(KAFKA_LIMIT) --rate $(KAFKA_RATE) --seed $(SEED)

kafka-detect: ## Run the detector as a Kafka consumer (run it in 2 terminals to see partitions shared)
	$(BIN)/python -m fraud_aml.kafka.detector

kafka-report: ## Score the alerts in Kafka against the ground truth
	$(BIN)/python -m fraud_aml.kafka.report

kafka-down: ## Stop Kafka and delete its data and the local detector state
	docker compose down -v
	rm -rf .local

clean: ## Remove local build artefacts
	rm -rf infra/terraform/build .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
