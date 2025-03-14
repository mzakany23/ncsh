.PHONY: clean clean-data clean-all install test lint deploy-scraper deploy-processing scrape-month process-data venv compile-requirements run-backfill check-backfill deploy-backfill

# Clean up data directories
clean-data:
	@echo "Cleaning data directories..."
	rm -rf data/html
	rm -rf data/json
	rm -f data/lookup.json
	mkdir -p data/html
	mkdir -p data/json

# Clean up all generated files and directories
clean-all: clean-data
	@echo "Cleaning all generated files..."
	rm -rf data
	rm -rf __pycache__
	rm -rf *.pyc
	rm -rf .scrapy
	mkdir -p data/html
	mkdir -p data/json

# Default clean command
clean: clean-data

venv:
	@echo "Creating virtual environment..."
	uv venv

compile-requirements:
	@echo "Compiling requirements..."
	cd scraping && uv pip compile requirements.in -o requirements.txt
	cd processing && uv pip compile requirements.in -o requirements.txt

install: venv compile-requirements
	@echo "Installing dependencies..."
	cd scraping && uv pip install -r requirements.txt && uv pip install -e ".[dev]"
	cd processing && uv pip install -r requirements.txt && uv pip install -e ".[dev]"

test: install
	@echo "Running tests..."
	source .venv/bin/activate && cd scraping && python -m pytest tests/
	@echo "Note: processing module has no tests yet"

lint: install
	@echo "Running linter..."
	source .venv/bin/activate && cd scraping && ruff check ncsoccer tests
	source .venv/bin/activate && cd processing && ruff check .

format: install
	@echo "Running formatter..."
	source .venv/bin/activate && cd scraping && ruff format ncsoccer tests
	source .venv/bin/activate && cd processing && ruff format .

deploy-scraper: compile-requirements
	cd terraform/infrastructure && terraform apply -target=aws_lambda_function.ncsoccer_scraper

deploy-processing: compile-requirements
	cd terraform/infrastructure && terraform apply -target=aws_lambda_function.processing

deploy-backfill:
	cd terraform/infrastructure && terraform apply -target=aws_sfn_state_machine.backfill_state_machine

scrape-month:
	AWS_PROFILE=mzakany python scripts/trigger_step_function.py \
		--state-machine-arn arn:aws:states:us-east-2:$${AWS_ACCOUNT:-}:stateMachine:$${STATE_MACHINE:-ncsoccer-workflow} \
		--mode month \
		--year $${YEAR} \
		--month $${MONTH} \
		$(if $(force),--force-scrape,)

process-data:
	AWS_PROFILE=mzakany python scripts/trigger_processing.py \
		--state-machine-arn arn:aws:states:us-east-2:$${AWS_ACCOUNT:-}:stateMachine:$${STATE_MACHINE:-ncsoccer-processing}

run-backfill:
	@echo "Starting backfill job..."
	AWS_PROFILE=mzakany AWS_REGION=us-east-2 aws stepfunctions start-execution \
		--state-machine-arn arn:aws:states:us-east-2:$${AWS_ACCOUNT:-552336166511}:stateMachine:ncsoccer-backfill \
		--name "backfill-smoke-test-$$(date +%s)" \
		--input '{}' \
		--output text
		
run-local-backfill:
	@echo "Starting local backfill..."
	@if [ -z "$(start_year)" ]; then \
		echo "Error: Missing start_year. Use: make run-local-backfill start_year=2007 start_month=1 [end_year=2023 end_month=12]"; \
		exit 1; \
	fi
	@if [ -z "$(start_month)" ]; then \
		echo "Error: Missing start_month. Use: make run-local-backfill start_year=2007 start_month=1 [end_year=2023 end_month=12]"; \
		exit 1; \
	fi
	python scripts/run_backfill.py \
		--start-year=$(start_year) \
		--start-month=$(start_month) \
		$(if $(end_year),--end-year=$(end_year),) \
		$(if $(end_month),--end-month=$(end_month),) \
		$(if $(force),--force-scrape,) \
		$(if $(timeout),--timeout=$(timeout),)

check-backfill:
	@echo "Checking backfill executions..."
	AWS_PROFILE=mzakany python scripts/backfill_monitor.py --verbose

monitor-backfill:
	@echo "Monitoring backfill job..."
	AWS_PROFILE=mzakany python scripts/backfill_monitor.py --monitor --interval 30 --count 10

analyze-execution:
	@if [ -z "$(execution)" ]; then \
		echo "Error: Missing execution ARN. Use: make analyze-execution execution=ARN"; \
		exit 1; \
	fi
	AWS_PROFILE=mzakany python scripts/backfill_monitor.py --execution-arn $(execution)

# Local testing commands to avoid deployment cycles
test-lambda-image:
	@echo "Building and testing Lambda Docker image locally..."
	./scripts/test_lambda_image.sh

test-workflow:
	@echo "Running local workflow simulation..."
	@if [ -z "$(year)" ] || [ -z "$(month)" ]; then \
		echo "Error: Missing year or month. Use: make test-workflow year=2025 month=3 [day=15 mode=day|month]"; \
		exit 1; \
	fi
	python scripts/local_workflow_simulator.py \
		--year $(year) \
		--month $(month) \
		$(if $(day),--day=$(day),) \
		$(if $(mode),--mode=$(mode),--mode=day) \
		$(if $(force),--force-scrape,)

# Run the real scraper test directly (without deployment)
test-real-scraper:
	@echo "Running real scraper test..."
	python -m pytest tests/functional/test_real_scraper.py -v
