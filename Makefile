.PHONY: clean clean-data clean-all install test lint deploy-scraper deploy-processing scrape-month process-data venv compile-requirements query-llama

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

scrape-month:
	AWS_PROFILE=mzakany python scripts/trigger_step_function.py \
		--state-machine-arn arn:aws:states:us-east-2:552336166511:stateMachine:ncsh-scraper \
		--mode month \
		--year $${YEAR} \
		--month $${MONTH}

process-data:
	AWS_PROFILE=mzakany python scripts/trigger_processing.py \
		--state-machine-arn arn:aws:states:us-east-2:552336166511:stateMachine:ncsoccer-processing

query-llama:
	@echo "Running LlamaIndex query engine..."
	cd analysis && python query_engine.py "$(query)"
