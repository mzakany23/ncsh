.PHONY: clean clean-data clean-all install test lint deploy-scraper deploy-processing scrape-month process-data venv compile-requirements run-backfill check-backfill deploy-backfill

clean-data:
	@echo "Cleaning data directories..."
	rm -rf data/html
	rm -rf data/json
	rm -f data/lookup.json
	mkdir -p data/html
	mkdir -p data/json

clean-all: clean-data
	@echo "Cleaning all generated files..."
	rm -rf data
	rm -rf __pycache__
	rm -rf *.pyc
	rm -rf .scrapy
	mkdir -p data/html
	mkdir -p data/json

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
	python -m pytest tests


lint: install
	@echo "Running linter..."
	source .venv/bin/activate && cd scraping && ruff check ncsoccer tests
	source .venv/bin/activate && cd processing && ruff check .

format: install
	@echo "Running formatter..."
	source .venv/bin/activate && cd scraping && ruff format ncsoccer tests
	source .venv/bin/activate && cd processing && ruff format .
