.PHONY: clean clean-data clean-all

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