# Makefile for op2ob project
# Executes deno run directly with optional TARGET parameter
# The 'all' target processes every benchmark listed in benchmarks.txt

# Default target - runs deno with TARGET if provided
.PHONY: extract
extract:
ifdef TARGET
	@if ! grep -q "^$(TARGET)$$" benchmarks.txt; then \
		echo "Error: TARGET '$(TARGET)' not found in benchmarks.txt"; \
		echo "Available benchmarks:"; \
		cat benchmarks.txt; \
		exit 1; \
	fi
	deno run --allow-net --allow-write extract_task_data.ts $(TARGET)
else
	@echo "Usage: make extract TARGET=<benchmark_name>"
	@echo "   or: make extract-all"
	@echo ""
	@echo "Available benchmarks:"
	@cat benchmarks.txt
endif

# Process all benchmarks listed in benchmarks.txt
.PHONY: extract-all
extract-all:
	@echo "Processing all benchmarks..."
	@while IFS= read -r benchmark; do \
		echo "Processing: $$benchmark"; \
		deno run --allow-net --allow-write extract_task_data.ts "$$benchmark" || exit 1; \
	done < benchmarks.txt
	@echo "All benchmarks processed successfully!"

# Default target when no arguments provided
.PHONY: help
help:
	@echo "op2ob Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  extract TARGET=<name>  - Extract data for a specific benchmark"
	@echo "  extract-all            - Process all benchmarks from benchmarks.txt"
	@echo "  help                   - Show this help message"
	@echo ""
	@echo "Available benchmarks:"
	@cat benchmarks.txt

# Make help the default target
.DEFAULT_GOAL := help

# Clean target (if needed for temporary files)
.PHONY: clean
clean:
	@echo "Cleaning temporary files..."
	# Add cleanup commands here if needed
