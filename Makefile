.PHONY: help fmt lint test install clean run-workers stop-workers demo verify

help:
	@echo "Available targets:"
	@echo "  install        Install package and dev dependencies"
	@echo "  fmt            Format code with black and ruff"
	@echo "  lint           Run linters (black check, ruff)"
	@echo "  test           Run pytest with coverage"
	@echo "  run-workers    Start N workers (default N=3)"
	@echo "  stop-workers   Stop all running workers"
	@echo "  demo           Run the demo script"
	@echo "  verify         Run verification script"
	@echo "  clean          Remove build artifacts and cache"

install:
	pip install -e ".[dev]"

fmt:
	black queuectl/ tests/
	ruff check --fix queuectl/ tests/

lint:
	black --check queuectl/ tests/
	ruff check queuectl/ tests/

test:
	pytest

run-workers:
	@powershell -Command "queuectl worker start --count $${env:N=3}"

stop-workers:
	@powershell -Command "queuectl worker stop"

demo:
	@bash scripts/demo.sh

verify:
	@bash scripts/verify.sh

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache/ .coverage htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
