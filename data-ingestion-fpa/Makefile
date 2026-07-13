.PHONY: install test lint fmt

install:
	pip install -e ".[dev]"

test:
	pytest pipelines/finance_data_pipelines/tests/unit/ -v

lint:
	ruff check .

fmt:
	ruff format .
