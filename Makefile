.PHONY: install test lint descriptor

install:
	uv sync --all-extras

test:
	uv run pytest -v

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

descriptor:
	@mkdir -p gen/descriptors
	buf build -o gen/descriptors/descriptor.bin
