SHELL := /bin/bash
VENV := .venv

# detect platform (Git Bash shows MINGW/MSYS)
UNAME_S := $(shell uname -s)
ifneq (,$(findstring MINGW,$(UNAME_S)))
  BIN := Scripts
  PY  := py -3
else ifneq (,$(findstring MSYS,$(UNAME_S)))
  BIN := Scripts
  PY  := py -3
else
  BIN := bin
  PY  := python3
endif

PIP     := $(VENV)/$(BIN)/pip
PYTHON  := $(VENV)/$(BIN)/python
PYTEST  := $(VENV)/$(BIN)/pytest
RUFF    := $(VENV)/$(BIN)/ruff
BLACK   := $(VENV)/$(BIN)/black
MYPY    := $(VENV)/$(BIN)/mypy

.PHONY: help
help:
	@echo "Targets:"
	@echo "  make bootstrap     - Create venv and install dev deps"
	@echo "  make install-all   - Install dev + service deps into venv"
	@echo "  make lint          - Ruff + Black check"
	@echo "  make format        - Black format"
	@echo "  make typecheck     - Run mypy (lenient)"
	@echo "  make test          - Run unit tests"
	@echo "  make clean         - Remove venv and caches"
	@echo "  make e2e           - (Placeholder) End-to-end test via Docker Compose"

$(VENV)/$(BIN)/activate: requirements-dev.txt
	$(PY) -m venv $(VENV)
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements-dev.txt

bootstrap: $(VENV)/$(BIN)/activate

install-all: bootstrap
	$(PYTHON) -m pip install -r services/producer/requirements.txt
	$(PYTHON) -m pip install -r services/spark/requirements.txt
	$(PYTHON) -m pip install -r services/rest/requirements.txt

# Lint only project code; configs still exclude .venv as well.
lint: bootstrap
	$(RUFF) check services tests
	$(BLACK) --check services tests

format: bootstrap
	$(BLACK) services tests

typecheck: bootstrap
	$(MYPY) -p services --install-types --non-interactive || true
	$(MYPY) -p services || true

test: install-all
	$(PYTEST) -q

clean:
	rm -rf $(VENV) .pytest_cache .mypy_cache .ruff_cache

e2e:
	@echo "E2E will be implemented in later steps (Docker Compose)."
