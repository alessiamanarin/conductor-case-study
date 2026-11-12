PY ?= .venv/bin/python
SCENARIO ?= happy
PAYLOAD ?= samples/portal.json

.PHONY: venv dev-server worker demo ingest test test-unit test-temporal lint clean

venv:
	python3.12 -m venv .venv && .venv/bin/pip install -q temporalio pytest pytest-asyncio ruff

dev-server:
	temporal server start-dev \
	  --search-attribute AppState=Keyword --search-attribute LoanProduct=Keyword \
	  --search-attribute SlaDeadline=Datetime

worker:
	$(PY) -m loan_origination.worker

demo:
	$(PY) -m loan_origination.demo $(SCENARIO)

ingest:
	$(PY) -m loan_origination.intake $(PAYLOAD)

test:
	$(PY) -m pytest -q -rs

test-unit:
	$(PY) -m pytest -q -m "not temporal"

test-temporal:
	$(PY) -m pytest -q -rs -m temporal

lint:
	$(PY) -m ruff check .

clean:
	rm -f loan.db loan.db-wal loan.db-shm
