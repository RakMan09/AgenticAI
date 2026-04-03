PYTHON ?= python3

.PHONY: fetch ingest api web

fetch:
	$(PYTHON) scripts/fetch_assetopsbench_subset.py

ingest:
	$(PYTHON) scripts/ingest_assetops.py

api:
	cd apps/api && uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

web:
	cd apps/web && npm run dev
