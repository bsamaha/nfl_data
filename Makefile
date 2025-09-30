VENV=.venv
PY=$(VENV)/bin/python
PIP=$(VENV)/bin/pip

.PHONY: app
app:
	streamlit run app/Home.py --server.port 8501 --server.headless true

.PHONY: venv install bootstrap update recache fmt

venv:
	python -m venv $(VENV)

install: venv
	$(PIP) install -r requirements.txt

bootstrap:
	$(PY) -m src.cli bootstrap --years 1999-2024

update:
	$(PY) -m src.cli update --season 2025

recache:
	$(PY) -m src.cli recache-pbp --season 2025

fmt:
	$(PY) -m black src || true

