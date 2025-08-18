VENV=.venv
PY=$(VENV)/bin/python
PIP=$(VENV)/bin/pip

.PHONY: venv install bootstrap update recache fmt app

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

app: install
	$(VENV)/bin/streamlit run src/app_streamlit.py

