PY?=python3
VENV=.venv
PIP=$(VENV)/bin/pip
RUN=$(VENV)/bin/python

venv:
	$(PY) -m venv $(VENV)

dev: venv
	$(PIP) install -r services/worker/requirements.txt
	$(PIP) install -r services/api/requirements.txt
	$(PIP) install -r services/dashboard/requirements.txt

etl:
	DB_PATH=data/ai_jobs.db $(RUN) -m services.worker.etl.main

api:
	DB_PATH=data/ai_jobs.db $(RUN) services/api/app.py

dashboard:
	DB_PATH=data/ai_jobs.db STREAMLIT_PORT=8501 $(RUN) -m streamlit run services/dashboard/app.py
