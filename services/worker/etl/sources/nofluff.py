# services/worker/etl/sources/nofluff.py
"""
NoFluffJobs source (portfolio-friendly).

Próbuje pobrać ogłoszenia z internetu (API/HTML) — jeśli nie wyjdzie, zwraca
zestaw DANYCH PRZYKŁADOWYCH (ok. 25), żeby pipeline (ETL→API→Dashboard)
zawsze miał co wstawić do bazy i demo nie było puste.
"""
from __future__ import annotations
import datetime as dt
from typing import List, Dict, Optional

SOURCE_NAME = "NoFluffJobs(fallback)"

_BASE = [
    ("Junior Data Engineer", "DataWorks", "Warszawa", "ETL, SQL, Python. Mile widziany Airflow i dbt.", "https://nofluffjobs.com/"),
    ("ML Engineer (NLP)", "AI Labs", "Kraków", "NLP, scikit-learn, PyTorch, MLOps (Docker).", "https://nofluffjobs.com/"),
    ("Data Scientist", "InsightX", "Gdańsk", "Modelowanie, walidacja, wizualizacje; Python, pandas, matplotlib.", "https://nofluffjobs.com/"),
    ("Senior Data Engineer", "CloudWare", "Zdalnie", "Spark, Airflow, AWS/GCP, inżynieria danych w skali.", "https://nofluffjobs.com/"),
    ("Junior AI Engineer", "VisionTech", "Wrocław", "Computer Vision, podstawy PyTorch, Docker, REST API.", "https://nofluffjobs.com/"),
]

# Dodatkowe warianty (proste mieszanie tytułów i miast)
_EXTRA_TITLES = [
    "Data Analyst", "Analytics Engineer", "BI Developer",
    "MLOps Engineer", "Machine Learning Engineer",
    "NLP Engineer", "Data Engineer", "Junior Data Analyst",
    "Research Scientist", "AI Engineer",
]
_EXTRA_CITIES = ["Warszawa", "Kraków", "Poznań", "Wrocław", "Gdańsk", "Zdalnie", "Łódź", "Katowice"]
_EXTRA_COMP = ["TechFlow", "InData", "MLWorks", "Quantica", "StreamSoft", "ModelX"]

def _samples(target: int = 25) -> List[Dict]:
    out: List[Dict] = []
    today = dt.date.today().isoformat()

    # bazowe 5
    for i, (title, company, city, desc, url) in enumerate(_BASE, start=1):
        out.append({
            "id": f"nfj-{1000+i}",
            "title": title,
            "company": company,
            "location": city,
            "desc": desc,
            "source": SOURCE_NAME,
            "posted_at": today,
            "url": url,
        })

    # generowane warianty
    idx = 1000 + len(out)
    while len(out) < target:
        t = _EXTRA_TITLES[(len(out)) % len(_EXTRA_TITLES)]
        c = _EXTRA_COMP[(len(out) * 3) % len(_EXTRA_COMP)]
        city = _EXTRA_CITIES[(len(out) * 5) % len(_EXTRA_CITIES)]
        idx += 1
        out.append({
            "id": f"nfj-{idx}",
            "title": t,
            "company": c,
            "location": city,
            "desc": f"{t} – Python, SQL, ETL/ML. Mile widziane: Airflow/dbt, Docker.",
            "source": SOURCE_NAME,
            "posted_at": today,
            "url": "https://nofluffjobs.com/",
        })
    return out

def _try_fetch_online(limit: int = 50) -> Optional[List[Dict]]:
    # Miejsce na prawdziwy fetch (na razie wyłączone, żeby demo zawsze działało)
    return None

def fetch_jobs(limit: int = 50) -> List[Dict]:
    online = _try_fetch_online(limit=limit)
    if online:
        out = []
        for r in online[:limit]:
            rr = dict(r)
            rr.setdefault("source", SOURCE_NAME)
            rr.setdefault("posted_at", dt.date.today().isoformat())
            out.append(rr)
        return out
    # Fallback – zawsze coś zwróć do demo
    return _samples(target=max(5, min(100, limit)))
