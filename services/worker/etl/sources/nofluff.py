# services/worker/etl/sources/nofluff.py
"""
NoFluffJobs source (portfolio-friendly).

Próbuje pobrać ogłoszenia z internetu (API/HTML) — jeśli nie wyjdzie, zwraca
zestaw DANYCH PRZYKŁADOWYCH (skalowalny), żeby pipeline (ETL→API→Dashboard)
zawsze miał co wstawić do bazy i demo nie było puste.

Zmiany:
- DUŻO większa różnorodność firm (generator nazw zamiast krótkiej listy).
- seniority dostępne już w RAW (zgadywane z tytułu).
"""
from __future__ import annotations
import datetime as dt
import re
from typing import List, Dict, Optional

SOURCE_NAME = "NoFluffJobs(fallback)"

# kilka bazowych wpisów (żeby mieć „realnie” wyglądające przykłady)
_BASE = [
    ("Junior Data Engineer", "DataWorks", "Warszawa", "ETL, SQL, Python. Mile widziany Airflow i dbt.", "https://nofluffjobs.com/"),
    ("ML Engineer (NLP)", "AI Labs", "Kraków", "NLP, scikit-learn, PyTorch, MLOps (Docker).", "https://nofluffjobs.com/"),
    ("Data Scientist", "InsightX", "Gdańsk", "Modelowanie, walidacja, wizualizacje; Python, pandas, matplotlib.", "https://nofluffjobs.com/"),
    ("Senior Data Engineer", "CloudWare", "Zdalnie", "Spark, Airflow, AWS/GCP, inżynieria danych w skali.", "https://nofluffjobs.com/"),
    ("Junior AI Engineer", "VisionTech", "Wrocław", "Computer Vision, podstawy PyTorch, Docker, REST API.", "https://nofluffjobs.com/"),
]

# stanowiska/tytuły i miasta – to może zostać listą (i tak łączymy je dowolnie)
_EXTRA_TITLES = [
    "Data Analyst", "Analytics Engineer", "BI Developer",
    "MLOps Engineer", "Machine Learning Engineer",
    "NLP Engineer", "Data Engineer", "Junior Data Analyst",
    "Research Scientist", "AI Engineer", "Senior Data Scientist",
    "Senior Machine Learning Engineer", "Junior BI Developer",
    "Lead Data Engineer", "Principal ML Engineer",
]
_EXTRA_CITIES = [
    "Warszawa", "Kraków", "Poznań", "Wrocław", "Gdańsk",
    "Zdalnie", "Łódź", "Katowice", "Szczecin", "Rzeszów",
]

# — generator nazw firm (pula kombinacji >> liczba ogłoszeń) —
_NAME_PRE = [
    "Data","Cloud","Quantum","Vector","Nova","Blue","Deep","Bright","Core","Peak","Apex",
    "Hyper","Neo","Alpha","Omega","Green","Silver","Golden","Urban","Prime","Next","Future",
    "Proto","Pixel","Spark","Stream","Model","Graph","Tensor","Matrix","Signal","Insight",
    "Vision","Logic","Numeric","Stat","Analytic","Bayes","Gradient","Kernel","Pattern",
    "Crystal","Nimbus","Orbit","Turbo","Solid","Clear","Rapid","Bold","True","Meta",
]
_NAME_CORE = [
    "Forge","Works","Labs","Flow","Metric","Nest","Mind","Smith","Nexus","Haven",
    "Pulse","Shift","Loop","Bridge","Scope","Scale","Stack","Craft","Hub","Studio",
    "Ops","Verse","Ray","Grid","Field","Point","Path","Beam","Core","Layer","Wave",
    "Drift","Engine","Peak","Stack","Engine","Dynami","Synth","Quanta","Orbit","Factor",
]
_NAME_SUF = [
    "AI","Analytics","Data","Systems","Solutions","Tech","Digital","Software",
    "Intelligence","Networks","Group","Partners","Studio","Research","Platforms",
]

def _company_name(i: int) -> str:
    """
    Deterministyczny generator nazw firm. Kombinacje ~ len(PRE)*len(CORE)*len(SUF)
    → dziesiątki tysięcy unikatów. Po wyczerpaniu puli dodajemy sufiks liczbowy.
    """
    a = _NAME_PRE[i % len(_NAME_PRE)]
    b = _NAME_CORE[(i * 7) % len(_NAME_CORE)]
    c = _NAME_SUF[(i * 13) % len(_NAME_SUF)]
    name = f"{a}{b} {c}"
    combos = len(_NAME_PRE) * len(_NAME_CORE) * len(_NAME_SUF)
    if i >= combos:
        name += f" {i // combos + 2}"
    return name

_JUN = re.compile(r"\b(junior|intern|trainee|student|jr)\b", re.I)
_SEN = re.compile(r"\b(senior|lead|principal|staff|expert|architect|sr)\b", re.I)
_MID = re.compile(r"\b(mid|middle|regular)\b", re.I)

def _guess_seniority_from_title(title: str) -> str:
    t = title or ""
    if _JUN.search(t): return "Junior"
    if _SEN.search(t): return "Senior"
    if _MID.search(t): return "Mid"
    return "Mid"  # brak jawnych sygnałów → Mid

def _samples(target: int) -> List[Dict]:
    out: List[Dict] = []
    today = dt.date.today().isoformat()

    # bazowe 5 (z realnymi nazwami)
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
            "seniority": _guess_seniority_from_title(title),
        })

    # generowane rekordy: TYLKO tytuł/city z list, firma z generatora (bez limitu)
    idx = 1000 + len(out)
    i = 0
    while len(out) < target:
        t = _EXTRA_TITLES[i % len(_EXTRA_TITLES)]
        city = _EXTRA_CITIES[(i * 7) % len(_EXTRA_CITIES)]
        company = _company_name(i)  # <— nieograniczona liczba firm
        idx += 1
        i += 1
        out.append({
            "id": f"nfj-{idx}",
            "title": t,
            "company": company,
            "location": city,
            "desc": f"{t} – Python, SQL, ETL/ML. Mile widziane: Airflow/dbt, Docker.",
            "source": SOURCE_NAME,
            "posted_at": today,
            "url": "https://nofluffjobs.com/",
            "seniority": _guess_seniority_from_title(t),
        })
    return out

def _try_fetch_online(limit: int = 50) -> Optional[List[Dict]]:
    # Miejsce na prawdziwy fetch (na razie wyłączone, żeby demo zawsze działało)
    return None

def fetch_jobs(limit: int = 50) -> List[Dict]:
    """
    Zwraca do 'limit' rekordów (bez sztucznego limitu 100).
    Dla bezpieczeństwa ograniczamy do max 5000, żeby nie wyprodukować zbyt wielkiej bazy.
    """
    limit = max(5, min(5000, int(limit)))
    online = _try_fetch_online(limit=limit)
    if online:
        out = []
        for r in online[:limit]:
            rr = dict(r)
            rr.setdefault("source", SOURCE_NAME)
            rr.setdefault("posted_at", dt.date.today().isoformat())
            rr.setdefault("seniority", _guess_seniority_from_title(rr.get("title","")))
            # jeśli online nie podało company — wygeneruj
            rr.setdefault("company", _company_name(len(out)))
            out.append(rr)
        return out
    return _samples(target=limit)
