# services/worker/etl/sources/adzuna.py
import os
import requests
from typing import List, Dict
from dotenv import load_dotenv

# wczytaj .env z katalogu projektu
load_dotenv()

APP_ID  = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
BASE = "https://api.adzuna.com/v1/api/jobs/pl/search/{page}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}

def _norm(rec: dict) -> Dict:
    loc = ""
    try:
        locs = rec.get("location", {}).get("area") or []
        if locs:
            loc = locs[-1]
    except Exception:
        pass
    desc = rec.get("description") or ""
    return {
        "id": str(rec.get("id") or rec.get("redirect_url") or ""),
        "title": (rec.get("title") or "").strip(),
        "company": (rec.get("company", {}).get("display_name") or "Unknown").strip(),
        "location": loc or "Unspecified",
        "description": desc,
        "source": "adzuna",
    }

def _check_env():
    missing = []
    if not APP_ID:
        missing.append("ADZUNA_APP_ID")
    if not APP_KEY:
        missing.append("ADZUNA_APP_KEY")
    if missing:
        raise RuntimeError(
            "Missing env vars: " + ", ".join(missing) +
            ". Create a .env in the project root, e.g.\n"
            "ADZUNA_APP_ID=...\nADZUNA_APP_KEY=...\n"
        )

def fetch_jobs(limit: int = 200, query: str = "data OR python OR sql") -> List[Dict]:
    _check_env()
    out: List[Dict] = []
    page = 1
    while len(out) < limit and page <= 5:
        params = {
            "app_id": APP_ID,
            "app_key": APP_KEY,
            "results_per_page": 50,
            "what": query,
            "where": "poland",
            "max_days_old": 30,
            "content-type": "application/json",
        }
        r = requests.get(BASE.format(page=page), params=params, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            # pokaż pierwsze 300 znaków odpowiedzi dla diagnozy
            raise RuntimeError(f"Adzuna API error {r.status_code}: {r.text[:300]}")
        data = r.json()
        results = data.get("results", [])
        for rec in results:
            o = _norm(rec)
            if o["id"] and o["title"]:
                out.append(o)
                if len(out) >= limit:
                    break
        if not results:
            break
        page += 1
    return out
