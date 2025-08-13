# services/worker/etl/sources/jj_apify.py
import os, requests
from typing import List, Dict
from dotenv import load_dotenv
load_dotenv()

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
ACTOR_ID    = os.getenv("APIFY_ACTOR_ID")  # np. "piotrv1001~just-join-it-scraper"

def _norm_location(rec: dict) -> str:
    city = (rec.get("city") or "").strip()
    wt   = (rec.get("workplace_type") or "").strip()  # remote/hybrid/office
    return city or (wt.title() if wt else "Unspecified")

def _skills(rec: dict) -> str:
    names = []
    for s in rec.get("skills") or []:
        if isinstance(s, dict) and s.get("name"):
            names.append(str(s["name"]).lower())
        elif isinstance(s, str):
            names.append(s.lower())
    return ",".join(sorted(set(names)))

def _seniority(rec: dict) -> str:
    exp = (rec.get("experience") or rec.get("experience_level") or "").lower()
    if "junior" in exp: return "Junior"
    if "senior" in exp or "sr" in exp: return "Senior"
    if "mid" in exp or "regular" in exp: return "Mid"
    return "Unspecified"

def fetch_jobs(limit: int = 200, query: str = None) -> List[Dict]:
    """
    Pobiera oferty z OSTATNIEGO udanego runu aktora JJ na Apify.
    Szybkie i stabilne: nie uruchamia nowego runu.
    """
    assert APIFY_TOKEN, "Missing APIFY_TOKEN in .env"
    assert ACTOR_ID,    "Missing APIFY_ACTOR_ID in .env"

    # 1) Weź ostatni udany run, żeby zdobyć datasetId
    run_url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs/last"
    params  = {"token": APIFY_TOKEN, "status": "SUCCEEDED"}
    r = requests.get(run_url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("data") or {}
    ds_id = data.get("defaultDatasetId")
    if not ds_id:
        raise RuntimeError(f"No successful run or dataset found for actor {ACTOR_ID}")

    # 2) Pobierz elementy datasetu (z limitem)
    items_url = f"https://api.apify.com/v2/datasets/{ds_id}/items"
    params = {"token": APIFY_TOKEN, "clean": "true", "limit": str(limit)}
    r = requests.get(items_url, params=params, timeout=60)
    r.raise_for_status()
    items = r.json()

    out: List[Dict] = []
    for rec in items:
        job_id = str(rec.get("id") or rec.get("slug") or rec.get("uuid") or "").strip()
        title  = (rec.get("title") or rec.get("position") or "").strip()
        company = (rec.get("company_name") or rec.get("company") or "Unknown").strip()
        desc = rec.get("body") or rec.get("description") or ""
        skills = _skills(rec)
        if skills:
            desc = f"{desc}\nSkills: {skills}"
        if not (job_id and title):
            continue
        out.append({
            "id": job_id,
            "title": title,
            "company": company,
            "location": _norm_location(rec),
            "description": desc,
            "source": "justjoin",
            "seniority": _seniority(rec),
        })
    return out
