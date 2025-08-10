# services/worker/etl/sources/justjoin.py
import requests
from typing import List, Dict

JJIT_URL = "https://justjoin.it/api/offers"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}

def _norm_location(rec: dict) -> str:
    city = (rec.get("city") or "").strip()
    work_type = (rec.get("workplace_type") or "").strip()  # remote/hybrid/office
    if city:
        return city
    return work_type.title() if work_type else "Unspecified"

def _skills(rec: dict) -> str:
    arr = rec.get("skills") or []
    names = []
    for s in arr:
        if isinstance(s, dict) and s.get("name"):
            names.append(str(s["name"]).lower())
        elif isinstance(s, str):
            names.append(s.lower())
    # usuwamy duplikaty i sortujemy, żeby było deterministycznie
    return ",".join(sorted(set(names)))

def fetch_jobs(limit: int = 300) -> List[Dict]:
    """
    Zwraca listę słowników w naszym formacie:
      id, title, company, location, description, source
    """
    resp = requests.get(JJIT_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    out: List[Dict] = []
    for rec in data[:limit]:
        job_id = str(rec.get("id") or rec.get("slug") or rec.get("uuid") or "").strip()
        title = (rec.get("title") or rec.get("position") or "").strip()
        company = (rec.get("company_name") or rec.get("company") or "Unknown").strip()
        location = _norm_location(rec)
        desc = rec.get("body") or rec.get("description") or ""
        skills = _skills(rec)
        full_desc = f"{desc}\nSkills: {skills}" if skills else desc

        # prosta walidacja minimalna
        if not (job_id and title):
            continue

        out.append({
            "id": job_id,
            "title": title,
            "company": company,
            "location": location,
            "description": full_desc or "",
            "source": "justjoin",
        })
    return out
