# services/worker/etl/sources/nofluff.py
import requests
from typing import List, Dict

# Uwaga: to jest nieoficjalny endpoint z frontendu NFJ – bywa, że zmienią schemat
BASE = "https://nofluffjobs.com/api/search/posting"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://nofluffjobs.com/pl",
}

def _skills(rec: dict) -> str:
    # w NFJ skille siedzą w polu 'skills' / 'requirements'
    out = set()
    for s in (rec.get("skills") or []):
        name = (s.get("name") or "").strip().lower()
        if name:
            out.add(name)
    return ",".join(sorted(out))

def _norm(rec: dict) -> Dict:
    # dopasuj do naszego formatu
    title = (rec.get("title") or "").strip()
    company = (rec.get("company", {}).get("name") or "Unknown").strip()
    location = "Unspecified"
    try:
        locs = rec.get("locations") or []
        if locs:
            location = (locs[0].get("city") or locs[0].get("slug") or "Unspecified").strip()
    except Exception:
        pass
    desc = (rec.get("longText") or rec.get("text") or rec.get("essentials") or "") or ""
    skills = _skills(rec)
    if skills:
        desc = f"{desc}\nSkills: {skills}"

    return {
        "id": str(rec.get("id") or rec.get("slug") or ""),
        "title": title,
        "company": company,
        "location": location,
        "description": desc,
        "source": "nofluff",
    }

def fetch_jobs(limit: int = 200, query: str = "data OR python OR sql") -> List[Dict]:
    """
    Pobiera oferty z NFJ. Paginuje po 20 wyników na stronę aż do 'limit'.
    query – proste słowa kluczowe do wyszukiwania.
    """
    out: List[Dict] = []
    page = 1
    while len(out) < limit and page <= 10:
        params = {
            "page": page,
            "salaryCurrency": "pln",
            "region": "pl",          # Polska
            "criteria": f"keyword={query}",
        }
        r = requests.get(BASE, params=params, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            # przy 403/429 spróbuj jeszcze raz lub przerwij grzecznie
            break
        data = r.json() if r.headers.get("content-type","").startswith("application/json") else {}
        results = (data.get("postings") or
                   data.get("data") or
                   data.get("results") or [])
        if not results:
            break
        for rec in results:
            o = _norm(rec)
            if o["id"] and o["title"]:
                out.append(o)
                if len(out) >= limit:
                    break
        page += 1
    return out
