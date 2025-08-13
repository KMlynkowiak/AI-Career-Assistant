# services/worker/etl/sources/nofluff.py
import requests
from typing import List, Dict

BASE = "https://nofluffjobs.com/api/search/posting"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://nofluffjobs.com/pl",
    "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
}

def _skills(rec: dict) -> str:
    out = set()
    for s in rec.get("skills") or []:
        name = (s.get("name") or "").strip().lower()
        if name:
            out.add(name)
    return ",".join(sorted(out))

def _seniority(rec: dict) -> str:
    # NFJ zwykle ma poziomy w polu 'seniority' lub 'experience'
    # zostawiamy string, np. 'junior', 'mid', 'senior' jeśli występuje
    for k in ("seniority", "experienceLevel", "experience"):
        v = rec.get(k)
        if isinstance(v, str) and v:
            return v.capitalize()
        if isinstance(v, list) and v:
            return str(v[0]).capitalize()
    return "Unspecified"

def _location(rec: dict) -> str:
    locs = rec.get("locations") or []
    if locs:
        return (locs[0].get("city") or locs[0].get("slug") or "Unspecified").strip()
    return "Unspecified"

def _norm(rec: dict) -> Dict:
    title = (rec.get("title") or "").strip()
    company = (rec.get("company", {}) or {}).get("name") or "Unknown"
    desc = (rec.get("longText") or rec.get("text") or rec.get("essentials") or "") or ""
    skills = _skills(rec)
    if skills:
        desc = f"{desc}\nSkills: {skills}"
    return {
        "id": str(rec.get("id") or rec.get("slug") or ""),
        "title": title,
        "company": company.strip(),
        "location": _location(rec),
        "description": desc,
        "source": "nofluff",
        "seniority": _seniority(rec),
    }

def fetch_jobs(limit: int = 250, query: str = "data OR python OR sql") -> List[Dict]:
    out: List[Dict] = []
    page = 1
    while len(out) < limit and page <= 15:
        params = {
            "page": page,
            "region": "pl",
            "criteria": f"keyword={query}",
        }
        r = requests.get(BASE, params=params, headers=HEADERS, timeout=25)
        if r.status_code != 200:
            break
        data = r.json() if r.headers.get("content-type","").startswith("application/json") else {}
        results = data.get("postings") or data.get("data") or data.get("results") or []
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
