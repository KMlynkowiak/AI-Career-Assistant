# services/worker/etl/sources/nofluff.py
from __future__ import annotations

import os
import re
import json
import time
import datetime as dt
from typing import List, Dict, Optional, Iterable, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

SOURCE_NAME = "NoFluffJobs(HTML)"

# ===== Konfiguracja (ENV) =====
NFJ_COUNTRY     = os.getenv("NFJ_COUNTRY", "pl").strip()  # "pl" | "en"
NFJ_REMOTE      = os.getenv("NFJ_REMOTE", "1") == "1"     # dołóż /remote/...
NFJ_PAGES       = int(os.getenv("NFJ_PAGES", "15"))       # głębokość paginacji na listingach
NFJ_DELAY       = float(os.getenv("NFJ_DELAY", "0.6"))    # throttle per request (sek.)
NFJ_WORKERS     = int(os.getenv("NFJ_WORKERS", "10"))     # równoległe pobieranie podstron ofert
NFJ_HARD_LIMIT  = int(os.getenv("NFJ_HARD_LIMIT", "10000")) # twardy bezpiecznik
# Pełne „wszystko”: dodaj listingi root /{country}?page=...
NFJ_ALL_LISTINGS= os.getenv("NFJ_ALL_LISTINGS", "1") == "1"

# Domyślny koszyk kategorii (gdy auto-discovery nie zadziała)
DEFAULT_CATEGORIES = [
    "backend","frontend","fullstack","devops","security","mobile","data","machine-learning",
    "analytics","ai","big-data","cloud","testing","qa","game","embedded","ux","erp",
    "pm","product","support","sales","marketing","bi","etl","java","python","dotnet","php",
]

USER_AGENT = os.getenv(
    "NFJ_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
)

# ===== Heurystyki seniority =====
_JUN = re.compile(r"\b(junior|intern|trainee|student|jr)\b", re.I)
_SEN = re.compile(r"\b(senior|lead|principal|staff|expert|architect|sr)\b", re.I)
_MID = re.compile(r"\b(mid|middle|regular)\b", re.I)
_REMOTE_HINT = re.compile(r"\b(remote|zdaln\w*|home\s*office|hybryd\w*)\b", re.I)

def _sen_from_title(title: str) -> str:
    t = title or ""
    if _JUN.search(t): return "Junior"
    if _SEN.search(t): return "Senior"
    if _MID.search(t): return "Mid"
    return "Mid"

# ===== Link finders =====
_LIST_JOB_LINK_RE        = re.compile(r'href="(/(?:en|pl)/job/[^"]+)"', re.I)
_LIST_JOB_LINK_DATAHREF  = re.compile(r'data-href="(/(?:en|pl)/job/[^"]+)"', re.I)
_LIST_JOB_LINK_ABS_RE    = re.compile(r'href="(https://nofluffjobs\.com/(?:en|pl)/job/[^"]+)"', re.I)
_ANY_JOB_URL_RE          = re.compile(r'https://nofluffjobs\.com/(?:en|pl)/job/[^"\'<>\s]+', re.I)
_CAT_SLUG_RE             = re.compile(r'href="/(?:en|pl)/([a-z0-9\-]+)"', re.I)

def _normalize_job_url(u: str) -> str:
    return u if u.startswith("http") else f"https://nofluffjobs.com{u}"

def _safe_get(url: str, timeout: int = 30) -> Optional[str]:
    try:
        r = requests.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Language": "pl,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Cache-Control": "no-cache",
            },
        )
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None

def _discover_categories(country: str) -> List[str]:
    """
    Próbujemy odczytać slugi kategorii z /{country} i /{country}/remote.
    Jeśli nie wyjdzie – wracamy do DEFAULT_CATEGORIES.
    """
    base = f"https://nofluffjobs.com/{country}"
    htmls = []
    for path in ("", "/remote"):
        h = _safe_get(base + path)
        if h:
            htmls.append(h)
        time.sleep(NFJ_DELAY)

    slugs: Set[str] = set()
    for h in htmls:
        for m in _CAT_SLUG_RE.finditer(h):
            slug = m.group(1).lower()
            if slug and slug not in {"job", "remote"}:
                slugs.add(slug)

    # sanity – usuwamy oczywiste „inne” linki
    slugs = {s for s in slugs if len(s) >= 2 and all(ch.isalnum() or ch == "-" for ch in s)}
    if not slugs:
        return DEFAULT_CATEGORIES
    return sorted(slugs)

def _listing_urls(country: str) -> Iterable[str]:
    base = "https://nofluffjobs.com"
    cats = _discover_categories(country)
    # 1) listingi po kategoriach
    for cat in cats:
        for p in range(1, NFJ_PAGES + 1):
            yield f"{base}/{country}/{cat}?page={p}"
        if NFJ_REMOTE:
            for p in range(1, NFJ_PAGES + 1):
                yield f"{base}/{country}/remote/{cat}?page={p}"
    # 2) globalne listingi root (łapią „wszystko” niezależnie od kategorii)
    if NFJ_ALL_LISTINGS:
        for p in range(1, NFJ_PAGES + 1):
            yield f"{base}/{country}?page={p}"
        if NFJ_REMOTE:
            for p in range(1, NFJ_PAGES + 1):
                yield f"{base}/{country}/remote?page={p}"

def _extract_links_from_listing(html: str) -> List[str]:
    links = []
    for m in _LIST_JOB_LINK_RE.finditer(html):
        links.append(_normalize_job_url(m.group(1)))
    for m in _LIST_JOB_LINK_DATAHREF.finditer(html):
        links.append(_normalize_job_url(m.group(1)))
    for m in _LIST_JOB_LINK_ABS_RE.finditer(html):
        links.append(_normalize_job_url(m.group(1)))
    for m in _ANY_JOB_URL_RE.finditer(html):
        links.append(_normalize_job_url(m.group(0)))

    seen: Set[str] = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def _extract_job_from_html(html: str, url: str) -> Dict:
    # JSON-LD JobPosting
    job = None
    for m in re.finditer(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, flags=re.S|re.I):
        raw = m.group(1).strip()
        try:
            data = json.loads(raw)
        except Exception:
            continue

        def pick(d):
            nonlocal job
            if isinstance(d, dict) and d.get("@type") == "JobPosting":
                job = d

        if isinstance(data, dict):
            if data.get("@type") == "JobPosting":
                job = data
            elif "@graph" in data and isinstance(data["@graph"], list):
                for d in data["@graph"]:
                    pick(d)
        elif isinstance(data, list):
            for d in data:
                pick(d)
        if job:
            break

    title = ""
    company = ""
    location = ""
    posted = None

    if job:
        title = job.get("title") or ""
        hiring = job.get("hiringOrganization") or {}
        if isinstance(hiring, dict):
            company = hiring.get("name") or ""
        locobj = job.get("jobLocation")
        addr = {}
        if isinstance(locobj, list) and locobj:
            addr = locobj[0].get("address", {}) or {}
        elif isinstance(locobj, dict):
            addr = (locobj.get("address", {}) or {})
        location = addr.get("addressLocality") or addr.get("addressRegion") or addr.get("addressCountry") or ""
        posted = job.get("datePosted") or job.get("validFrom")

    if not location:
        # Remote/hybrid
        if re.search(_REMOTE_HINT, html) or re.search(_REMOTE_HINT, title or ""):
            location = "Zdalnie"
        else:
            mx = re.search(r'"addressLocality"\s*:\s*"([^"]+)"', html)
            if mx:
                location = mx.group(1)

    if not title:
        mt = re.search(r"<title>(.*?)</title>", html, re.S|re.I)
        if mt:
            title = mt.group(1).split("|")[0].strip()

    if not company:
        mco = re.search(r'"hiringOrganization"\s*:\s*{[^}]*"name"\s*:\s*"([^"]+)"', html)
        if mco:
            company = mco.group(1)

    if not location:
        location = "Nie podano"

    return {
        "id": url,
        "title": title,
        "company": company,
        "location": location,
        "seniority": _sen_from_title(title),
        "url": url,
        "posted_at": posted,
        "source": SOURCE_NAME,
    }

def fetch_jobs(limit: int = 1000000) -> List[Dict]:
    """
    Ściąga WSZYSTKO (w granicach NFJ_PAGES i NFJ_ALL_LISTINGS + kategorii), bez filtrowania po branży.
    Minimalny zestaw pól: title, company, location, seniority, url (+ posted_at).
    """
    limit = max(1, min(int(limit), NFJ_HARD_LIMIT))

    # 1) Zbierz linki z WSZYSTKICH listingów (PL + opcjonalnie EN)
    countries = [NFJ_COUNTRY]
    if NFJ_COUNTRY.lower() != "en":
        countries.append("en")  # dorzuć EN – często więcej ofert

    all_job_urls: List[str] = []
    for country in countries:
        for lst in _listing_urls(country):
            html = _safe_get(lst)
            time.sleep(NFJ_DELAY)
            if not html:
                continue
            links = _extract_links_from_listing(html)
            all_job_urls.extend(links)

    # unikalne URL-e
    seen: Set[str] = set()
    uniq_urls: List[str] = []
    for u in all_job_urls:
        if u not in seen:
            seen.add(u)
            uniq_urls.append(u)

    if not uniq_urls:
        return []

    # 2) Równoległe pobieranie i parsowanie ofert
    def fetch_one(u: str) -> Optional[Dict]:
        h = _safe_get(u)
        time.sleep(NFJ_DELAY)
        if not h:
            return None
        return _extract_job_from_html(h, u)

    out: List[Dict] = []
    with ThreadPoolExecutor(max_workers=max(1, NFJ_WORKERS)) as ex:
        futures = (ex.submit(fetch_one, u) for u in uniq_urls)
        for fut in as_completed(futures):
            try:
                rec = fut.result()
            except Exception:
                rec = None
            if rec and rec.get("title"):
                out.append(rec)
                if len(out) >= limit:
                    break

    today = dt.date.today().isoformat()
    for r in out:
        r.setdefault("posted_at", today)
        r.setdefault("source", SOURCE_NAME)

    return out[:limit]
