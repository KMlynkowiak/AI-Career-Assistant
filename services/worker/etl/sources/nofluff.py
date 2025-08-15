# services/worker/etl/sources/nofluff.py
from __future__ import annotations

import os
import re
import json
import time
import datetime as dt
from typing import List, Dict, Optional, Iterable, Set

import requests

SOURCE_NAME = "NoFluffJobs(HTML)"

# ===== Konfiguracja (ENV) =====
NFJ_COUNTRY     = os.getenv("NFJ_COUNTRY", "pl").strip()  # "pl" | "en"
NFJ_REMOTE      = os.getenv("NFJ_REMOTE", "1") == "1"     # dołóż /remote/...
NFJ_PAGES       = int(os.getenv("NFJ_PAGES", "12"))       # głębokość paginacji listingów
NFJ_DELAY       = float(os.getenv("NFJ_DELAY", "0.6"))    # throttle per HTTP (sek.)
NFJ_ALL_LISTINGS= os.getenv("NFJ_ALL_LISTINGS", "1") == "1"  # root /{country}?page=...
NFJ_HARD_LIMIT  = int(os.getenv("NFJ_HARD_LIMIT", "20000"))  # bezpiecznik

# fallback kategorii, gdyby auto-discovery nic nie znalazł
DEFAULT_CATEGORIES = [
    "backend","frontend","fullstack","devops","security","mobile","data","machine-learning",
    "analytics","ai","big-data","cloud","testing","qa","game","embedded","ux",
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
    t = (title or "")
    if _JUN.search(t): return "Junior"
    if _SEN.search(t): return "Senior"
    if _MID.search(t): return "Mid"
    return "Mid"

# ===== RegEx do linków i kategorii =====
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
    base = f"https://nofluffjobs.com/{country}"
    htmls = []
    for path in ("", "/remote"):
        h = _safe_get(base + path)
        if h: htmls.append(h)
        time.sleep(NFJ_DELAY)

    slugs: Set[str] = set()
    for h in htmls:
        for m in _CAT_SLUG_RE.finditer(h):
            slug = m.group(1).lower()
            if slug and slug not in {"job", "remote"}:
                slugs.add(slug)

    slugs = {s for s in slugs if len(s) >= 2 and all(ch.isalnum() or ch == "-" for ch in s)}
    return sorted(slugs) or DEFAULT_CATEGORIES

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

def iter_job_urls(limit: Optional[int] = None) -> Iterable[str]:
    """
    Generator URL-i ofert (PL + opcjonalnie EN, kategorie + root + /remote).
    Yielduje na bieżąco, żeby main mógł od razu zaczynać pobieranie i zapisy.
    """
    countries = [NFJ_COUNTRY]
    if NFJ_COUNTRY.lower() != "en":
        countries.append("en")

    seen_urls: Set[str] = set()
    yielded = 0
    lim = min(limit or NFJ_HARD_LIMIT, NFJ_HARD_LIMIT)

    for country in countries:
        base = "https://nofluffjobs.com"
        cats = _discover_categories(country)
        # kategorie
        for cat in cats:
            for p in range(1, NFJ_PAGES + 1):
                lst = f"{base}/{country}/{cat}?page={p}"
                html = _safe_get(lst); time.sleep(NFJ_DELAY)
                if not html: continue
                for u in _extract_links_from_listing(html):
                    if u not in seen_urls:
                        seen_urls.add(u); yield u; yielded += 1
                        if yielded >= lim: return
            if NFJ_REMOTE:
                for p in range(1, NFJ_PAGES + 1):
                    lst = f"{base}/{country}/remote/{cat}?page={p}"
                    html = _safe_get(lst); time.sleep(NFJ_DELAY)
                    if not html: continue
                    for u in _extract_links_from_listing(html):
                        if u not in seen_urls:
                            seen_urls.add(u); yield u; yielded += 1
                            if yielded >= lim: return
        # root listingi
        if NFJ_ALL_LISTINGS:
            for p in range(1, NFJ_PAGES + 1):
                lst = f"{base}/{country}?page={p}"
                html = _safe_get(lst); time.sleep(NFJ_DELAY)
                if not html: continue
                for u in _extract_links_from_listing(html):
                    if u not in seen_urls:
                        seen_urls.add(u); yield u; yielded += 1
                        if yielded >= lim: return
            if NFJ_REMOTE:
                for p in range(1, NFJ_PAGES + 1):
                    lst = f"{base}/{country}/remote?page={p}"
                    html = _safe_get(lst); time.sleep(NFJ_DELAY)
                    if not html: continue
                    for u in _extract_links_from_listing(html):
                        if u not in seen_urls:
                            seen_urls.add(u); yield u; yielded += 1
                            if yielded >= lim: return

def fetch_job(url: str) -> Optional[Dict]:
    """
    Pobiera i parsuje JEDNĄ ofertę -> minimalne pola.
    """
    html = _safe_get(url)
    time.sleep(NFJ_DELAY)
    if not html:
        return None

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

    # fallbacki
    if not location:
        if _REMOTE_HINT.search(html) or _REMOTE_HINT.search(title or ""):
            location = "Zdalnie"
    if not title:
        mt = re.search(r"<title>(.*?)</title>", html, re.S|re.I)
        if mt: title = mt.group(1).split("|")[0].strip()
    if not company:
        mco = re.search(r'"hiringOrganization"\s*:\s*{[^}]*"name"\s*:\s*"([^"]+)"', html)
        if mco: company = mco.group(1)
    if not location:
        mx = re.search(r'"addressLocality"\s*:\s*"([^"]+)"', html)
        if mx: location = mx.group(1)

    if not location:
        location = "Nie podano"

    return {
        "id": url,
        "title": title,
        "company": company,
        "location": location,
        "seniority": _sen_from_title(title),
        "url": url,
        "posted_at": posted or dt.date.today().isoformat(),
        "source": SOURCE_NAME,
    }
