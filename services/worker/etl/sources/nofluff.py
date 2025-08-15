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

# ===== Konfiguracja (ENV; dobrane pod ≥500) =====
NFJ_COUNTRY = os.getenv("NFJ_COUNTRY", "pl").strip()  # "pl" | "en"
NFJ_CATEGORIES = os.getenv(
    "NFJ_CATEGORIES",
    # szeroki wachlarz kategorii związanych z danymi / backendem / ML
    "data,backend,devops,machine-learning,analytics,ai,big-data,cloud,security,fullstack,testing"
).split(",")
NFJ_REMOTE = os.getenv("NFJ_REMOTE", "1") == "1"      # 1 => dodaj /remote/<cat>
NFJ_PAGES = int(os.getenv("NFJ_PAGES", "10"))         # strony listingu na kategorię
NFJ_DELAY = float(os.getenv("NFJ_DELAY", "0.6"))      # przerwa między żądaniami (sek.)
NFJ_WORKERS = int(os.getenv("NFJ_WORKERS", "8"))      # wątki do pobierania stron ogłoszeń
USER_AGENT = os.getenv(
    "NFJ_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome Safari"
)

# ===== Heurystyki =====
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

# ===== HTML helpers =====
_LIST_JOB_LINK_RE = re.compile(r'href="(/(?:en|pl)/job/[^"]+)"')
_LIST_JOB_LINK_ABS_RE = re.compile(r'href="(https://nofluffjobs\.com/(?:en|pl)/job/[^"]+)"', re.I)
_ANY_JOB_URL_RE = re.compile(r'https://nofluffjobs\.com/(?:en|pl)/job/[A-Za-z0-9_\-\/]+', re.I)

def _normalize_job_url(u: str) -> str:
    return u if u.startswith("http") else f"https://nofluffjobs.com{u}"

def _listing_urls() -> Iterable[str]:
    base = "https://nofluffjobs.com"
    cats = [c.strip() for c in NFJ_CATEGORIES if c.strip()]
    for cat in cats:
        for p in range(1, NFJ_PAGES + 1):
            yield f"{base}/{NFJ_COUNTRY}/{cat}?page={p}"
        if NFJ_REMOTE:
            for p in range(1, NFJ_PAGES + 1):
                yield f"{base}/{NFJ_COUNTRY}/remote/{cat}?page={p}"

def _extract_links_from_listing(html: str) -> List[str]:
    links = []
    for m in _LIST_JOB_LINK_RE.finditer(html):
        links.append(_normalize_job_url(m.group(1)))
    for m in _LIST_JOB_LINK_ABS_RE.finditer(html):
        links.append(_normalize_job_url(m.group(1)))
    for m in _ANY_JOB_URL_RE.finditer(html):
        links.append(_normalize_job_url(m.group(0)))

    seen: Set[str] = set()
    uniq = []
    for u in links:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq

def _extract_job_from_html(html: str, url: str) -> Dict:
    # Szukamy JSON-LD JobPosting
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
    desc = ""
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
        desc = job.get("description") or ""
        posted = job.get("datePosted") or job.get("validFrom")

    # Fallback lokalizacji: remote/hybrid, a potem prosty wycinek z JSON-a
    if not location:
        if _REMOTE_HINT.search(html) or _REMOTE_HINT.search(title) or _REMOTE_HINT.search(desc or ""):
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
        "desc": desc,
        "source": SOURCE_NAME,
        "posted_at": posted,
        "url": url,
        "seniority": _sen_from_title(title),
    }

def _safe_get(session: requests.Session, url: str, timeout: int = 30) -> Optional[str]:
    try:
        r = session.get(url, timeout=timeout)
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None

def fetch_jobs(limit: int = 50) -> List[Dict]:
    """
    Realne oferty z NoFluffJobs przez HTML (bez Apify), z równoległym pobieraniem stron ogłoszeń.
    """
    limit = max(1, min(5000, int(limit)))
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Language": "pl,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "no-cache",
    })

    # 1) Zbierz jak najwięcej linków do /job/… z wielu listingów
    all_job_urls: List[str] = []
    seen_listings: Set[str] = set()
    for lst_url in _listing_urls():
        if lst_url in seen_listings:
            continue
        seen_listings.add(lst_url)
        html = _safe_get(sess, lst_url)
        time.sleep(NFJ_DELAY)
        if not html:
            continue
        links = _extract_links_from_listing(html)
        all_job_urls.extend(links)
        # jeśli już mamy sporo, możemy przejść do fetchu stron ogłoszeń
        if len(all_job_urls) >= limit * 2:
            break

    # unikalne URL-e
    seen_jobs: Set[str] = set()
    uniq_job_urls = []
    for u in all_job_urls:
        if u not in seen_jobs:
            seen_jobs.add(u)
            uniq_job_urls.append(u)

    # 2) Równoległe pobieranie stron ogłoszeń i parsowanie JSON-LD
    out: List[Dict] = []
    if not uniq_job_urls:
        return out

    def fetch_one(u: str) -> Optional[Dict]:
        jhtml = _safe_get(sess, u)
        # delikatny throttle per-request (nawet w wątkach)
        time.sleep(NFJ_DELAY)
        if not jhtml:
            return None
        return _extract_job_from_html(jhtml, u)

    with ThreadPoolExecutor(max_workers=max(1, NFJ_WORKERS)) as ex:
        futures = [ex.submit(fetch_one, u) for u in uniq_job_urls]
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
