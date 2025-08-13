# services/worker/etl/sources/nofluff.py
from __future__ import annotations

import os
import re
import json
import time
import datetime as dt
from typing import List, Dict, Optional, Iterable

import requests

SOURCE_NAME = "NoFluffJobs(HTML)"

# Konfiguracja scrapera przez ENV (prosto, bez zależności):
NFJ_COUNTRY = os.getenv("NFJ_COUNTRY", "pl").strip()          # "pl"
NFJ_CATEGORIES = os.getenv("NFJ_CATEGORIES", "data,backend").split(",")
NFJ_REMOTE = os.getenv("NFJ_REMOTE", "1") == "1"              # 1 => użyj też /remote/<cat>
NFJ_PAGES = int(os.getenv("NFJ_PAGES", "5"))                  # ile stron listy na kategorię
NFJ_DELAY = float(os.getenv("NFJ_DELAY", "0.8"))              # sekundy między requestami

USER_AGENT = os.getenv(
    "NFJ_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome Safari"
)

# Heurystyki seniority
_JUN = re.compile(r"\b(junior|intern|trainee|student|jr)\b", re.I)
_SEN = re.compile(r"\b(senior|lead|principal|staff|expert|architect|sr)\b", re.I)
_MID = re.compile(r"\b(mid|middle|regular)\b", re.I)

def _guess_seniority_from_title(title: str) -> str:
    t = title or ""
    if _JUN.search(t): return "Junior"
    if _SEN.search(t): return "Senior"
    if _MID.search(t): return "Mid"
    return "Mid"

# ---------- HTML helpers ----------

_LIST_JOB_LINK_RE = re.compile(r'href="(/(?:en|pl)/job/[^"]+)"')
_LIST_JOB_LINK_ABS_RE = re.compile(r'href="(https://nofluffjobs\.com/(?:en|pl)/job/[^"]+)"', re.I)

def _normalize_job_url(u: str) -> str:
    if u.startswith("http"):
        return u
    return f"https://nofluffjobs.com{u}"

def _listing_urls() -> Iterable[str]:
    base = "https://nofluffjobs.com"
    cats = [c.strip() for c in NFJ_CATEGORIES if c.strip()]
    for cat in cats:
        # /pl/<cat>?page=1..N
        for p in range(1, NFJ_PAGES + 1):
            yield f"{base}/{NFJ_COUNTRY}/{cat}?page={p}"
        if NFJ_REMOTE:
            for p in range(1, NFJ_PAGES + 1):
                yield f"{base}/{NFJ_COUNTRY}/remote/{cat}?page={p}"

def _extract_links_from_listing(html: str) -> List[str]:
    # proste wyłuskanie linków do /pl/job/...
    links = []
    for m in _LIST_JOB_LINK_RE.finditer(html):
        links.append(_normalize_job_url(m.group(1)))
    for m in _LIST_JOB_LINK_ABS_RE.finditer(html):
        links.append(_normalize_job_url(m.group(1)))
    # unikalność + zachowanie kolejności
    seen = set()
    uniq = []
    for u in links:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq

def _extract_job_from_html(html: str, url: str) -> Dict:
    # Szukamy bloków <script type="application/ld+json"> z JobPosting
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
        # firma
        hiring = job.get("hiringOrganization") or {}
        if isinstance(hiring, dict):
            company = hiring.get("name") or ""
        # lokalizacja
        locobj = job.get("jobLocation")
        addr = {}
        if isinstance(locobj, list) and locobj:
            addr = locobj[0].get("address", {}) or {}
        elif isinstance(locobj, dict):
            addr = (locobj.get("address", {}) or {})
        location = addr.get("addressLocality") or addr.get("addressRegion") or addr.get("addressCountry") or ""
        # opis + data
        desc = job.get("description") or ""
        posted = job.get("datePosted") or job.get("validFrom")
    else:
        # awaryjnie: tytuł strony
        mt = re.search(r"<title>(.*?)</title>", html, re.S|re.I)
        if mt:
            title = mt.group(1).split("|")[0].strip()

    return {
        "id": url,
        "title": title,
        "company": company,
        "location": location,
        "desc": desc,
        "source": SOURCE_NAME,
        "posted_at": posted,
        "url": url,
        "seniority": _guess_seniority_from_title(title),
    }

def _safe_get(session: requests.Session, url: str, timeout: int = 30) -> Optional[str]:
    try:
        r = session.get(url, timeout=timeout)
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None

# ---------- Public API ----------

def fetch_jobs(limit: int = 50) -> List[Dict]:
    """
    Realne oferty z NoFluffJobs przez HTML (bez Apify).
    - przechodzi po stronach list (kilka kategorii, tryb remote),
    - wyciąga linki do ogłoszeń,
    - z podstron parsuje JSON-LD JobPosting.

    Uwaga: serwis bywa dynamiczny (SPA). Ten kod nie gwarantuje 100% skuteczności,
    ale działa bezpłatnie i minimalnie obciąża stronę (NFJ_DELAY).
    """
    limit = max(1, min(5000, int(limit)))
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Language": "pl,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "no-cache",
    })

    out: List[Dict] = []
    seen = set()

    for lst_url in _listing_urls():
        if len(out) >= limit:
            break
        html = _safe_get(sess, lst_url)
        time.sleep(NFJ_DELAY)
        if not html:
            continue

        links = _extract_links_from_listing(html)
        for job_url in links:
            if len(out) >= limit:
                break
            if job_url in seen:
                continue
            seen.add(job_url)

            jhtml = _safe_get(sess, job_url)
            time.sleep(NFJ_DELAY)
            if not jhtml:
                continue

            rec = _extract_job_from_html(jhtml, job_url)
            # minimalne sanity – potrzebujemy chociaż tytułu i firmy
            if not rec.get("title"):
                continue
            out.append(rec)

    # brak wyników? Zwróć pustą listę (ETL ma fallback w innych źródłach, jeśli dodasz)
    today = dt.date.today().isoformat()
    for r in out:
        r.setdefault("posted_at", today)
        r.setdefault("source", SOURCE_NAME)

    return out[:limit]
