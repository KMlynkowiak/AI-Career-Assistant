# services/worker/etl/main.py
from __future__ import annotations

import os
import re
import json
import logging
import datetime as dt
from pathlib import Path
from typing import List, Dict, Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, insert, delete, text
from sqlalchemy.engine import Engine

from services.worker.etl.schema import metadata, jobs_table, jobs_clean
from services.worker.etl.dedup import simple_dedup
from services.worker.etl.nlp import extract_skills, infer_seniority

# Źródła ogłoszeń
from services.worker.etl.sources.nofluff import fetch_jobs as fetch_nfj
from services.worker.etl.sources.jj_apify import fetch_jobs as fetch_jj

# -----------------------
# Konfiguracja i logging
# -----------------------
load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/ai_jobs.db")
NFJ_LIMIT = int(os.getenv("NFJ_LIMIT", "200"))
JJ_LIMIT = int(os.getenv("JJ_LIMIT", "200"))
RAW_DUMP = os.getenv("RAW_DUMP") == "1"  # <- włącz zapisy surowych plików

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("etl")


def get_engine() -> Engine:
    return create_engine(f"sqlite:///{DB_PATH}", future=True)


def upsert_raw(engine: Engine, rows: List[Dict]):
    """Czyści jobs_table i wstawia świeże rekordy (prosta, powtarzalna demo-ładowarka)."""
    with engine.begin() as conn:
        conn.execute(delete(jobs_table))
        if rows:
            conn.execute(insert(jobs_table), rows)

# ---------------------------------
# Seniority – HELPERY I PRIORYTETY
# ---------------------------------

# szerokie synonimy
_JUN_RE = re.compile(r"\b(junior|intern|trainee|student|jr)\b", re.I)
_SEN_RE = re.compile(r"\b(senior|lead|principal|staff|expert|architect|sr)\b", re.I)
_MID_RE = re.compile(r"\b(mid|middle|regular)\b", re.I)

def normalize_source_seniority(value: Optional[str]) -> Optional[str]:
    """
    Priorytet 1: to co mówi źródło.
      - Jeśli WARTOŚĆ NIE ISTNIEJE (None) → zwróć None (DAJ SZANSĘ TYTUŁOWI).
      - Jeśli źródło jawnie mówi 'brak/unspecified/empty' → 'Mid'.
      - Jeśli Junior/Senior/Mid (lub synonimy) → odpowiedni poziom.
      - Jeśli nic nie pasuje → None (spróbuje tytuł).
    """
    if value is None:
        return None
    v = str(value).strip().lower()
    if v in {"", "brak", "unspecified", "unknown", "none", "n/a", "na"}:
        return "Mid"
    if any(k in v for k in ["junior", "intern", "trainee", "student", "jr"]):
        return "Junior"
    if any(k in v for k in ["senior", "lead", "principal", "staff", "expert", "architect", "sr"]):
        return "Senior"
    if any(k in v for k in ["regular", "middle", "mid"]):
        return "Mid"
    return None  # zostaw tytuł/NLP do decyzji

def infer_from_title(title: str) -> Optional[str]:
    """Priorytet 2: tytuł."""
    t = (title or "").strip()
    if not t:
        return None
    if _JUN_RE.search(t):
        return "Junior"
    if _SEN_RE.search(t):
        return "Senior"
    if _MID_RE.search(t):
        return "Mid"
    return None

def choose_seniority(raw_sen: Optional[str], title: str, desc: str) -> str:
    """
    Priorytety:
      1) źródło (normalize_source_seniority)
      2) tytuł
      3) NLP (opcjonalny tie-breaker)
      4) domyślnie 'Mid'
    """
    s1 = normalize_source_seniority(raw_sen)
    if s1 is not None:
        return s1

    s2 = infer_from_title(title)
    if s2 is not None:
        return s2

    s3 = infer_seniority(f"{title} {desc}") or None
    if s3:
        lo = str(s3).lower()
        if "jun" in lo:
            return "Junior"
        if "sen" in lo:
            return "Senior"
        return "Mid"

    return "Mid"

# -----------------------
# RAW dump helper (JSONL)
# -----------------------

def dump_jsonl(rows: List[Dict] | None, name: str):
    """Zapisz listę dictów do data/raw/<name>_YYYYmmdd_HHMMSS.jsonl"""
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = Path(f"data/raw/{name}_{ts}.jsonl")
    with path.open("w", encoding="utf-8") as f:
        for r in rows or []:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    logger.info("RAW dump -> %s (%d rekordów)", path, len(rows or []))

# -----------------------
# Transform + Load CLEAN
# -----------------------

def build_clean_rows(rows: List[Dict]) -> List[Dict]:
    """
    Normalizacja pod jobs_clean + proste NLP (skills) + klasyfikacja seniority.
    """
    out: List[Dict] = []
    for r in rows:
        title = (r.get("title") or "").strip()
        desc = (r.get("desc") or r.get("description") or "").strip()
        raw_sen = r.get("seniority")  # jeśli źródło coś daje – użyjemy najpierw

        skills_list = extract_skills(desc) or []
        skills_csv = ", ".join(sorted({s.strip() for s in skills_list if s}))

        seniority = choose_seniority(raw_sen=raw_sen, title=title, desc=desc)

        out.append(
            {
                "id": r.get("id"),
                "title": title,
                "company": r.get("company"),
                "location": r.get("location"),
                "desc": desc,
                "source": r.get("source"),
                "posted_at": r.get("posted_at"),  # może być None
                "url": r.get("url"),
                "skills": skills_csv,
                "seniority": seniority,
            }
        )
    return out


def upsert_clean(engine: Engine, rows: List[Dict]):
    """Czyści jobs_clean i wstawia świeże rekordy."""
    with engine.begin() as conn:
        conn.execute(delete(jobs_clean))
        if rows:
            conn.execute(insert(jobs_clean), rows)

# -----------
# Główny ETL
# -----------

def main():
    logger.info("Start ETL | DB_PATH=%s", DB_PATH)
    engine = get_engine()

    # Schemat
    metadata.create_all(engine)
    logger.info("Połączono z bazą i upewniono się, że schemat istnieje")

    # 1) Extract
    logger.info(
        "Pobieram oferty: NoFluffJobs (limit=%d) + JustJoinIT/Apify (limit=%d)",
        NFJ_LIMIT, JJ_LIMIT,
    )
    nfj = fetch_nfj(limit=NFJ_LIMIT) or []
    logger.info("NFJ: pobrano %d ofert", len(nfj))
    if RAW_DUMP:
        dump_jsonl(nfj, "nfj")

    try:
        jj = fetch_jj(limit=JJ_LIMIT) or []
        logger.info("JJ: pobrano %d ofert", len(jj))
        if RAW_DUMP:
            dump_jsonl(jj, "jj")
    except Exception as e:
        logger.warning("JJ (Apify) pominięte: %s", e)
        jj = []

    rows = nfj + jj
    logger.info("Razem surowych rekordów: %d", len(rows))

    # 2) Dedup
    rows_dedup = simple_dedup(rows, key="id")
    logger.info("Po deduplikacji: %d (usunięto %d)", len(rows_dedup), len(rows) - len(rows_dedup))

    # 3) Load RAW
    upsert_raw(engine, rows_dedup)
    logger.info("Zapisano %d rekordów do jobs_table", len(rows_dedup))

    # 4) Transform + klasyfikacja seniority wg priorytetów
    clean_rows = build_clean_rows(rows_dedup)
    logger.info("Wzbogacono NLP (skills/seniority) dla %d rekordów", len(clean_rows))

    # 5) Load CLEAN
    upsert_clean(engine, clean_rows)
    logger.info("Zapisano %d rekordów do jobs_clean", len(clean_rows))

    # 6) Metryki
    with engine.begin() as conn:
        rc = conn.execute(text("SELECT COUNT(*) FROM jobs_clean")).scalar_one()
        sc = conn.execute(text("SELECT COUNT(DISTINCT company) FROM jobs_clean")).scalar_one()
    logger.info("Metryki: jobs_clean=%s, firmy=%s", rc, sc)

    logger.info("ETL zakończony sukcesem")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("ETL failed")
        raise
