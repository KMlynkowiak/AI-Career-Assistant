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
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from services.worker.etl.schema import metadata, jobs_table, jobs_clean
from services.worker.etl.dedup import simple_dedup
from services.worker.etl.nlp import extract_skills, infer_seniority

# Źródła
from services.worker.etl.sources.nofluff import fetch_jobs as fetch_nfj
from services.worker.etl.sources.jj_apify import fetch_jobs as fetch_jj

# -----------------------
# Konfiguracja i logging
# -----------------------
load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/ai_jobs.db")
NFJ_LIMIT = int(os.getenv("NFJ_LIMIT", "500"))  # ile maksymalnie pobierać W TEJ SESJI
JJ_LIMIT = int(os.getenv("JJ_LIMIT", "0"))
RAW_DUMP = os.getenv("RAW_DUMP") == "1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("etl")


def get_engine() -> Engine:
    return create_engine(f"sqlite:///{DB_PATH}", future=True)


def ensure_unique_indexes(engine: Engine):
    """Zapewnij unikalność 'id' (wymagane dla UPSERT)."""
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS jobs_table (id TEXT, title TEXT, company TEXT, location TEXT, desc TEXT, source TEXT, posted_at TEXT, url TEXT, seniority TEXT)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS jobs_clean  (id TEXT, title TEXT, company TEXT, location TEXT, desc TEXT, source TEXT, posted_at TEXT, url TEXT, skills TEXT, seniority TEXT)"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_table_id ON jobs_table(id)"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_clean_id ON jobs_clean(id)"))


def bulk_upsert(engine: Engine, table, rows: List[Dict], update_cols: List[str]):
    """UPSERT po kluczu 'id' (SQLite)."""
    if not rows:
        return
    with engine.begin() as conn:
        stmt = sqlite_insert(table).values(rows)
        update_dict = {c: getattr(stmt.excluded, c) for c in update_cols}
        stmt = stmt.on_conflict_do_update(index_elements=["id"], set_=update_dict)
        conn.execute(stmt)

# ---------------------------------
# Seniority – HELPERY I PRIORYTETY
# ---------------------------------

_JUN_RE = re.compile(r"\b(junior|intern|trainee|student|jr)\b", re.I)
_SEN_RE = re.compile(r"\b(senior|lead|principal|staff|expert|architect|sr)\b", re.I)
_MID_RE = re.compile(r"\b(mid|middle|regular)\b", re.I)

def normalize_source_seniority(value: Optional[str]) -> Optional[str]:
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
    return None

def infer_from_title(title: str) -> Optional[str]:
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
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = Path(f"data/raw/{name}_{ts}.jsonl")
    with path.open("w", encoding="utf-8") as f:
        for r in rows or []:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    logger.info("RAW dump -> %s (%d rekordów)", path, len(rows or []))

# -----------------------
# Transform → CLEAN rows
# -----------------------
def build_clean_rows(rows: List[Dict]) -> List[Dict]:
    out: List[Dict] = []
    for r in rows:
        title = (r.get("title") or "").strip()
        desc = (r.get("desc") or r.get("description") or "").strip()
        raw_sen = r.get("seniority")
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
                "posted_at": r.get("posted_at"),
                "url": r.get("url"),
                "skills": skills_csv,
                "seniority": seniority,
            }
        )
    return out

# -----------
# Główny ETL
# -----------
def main():
    logger.info("Start ETL | DB_PATH=%s", DB_PATH)
    engine = get_engine()

    # Schemat + unikalne indeksy (do UPSERT)
    metadata.create_all(engine)
    ensure_unique_indexes(engine)
    logger.info("Połączono z bazą i upewniono się, że schemat istnieje")

    # 1) Extract (tu możesz zwiększać zasięg – PAGES/kategorie – a my zapiszemy WSZYSTKO)
    logger.info(
        "Pobieram oferty: NoFluffJobs(HTML) (limit=%d) + JustJoinIT/Apify (limit=%d)",
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
    logger.info("Razem surowych rekordów (przed dedup): %d", len(rows))

    # 2) Dedup (wewnątrz wsadu) – baza i tak ma unikalność po 'id'
    rows_dedup = simple_dedup(rows, key="id")
    logger.info("Po deduplikacji wsadu: %d (usunięto %d)", len(rows_dedup), len(rows) - len(rows_dedup))

    # 3) UPSERT RAW (DOPISUJEMY/AKTUALIZUJEMY – NIC NIE KASUJEMY)
    bulk_upsert(
        engine,
        jobs_table,
        rows_dedup,
        update_cols=["title", "company", "location", "desc", "source", "posted_at", "url", "seniority"],
    )

    # 4) Transform → CLEAN
    clean_rows = build_clean_rows(rows_dedup)
    logger.info("Wzbogacono NLP (skills/seniority) dla %d rekordów", len(clean_rows))

    # 5) UPSERT CLEAN
    bulk_upsert(
        engine,
        jobs_clean,
        clean_rows,
        update_cols=["title", "company", "location", "desc", "source", "posted_at", "url", "skills", "seniority"],
    )
    if RAW_DUMP:
        dump_jsonl(clean_rows, "clean")

    # 6) Metryki
    with engine.begin() as conn:
        rc = conn.execute(text("SELECT COUNT(*) FROM jobs_clean")).scalar_one()
        sc = conn.execute(text("SELECT COUNT(DISTINCT company) FROM jobs_clean")).scalar_one()
    logger.info("Metryki: jobs_clean=%s (łącznie w bazie), firmy=%s", rc, sc)
    logger.info("ETL zakończony sukcesem")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("ETL failed")
        raise
