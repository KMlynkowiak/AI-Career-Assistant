# services/worker/etl/main.py
from __future__ import annotations

import os
import json
import logging
import datetime as dt
from pathlib import Path
from typing import List, Dict

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from services.worker.etl.schema import metadata, jobs_table, jobs_clean
from services.worker.etl.sources.nofluff import fetch_jobs as fetch_nfj

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/ai_jobs.db")
NFJ_LIMIT = int(os.getenv("NFJ_LIMIT", str(10_000)))   # ile maksymalnie w TEJ sesji (baza akumuluje)
RAW_DUMP = os.getenv("RAW_DUMP") == "1"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("etl")

def get_engine() -> Engine:
    return create_engine(f"sqlite:///{DB_PATH}", future=True)

def ensure_unique_indexes(engine: Engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS jobs_table (
                id TEXT, title TEXT, company TEXT, location TEXT, seniority TEXT, url TEXT, posted_at TEXT, source TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS jobs_clean (
                id TEXT, title TEXT, company TEXT, location TEXT, seniority TEXT, url TEXT, posted_at TEXT, source TEXT
            )
        """))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_table_id ON jobs_table(id)"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_clean_id ON jobs_clean(id)"))

def bulk_upsert(engine: Engine, table, rows: List[Dict], update_cols: List[str]):
    if not rows:
        return
    with engine.begin() as conn:
        stmt = sqlite_insert(table).values(rows)
        update_dict = {c: getattr(stmt.excluded, c) for c in update_cols}
        stmt = stmt.on_conflict_do_update(index_elements=["id"], set_=update_dict)
        conn.execute(stmt)

def dump_jsonl(rows: List[Dict] | None, name: str):
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = Path(f"data/raw/{name}_{ts}.jsonl")
    with path.open("w", encoding="utf-8") as f:
        for r in rows or []:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    logger.info("RAW dump -> %s (%d rekordów)", path, len(rows or []))

def main():
    logger.info("Start ETL | DB_PATH=%s", DB_PATH)
    engine = get_engine()
    metadata.create_all(engine)
    ensure_unique_indexes(engine)
    logger.info("Połączono z bazą i upewniono się, że schemat istnieje")

    logger.info("Pobieram oferty: NoFluffJobs(HTML) (limit=%d)", NFJ_LIMIT)
    rows = fetch_nfj(limit=NFJ_LIMIT) or []
    logger.info("NFJ: pobrano %d ofert (przed dedup w bazie)", len(rows))
    if RAW_DUMP:
        dump_jsonl(rows, "nfj")

    # UPSERT do RAW
    bulk_upsert(engine, jobs_table, rows, update_cols=["title","company","location","seniority","url","posted_at","source"])
    # CLEAN = to samo (minimalny zestaw pól)
    bulk_upsert(engine, jobs_clean, rows, update_cols=["title","company","location","seniority","url","posted_at","source"])

    with engine.begin() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM jobs_clean")).scalar_one()
        firms = conn.execute(text("SELECT COUNT(DISTINCT company) FROM jobs_clean")).scalar_one()
    logger.info("Metryki: jobs_clean=%s, firmy=%s", total, firms)
    logger.info("ETL zakończony sukcesem")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("ETL failed")
        raise
