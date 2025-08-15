# services/worker/etl/main.py
from __future__ import annotations

import os
import json
import logging
import datetime as dt
from pathlib import Path
from typing import List, Dict, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from services.worker.etl.schema import metadata, jobs_table, jobs_clean
from services.worker.etl.sources.nofluff import iter_job_urls, fetch_job

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "data/ai_jobs.db")

# limit na JEDEN bieg (baza i tak akumuluje; ustaw spory, np. 10000)
NFJ_LIMIT = int(os.getenv("NFJ_LIMIT", "10000"))

# ile równolegle pobierać stron ogłoszeń
NFJ_WORKERS = int(os.getenv("NFJ_WORKERS", "10"))

# zapisuj partiami co N rekordów (żeby przerwanie nie kasowało postępu)
ETL_FLUSH_EVERY = int(os.getenv("ETL_FLUSH_EVERY", "150"))

# pominąć oferty, które JUŻ są w bazie? (szybsze odświeżenia)
NFJ_SKIP_EXISTING = os.getenv("NFJ_SKIP_EXISTING", "1") == "1"

# dump surowych partii do data/raw/
RAW_DUMP = os.getenv("RAW_DUMP") == "1"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("etl-nfj")

def get_engine() -> Engine:
    return create_engine(f"sqlite:///{DB_PATH}", future=True)

def ensure_schema(engine: Engine):
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_table_id ON jobs_table(id)"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_clean_id ON jobs_clean(id)"))

def bulk_upsert(engine: Engine, rows: List[Dict]):
    if not rows:
        return
    with engine.begin() as conn:
        stmt = sqlite_insert(jobs_table).values(rows)
        upd = {c: getattr(stmt.excluded, c) for c in ["title","company","location","seniority","url","posted_at","source"]}
        conn.execute(stmt.on_conflict_do_update(index_elements=["id"], set_=upd))

        stmt2 = sqlite_insert(jobs_clean).values(rows)
        upd2 = {c: getattr(stmt2.excluded, c) for c in ["title","company","location","seniority","url","posted_at","source"]}
        conn.execute(stmt2.on_conflict_do_update(index_elements=["id"], set_=upd2))

def dump_jsonl(rows: List[Dict], tag: str):
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    p = Path(f"data/raw/{tag}_{ts}.jsonl")
    with p.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    logger.info("RAW dump -> %s (%d)", p, len(rows))

def load_existing_ids(engine: Engine) -> set:
    if not NFJ_SKIP_EXISTING:
        return set()
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT id FROM jobs_clean")).fetchall()
    return {r[0] for r in rows}

def main():
    logger.info("Start ETL NFJ-only | DB_PATH=%s", DB_PATH)
    engine = get_engine()
    ensure_schema(engine)

    existing = load_existing_ids(engine)
    if existing:
        logger.info("Pomijam istniejące oferty: %d", len(existing))

    # Zbiór URL-i (generator) -> filtr na istniejące -> do listy „todo”
    todo: List[str] = []
    taken = 0
    for u in iter_job_urls(limit=NFJ_LIMIT):
        if NFJ_SKIP_EXISTING and u in existing:
            continue
        todo.append(u)
        taken += 1
    logger.info("Zebrane URL-e ofert do pobrania: %d (po odfiltrowaniu istniejących)", len(todo))

    saved_total = 0
    buf: List[Dict] = []

    try:
        with ThreadPoolExecutor(max_workers=max(1, NFJ_WORKERS)) as ex:
            futures = [ex.submit(fetch_job, u) for u in todo]
            for i, fut in enumerate(as_completed(futures), start=1):
                try:
                    rec = fut.result()
                except Exception:
                    rec = None
                if rec and rec.get("title"):
                    buf.append(rec)

                if buf and (len(buf) >= ETL_FLUSH_EVERY):
                    bulk_upsert(engine, buf)
                    saved_total += len(buf)
                    if RAW_DUMP: dump_jsonl(buf, "nfj_part")
                    logger.info("Zapisano partię: +%d (łącznie: %d / %d)", len(buf), saved_total, len(todo))
                    buf.clear()

        # flush końcowy
        if buf:
            bulk_upsert(engine, buf)
            saved_total += len(buf)
            if RAW_DUMP: dump_jsonl(buf, "nfj_part")
            logger.info("Zapisano ostatnią partię: +%d (łącznie: %d / %d)", len(buf), saved_total, len(todo))
            buf.clear()

    except KeyboardInterrupt:
        # flush tego co mamy i eleganckie wyjście
        if buf:
            bulk_upsert(engine, buf)
            saved_total += len(buf)
            if RAW_DUMP: dump_jsonl(buf, "nfj_part")
            logger.info("Przerwano — zapisano bieżącą partię: +%d (łącznie: %d / %d)", len(buf), saved_total, len(todo))

    # metryki
    with engine.begin() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM jobs_clean")).scalar_one()
        firms = conn.execute(text("SELECT COUNT(DISTINCT company) FROM jobs_clean")).scalar_one()
    logger.info("NFJ-only — run summary: saved_in_run=%d | total_in_db=%d | firms=%d", saved_total, total, firms)
    logger.info("ETL zakończony")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("ETL failed")
        raise
