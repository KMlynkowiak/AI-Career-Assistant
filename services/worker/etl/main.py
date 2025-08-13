# services/worker/etl/main.py
import os
import logging
from typing import List, Dict

from dotenv import load_dotenv
from sqlalchemy import create_engine, insert, delete, text
from sqlalchemy.engine import Engine

# Lokalny import struktur i narzędzi w projekcie
# (pliki już są w repo)
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("etl")


def get_engine() -> Engine:
    # sqlite:///relative_path
    uri = f"sqlite:///{DB_PATH}"
    engine = create_engine(uri, future=True)
    return engine


def upsert_raw(engine: Engine, rows: List[Dict]):
    """Wrzuca rekordy do tabeli surowej (jobs_table).
    Prosto: czyścimy tabelę i wstawiamy od nowa (dla demo/portfolio)."""
    with engine.begin() as conn:
        conn.execute(delete(jobs_table))
        if rows:
            conn.execute(insert(jobs_table), rows)


def build_clean_rows(rows: List[Dict]) -> List[Dict]:
    """Wzbogacanie NLP + normalizacja pod jobs_clean."""
    out = []
    for r in rows:
        desc = r.get("desc") or r.get("description") or ""
        skills_list = extract_skills(desc) or []
        seniority = infer_seniority(f"{r.get('title','')} {desc}") or None

        out.append(
            {
                "id": r.get("id"),
                "title": r.get("title"),
                "company": r.get("company"),
                "location": r.get("location"),
                "desc": desc,
                "source": r.get("source"),
                "posted_at": r.get("posted_at"),
                "url": r.get("url"),
                "skills": ", ".join(sorted(set([s.strip() for s in skills_list if s]))),
                "seniority": seniority,
            }
        )
    return out


def upsert_clean(engine: Engine, rows: List[Dict]):
    """Wrzuca rekordy do tabeli przetworzonej (jobs_clean)."""
    with engine.begin() as conn:
        conn.execute(delete(jobs_clean))
        if rows:
            conn.execute(insert(jobs_clean), rows)


def main():
    logger.info("Start ETL | DB_PATH=%s", DB_PATH)
    engine = get_engine()

    # Tworzymy schemat jeśli brak
    metadata.create_all(engine)
    logger.info("Połączono z bazą i upewniono się, że schemat istnieje")

    # 1) Extract
    logger.info("Pobieram oferty: NoFluffJobs (limit=%d) + JustJoinIT/Apify (limit=%d)", NFJ_LIMIT, JJ_LIMIT)
    nfj = fetch_nfj(limit=NFJ_LIMIT) or []
    logger.info("NFJ: pobrano %d ofert", len(nfj))

    # JJ via Apify może wymagać tokena; jeśli nie ma – zrób pustą listę i zaloguj
    try:
        jj = fetch_jj(limit=JJ_LIMIT) or []
        logger.info("JJ: pobrano %d ofert", len(jj))
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

    # 4) Transform + NLP
    clean_rows = build_clean_rows(rows_dedup)
    logger.info("Wzbogacono NLP (skills/seniority) dla %d rekordów", len(clean_rows))

    # 5) Load CLEAN
    upsert_clean(engine, clean_rows)
    logger.info("Zapisano %d rekordów do jobs_clean", len(clean_rows))

    # 6) Kilka prostych metryk do logów
    with engine.begin() as conn:
        rc = conn.execute(text("SELECT COUNT(*) FROM jobs_clean")).scalar_one()
        sc = conn.execute(text("SELECT COUNT(DISTINCT company) FROM jobs_clean")).scalar_one()
    logger.info("Metryki: jobs_clean=%s, firmy=%s", rc, sc)

    logger.info("ETL zakończony sukcesem")


if __name__ == "__main__":
    try:
        main()
    except Exception:
