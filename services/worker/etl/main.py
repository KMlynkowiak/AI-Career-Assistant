# services/worker/etl/main.py
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, insert, delete, text
from schema import metadata, jobs_table, jobs_clean
from dedup import simple_dedup
from nlp import extract_skills, infer_seniority

# ŹRÓDŁA (Adzuna działa stabilnie; NFJ może czasem zwrócić 0)
from sources.adzuna import fetch_jobs as fetch_adzuna
try:
    from sources.nofluff import fetch_jobs as fetch_nofluff
except Exception:
    fetch_nofluff = None

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/ai_jobs.db")

def get_engine():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return create_engine(f"sqlite:///{DB_PATH}", echo=False)

def ensure_tables(engine):
    metadata.create_all(engine)

def ingest_raw(engine):
    print("[ETL] fetching from Adzuna...")
    a = fetch_adzuna(limit=150, query="data OR python OR sql")
    print(f"[ETL] Adzuna: {len(a)}")

    n = []
    if fetch_nofluff:
        print("[ETL] fetching from NoFluffJobs...")
        try:
            n = fetch_nofluff(limit=150, query="data")
        except Exception as e:
            print(f"[ETL] NFJ error: {e}")
        print(f"[ETL] NFJ: {len(n)}")

    data = a + n
    print(f"[ETL] total before dedup: {len(data)}")
    data = simple_dedup(data, key="id")
    print(f"[ETL] total after dedup:  {len(data)}")

    with engine.begin() as conn:
        conn.execute(delete(jobs_table))  # pełna podmiana (demo)
        for row in data:
            conn.execute(insert(jobs_table).values(**row))
    return len(data)

def transform_clean(engine):
    with engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT id,title,company,location,description,source FROM jobs_raw"
        )).mappings().all()
        conn.execute(delete(jobs_clean))
        for r in rows:
            skills = extract_skills(r["description"] or "")
            seniority = infer_seniority(f"{r['title']} {r['description'] or ''}")
            conn.execute(insert(jobs_clean).values(
                id=r["id"],
                title=r["title"],
                company=r["company"],
                location=r["location"],
                skills=",".join(skills),
                seniority=seniority,
                tech_stack=",".join(skills),
                source=r["source"]
            ))

def main():
    engine = get_engine()
    ensure_tables(engine)
    count = ingest_raw(engine)
    transform_clean(engine)
    print(f"ETL done. Loaded {count} rows into jobs_clean at {DB_PATH}")

if __name__ == "__main__":
    main()
