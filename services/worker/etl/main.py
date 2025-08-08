import os
from sqlalchemy import create_engine, insert, text, delete
from schema import metadata, jobs_table, jobs_clean
from sources.dummy_source import fetch_jobs
from dedup import simple_dedup
from nlp import extract_skills, infer_seniority

DB_PATH = os.getenv("DB_PATH", "data/ai_jobs.db")

def get_engine():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return create_engine(f"sqlite:///{DB_PATH}", echo=False)

def ingest_raw(engine):
    data = fetch_jobs()
    data = simple_dedup(data, key="id")
    with engine.begin() as conn:
        conn.execute(delete(jobs_table))  # demo: replace
        for row in data:
            conn.execute(insert(jobs_table).values(**row))
    return len(data)

def transform_clean(engine):
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT id,title,company,location,description,source FROM jobs_raw")).mappings().all()
        conn.execute(delete(jobs_clean))
        for r in rows:
            skills = extract_skills(r["description"] or "")
            seniority = infer_seniority(" ".join([r["title"], r["description"] or ""]))
            tech_stack = ",".join(skills)
            conn.execute(insert(jobs_clean).values(
                id=r["id"],
                title=r["title"],
                company=r["company"],
                location=r["location"],
                skills=",".join(skills),
                seniority=seniority,
                tech_stack=tech_stack,
                source=r["source"]
            ))

def ensure_tables(engine):
    metadata.create_all(engine)

def main():
    engine = get_engine()
    ensure_tables(engine)
    n = ingest_raw(engine)
    transform_clean(engine)
    print(f"ETL done. Loaded {n} rows into jobs_clean at {DB_PATH}")

if __name__ == "__main__":
    main()
