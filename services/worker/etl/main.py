# services/worker/etl/main.py
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, insert, delete, text
from schema import metadata, jobs_table, jobs_clean
from dedup import simple_dedup
from nlp import extract_skills, infer_seniority

from sources.nofluff import fetch_jobs as fetch_nfj
from sources.jj_apify import fetch_jobs as fetch_jj

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/ai_jobs.db")

def get_engine():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return create_engine(f"sqlite:///{DB_PATH}", echo=False)

def ensure_tables(engine):
    metadata.create_all(engine)

def ingest_raw(engine):
    print("[ETL] fetching from NoFluffJobs...")
    nfj = fetch_nfj(limit=200, query="data OR python OR sql")
    print(f"[ETL] NFJ: {len(nfj)}")

    print("[ETL] fetching from JustJoinIT (Apify)...")
    jj = fetch_jj(limit=200, query="data OR python OR sql")
    print(f"[ETL] JJ: {len(jj)}")

    data = nfj + jj
    print(f"[ETL] total before dedup: {len(data)}")
    # dedup po id; jeśli brak id, dedup po (title,company)
    data = simple_dedup(data, key="id") or data
    seen = set()
    merged = []
    for d in data:
        k = (d["title"].lower(), d["company"].lower())
        if k in seen: 
            continue
        seen.add(k)
        merged.append(d)
    print(f"[ETL] total after dedup:  {len(merged)}")

    with engine.begin() as conn:
        conn.execute(delete(jobs_table))
        for row in merged:
            conn.execute(insert(jobs_table).values(**row))
    return len(merged)

def transform_clean(engine):
    with engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT id,title,company,location,description,source FROM jobs_raw"
        )).mappings().all()
        conn.execute(delete(jobs_clean))
        for r in rows:
            # skille z naszego prostego ekstraktora
            skills = extract_skills(r["description"] or "")
            # seniority: jeśli nie ma od źródła, spróbuj zgadnąć z tytułu/opisu
            guessed = infer_seniority(f"{r['title']} {r['description'] or ''}")
            conn.execute(insert(jobs_clean).values(
                id=r["id"], title=r["title"], company=r["company"],
                location=r["location"],
                skills=",".join(skills),
                seniority=guessed,
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
