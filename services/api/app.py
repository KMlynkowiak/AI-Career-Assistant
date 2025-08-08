import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, text
import pandas as pd

DB_PATH = os.getenv("DB_PATH", "data/ai_jobs.db")
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)

app = FastAPI(title="AI Career Assistant API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class JobOut(BaseModel):
    id: str
    title: str
    company: str
    location: str
    skills: str
    seniority: str
    source: str

@app.get("/jobs", response_model=list[JobOut])
def list_jobs(q: str | None = None, location: str | None = None, seniority: str | None = None, limit: int = 50):
    where = []
    params = {}
    if q:
        where.append("(title LIKE :q OR skills LIKE :q)")
        params["q"] = f"%{q}%"
    if location:
        where.append("location = :loc")
        params["loc"] = location
    if seniority:
        where.append("seniority = :sen")
        params["sen"] = seniority
    sql = "SELECT id,title,company,location,skills,seniority,source FROM jobs_clean"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " LIMIT :lim"
    params["lim"] = limit
    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [JobOut(**dict(r)) for r in rows]

@app.get("/skills/trending")
def trending_skills(top: int = 10):
    df = pd.read_sql("SELECT skills FROM jobs_clean", engine)
    from collections import Counter
    c = Counter()
    for s in df["skills"].dropna():
        for sk in s.split(","):
            if sk.strip():
                c[sk.strip()] += 1
    return [{"skill": k, "count": v} for k, v in c.most_common(top)]

if __name__ == "__main__":
    import uvicorn
    host = os.getenv("APP_HOST","0.0.0.0")
    port = int(os.getenv("APP_PORT","8000"))
    uvicorn.run(app, host=host, port=port)
