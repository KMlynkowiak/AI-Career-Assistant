# services/worker/etl/schema.py
from __future__ import annotations
from sqlalchemy import MetaData, Table, Column, String

metadata = MetaData()

jobs_table = Table(
    "jobs_table", metadata,
    Column("id", String),
    Column("title", String),
    Column("company", String),
    Column("location", String),
    Column("seniority", String),
    Column("url", String),
    Column("posted_at", String),
    Column("source", String),
)

jobs_clean = Table(
    "jobs_clean", metadata,
    Column("id", String),
    Column("title", String),
    Column("company", String),
    Column("location", String),
    Column("seniority", String),
    Column("url", String),
    Column("posted_at", String),
    Column("source", String),
)
