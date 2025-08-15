# services/worker/etl/schema.py
from __future__ import annotations

from sqlalchemy import MetaData, Table, Column, String, Text

metadata = MetaData()

# surowe rekordy (RAW) – trzymamy to co przychodzi ze źródeł
jobs_table = Table(
    "jobs_table",
    metadata,
    Column("id", String, primary_key=False),  # unikalność wymusimy indeksem
    Column("title", String),
    Column("company", String),
    Column("location", String),
    Column("desc", Text),
    Column("source", String),
    Column("posted_at", String),
    Column("url", String),
    Column("seniority", String),  # to co podaje źródło albo nasz fallback
)

# oczyszczone rekordy (CLEAN) – gotowe do UI/API
jobs_clean = Table(
    "jobs_clean",
    metadata,
    Column("id", String, primary_key=False),
    Column("title", String),
    Column("company", String),
    Column("location", String),
    Column("desc", Text),
    Column("source", String),
    Column("posted_at", String),
    Column("url", String),
    Column("skills", String),
    Column("seniority", String),  # finalna klasyfikacja
)
