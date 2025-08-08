from sqlalchemy import Table, Column, String, MetaData, Text

metadata = MetaData()

jobs_table = Table(
    "jobs_raw",
    metadata,
    Column("id", String, primary_key=True),
    Column("title", String),
    Column("company", String),
    Column("location", String),
    Column("description", Text),
    Column("source", String),
)

jobs_clean = Table(
    "jobs_clean",
    metadata,
    Column("id", String, primary_key=True),
    Column("title", String),
    Column("company", String),
    Column("location", String),
    Column("skills", String),  # comma-separated
    Column("seniority", String),
    Column("tech_stack", String),  # comma-separated normalized
    Column("source", String),
)
