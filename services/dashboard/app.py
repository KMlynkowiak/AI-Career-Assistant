# services/dashboard/app.py
import os
import sqlite3
import streamlit as st
import pandas as pd

DB_PATH = os.environ.get("DB_PATH", "data/ai_jobs.db")

st.set_page_config(page_title="AI Jobs Finder", page_icon="🔎", layout="wide")

st.markdown(
    """
    <style>
    body, .stApp { background: #0f1115; color: #e8e8e8; }
    .css-ffhzg2, .stTextInput>div>div>input { background: #161a22; color: #e8e8e8; }
    .stButton>button { background: #222938; color:#e8e8e8; border:1px solid #2a3142; }
    .stDataFrame { border: 1px solid #2a3142; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🔎 AI / Data Jobs (minimal)")

with st.container():
    col1, col2, col3, col4 = st.columns([2,1,1,1])
    with col1:
        ttl = st.text_input("Tytuł (fragment)", value="", placeholder="np. data, ml, analytics …")
    with col2:
        sen = st.text_input("Seniority", value="", placeholder="Junior / Mid / Senior")
    with col3:
        loc = st.text_input("Lokalizacja", value="", placeholder="np. Warszawa / Zdalnie …")
    with col4:
        limit_txt = st.text_input("Limit wyników (puste = brak limitu)", value="")

    btn = st.button("Szukaj", type="primary", use_container_width=True)

def run_query(q: str, s: str, l: str, limit_txt: str) -> pd.DataFrame:
    where = []
    params = {}
    if q:
        where.append("LOWER(title) LIKE :q")
        params["q"] = f"%{q.lower()}%"
    if s:
        where.append("LOWER(seniority) = :s")
        params["s"] = s.lower()
    if l:
        where.append("LOWER(location) LIKE :l")
        params["l"] = f"%{l.lower()}%"

    wh = ("WHERE " + " AND ".join(where)) if where else ""
    lim = ""
    if limit_txt.strip():
        try:
            limv = int(limit_txt.strip())
            if limv > 0:
                lim = f"LIMIT {limv}"
        except:
            pass

    sql = f"""
        SELECT title, seniority, location, company, url
        FROM jobs_clean
        {wh}
        ORDER BY COALESCE(posted_at, '') DESC, rowid DESC
        {lim}
    """
    con = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(sql, con, params=params)
    finally:
        con.close()
    return df

if btn:
    df = run_query(ttl, sen, loc, limit_txt)
    st.caption(f"Wyników: {len(df):,}")
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("Uzupełnij filtry i kliknij **Szukaj**. (Puste „Limit wyników” = brak limitu)")

