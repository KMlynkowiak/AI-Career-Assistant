# services/dashboard/app.py
import os
import sqlite3
import html
import streamlit as st
import pandas as pd

DB_PATH = os.environ.get("DB_PATH", "data/ai_jobs.db")

st.set_page_config(page_title="AI Jobs Finder", page_icon="🔎", layout="wide")

# Minimalistyczny, ciemny styl
st.markdown(
    """
    <style>
      :root { --bg:#0f1115; --panel:#141822; --text:#e8e8e8; --muted:#a8b0c0; --border:#243048; }
      .stApp { background: var(--bg); color: var(--text); }
      .block-container { padding-top: 2rem; }
      .stTextInput>div>div>input, .stTextInput>div>div>textarea {
        background: var(--panel); color: var(--text); border: 1px solid var(--border);
      }
      .stButton>button {
        background:#1c2230; color:var(--text); border:1px solid var(--border);
      }
      table.jobs {
        border-collapse: collapse; width: 100%;
        background: var(--panel); color: var(--text);
        border: 1px solid var(--border);
        font-size: 0.95rem;
      }
      table.jobs th, table.jobs td {
        border-bottom: 1px solid var(--border);
        padding: 10px 12px;
      }
      table.jobs th {
        text-align: left; color: var(--muted); font-weight: 600; background: #161b27;
      }
      table.jobs tr:hover td { background: #182033; }
      a.job {
        color: #9ecbff; text-decoration: none;
      }
      a.job:hover { text-decoration: underline; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🔎 AI / Data Jobs — minimal")

with st.container():
    c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
    with c1:
        ttl = st.text_input("Tytuł (fragment)", value="", placeholder="np. data, ml, analytics …")
    with c2:
        sen = st.text_input("Seniority", value="", placeholder="Junior / Mid / Senior")
    with c3:
        loc = st.text_input("Lokalizacja", value="", placeholder="np. Warszawa / Zdalnie …")
    with c4:
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

def render_html_table(df: pd.DataFrame) -> str:
    # Zbuduj kolumnę „Tytuł” jako klikalny link
    titles = []
    for t, u in zip(df["title"], df["url"]):
        safe_t = html.escape(t or "")
        if isinstance(u, str) and u.strip():
            titles.append(f'<a class="job" href="{html.escape(u)}" target="_blank" rel="noopener">{safe_t}</a>')
        else:
            titles.append(safe_t)

    # Złóż minimalną tabelę HTML (bez indeksów i bez kolumny url)
    rows_html = []
    for i in range(len(df)):
        rows_html.append(
            "<tr>"
            f"<td>{titles[i]}</td>"
            f"<td>{html.escape(str(df.iloc[i]['seniority'] or ''))}</td>"
            f"<td>{html.escape(str(df.iloc[i]['location'] or ''))}</td>"
            f"<td>{html.escape(str(df.iloc[i]['company'] or ''))}</td>"
            "</tr>"
        )

    table = (
        "<table class='jobs'>"
        "<thead><tr>"
        "<th>Tytuł</th><th>Seniority</th><th>Lokalizacja</th><th>Firma</th>"
        "</tr></thead>"
        "<tbody>"
        + "".join(rows_html) +
        "</tbody></table>"
    )
    return table

if btn:
    df = run_query(ttl, sen, loc, limit_txt)
    st.caption(f"Wyników: {len(df):,}")
    if df.empty:
        st.info("Brak dopasowań. Zmień filtr lub usuń część kryteriów.")
    else:
        html_table = render_html_table(df)
        st.markdown(html_table, unsafe_allow_html=True)
else:
    st.info("Uzupełnij filtry i kliknij **Szukaj**. (Puste „Limit wyników” = brak limitu)")
