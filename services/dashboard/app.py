# services/dashboard/app.py
import os
import sqlite3
import html
import unicodedata
import streamlit as st
import pandas as pd

DB_PATH = os.environ.get("DB_PATH", "data/ai_jobs.db")

st.set_page_config(page_title="AI Jobs Finder", page_icon="🔎", layout="wide")

# ===== Styl minimal =====
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

# ====== Formularz (ENTER submit) ======
with st.form("search_form", clear_on_submit=False):
    c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
    with c1:
        ttl = st.text_input("Tytuł (fragment)", value="", placeholder="np. data, ml, analytics …")
    with c2:
        sen = st.text_input("Seniority", value="", placeholder="Junior / Mid / Senior")
    with c3:
        loc = st.text_input("Lokalizacja", value="", placeholder="np. Warszawa / Zdalnie …")
    with c4:
        limit_txt = st.text_input("Limit wyników (puste = brak limitu)", value="")
    submitted = st.form_submit_button("Szukaj", use_container_width=True)

# ====== Akcenty/diakrytyki: funkcja do porównań ======
def noacc(s: str) -> str:
    if s is None:
        return ""
    # usuwamy znaki diakrytyczne i zamieniamy na lower
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c)).lower()

def run_query(q: str, s: str, l: str, limit_txt: str) -> pd.DataFrame:
    where = []
    params = {}
    if q:
        where.append("NOACC(title) LIKE NOACC(:q)")
        params["q"] = f"%{q}%"
    if s:
        where.append("NOACC(seniority) = NOACC(:s)")
        params["s"] = s
    if l:
        where.append("NOACC(location) LIKE NOACC(:l)")
        params["l"] = f"%{l}%"

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
        # rejestrujemy funkcję NOACC w SQLite (akcent-insensitive match)
        con.create_function("NOACC", 1, noacc)
        df = pd.read_sql_query(sql, con, params=params)
    finally:
        con.close()
    return df

def render_html_table(df: pd.DataFrame) -> str:
    titles = []
    for t, u in zip(df["title"], df["url"]):
        safe_t = html.escape(t or "")
        if isinstance(u, str) and u.strip():
            titles.append(f'<a class="job" href="{html.escape(u)}" target="_blank" rel="noopener">{safe_t}</a>')
        else:
            titles.append(safe_t)

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

    return (
        "<table class='jobs'>"
        "<thead><tr>"
        "<th>Tytuł</th><th>Seniority</th><th>Lokalizacja</th><th>Firma</th>"
        "</tr></thead><tbody>" + "".join(rows_html) + "</tbody></table>"
    )

if submitted:
    df = run_query(ttl, sen, loc, limit_txt)
    st.caption(f"Wyników: {len(df):,}")
    if df.empty:
        st.info("Brak dopasowań. Zmień filtr lub usuń część kryteriów.")
    else:
        st.markdown(render_html_table(df), unsafe_allow_html=True)
else:
    st.info("Uzupełnij filtry i kliknij **Szukaj** lub naciśnij **Enter**. (Puste „Limit wyników” = brak limitu)")
