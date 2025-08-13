# services/dashboard/app.py
import os, html, sqlite3
import streamlit as st

# — Minimalny wygląd —
st.set_page_config(page_title="Jobs – Minimal", layout="centered")
st.markdown(
    '''
    <style>
      #MainMenu {visibility: hidden;} footer {visibility: hidden;}
      .block-container {padding-top: 2rem; padding-bottom: 2rem; max-width: 900px;}
      table.mini {width: 100%; border-collapse: collapse; font-size: 0.95rem;}
      table.mini th, table.mini td {padding: 8px 10px; border-bottom: 1px solid #2a2a2a;}
      table.mini th {text-align: left; font-weight: 600;}
    </style>
    ''',
    unsafe_allow_html=True,
)

DB_PATH = os.getenv("DB_PATH", "data/ai_jobs.db")

def _norm(s: str | None) -> str | None:
    if not s: return None
    s = s.strip()
    return s if s else None

def _norm_seniority(s: str | None) -> str | None:
    s = _norm(s)
    if not s: return None
    low = s.lower()
    if low.startswith("jun"): return "Junior"
    if low.startswith("mid") or low == "m": return "Mid"
    if low.startswith("sen"): return "Senior"
    return s  # inne wartości traktujemy jako exact

def _where_and_params(q=None, loc=None, sen=None):
    sql = " WHERE 1=1 "
    params = {}
    if q := _norm(q):
        sql += " AND lower(title) LIKE :q "
        params["q"] = f"%{q.lower()}%"
    if loc := _norm(loc):
        sql += " AND lower(location) LIKE :loc "
        params["loc"] = f"%{loc.lower()}%"
    if sen := _norm_seniority(sen):
        sql += " AND lower(seniority) = :sen "
        params["sen"] = sen.lower()
    return sql, params

def count_jobs(q=None, loc=None, sen=None) -> int:
    where_sql, params = _where_and_params(q, loc, sen)
    sql = "SELECT COUNT(*) FROM jobs_clean" + where_sql
    with sqlite3.connect(DB_PATH) as conn:
        return int(conn.execute(sql, params).fetchone()[0])

def query_jobs(q=None, loc=None, sen=None, limit: int = 200):
    where_sql, params = _where_and_params(q, loc, sen)
    sql = (
        "SELECT title, seniority, location, company "
        "FROM jobs_clean " + where_sql +
        " ORDER BY rowid DESC LIMIT :limit "
    )
    params["limit"] = int(limit)
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
    return rows

# — UI: formularz; wyniki dopiero po "Szukaj" —
st.title("🔎 Jobs – Minimal search")

with st.form("search"):
    c1, c2, c3 = st.columns(3)
    with c1:
        loc_in = st.text_input("Lokalizacja", placeholder="np. warsz / wro / zdalnie")
    with c2:
        sen_in = st.text_input("Seniority", placeholder="Junior / Mid / Senior (puste = wszystkie)")
    with c3:
        ttl_in = st.text_input("Tytuł pracy", placeholder="np. Data (puste = wszystkie)")
    limit_in = st.number_input("Limit wyników", min_value=10, max_value=5000, value=200, step=10)
    submitted = st.form_submit_button("Szukaj")

st.caption(f"Baza: {DB_PATH}")

if not submitted:
    st.info("Wpisz filtry (lub zostaw puste), ustaw limit i kliknij **Szukaj**.")
else:
    total = count_jobs(q=ttl_in, loc=loc_in, sen=sen_in)
    rows = query_jobs(q=ttl_in, loc=loc_in, sen=sen_in, limit=int(limit_in))
    st.write(f"Wyniki: **{total}**  •  pokazuję: **{len(rows)}** (limit = {int(limit_in)})")
    if not rows:
        st.warning("Brak rekordów. Spróbuj krótszego fragmentu (np. 'data', 'wro').")
    else:
        header = ["Tytuł", "Seniority", "Lokalizacja", "Firma"]
        body = "".join(
            f"<tr><td>{html.escape(r['title'] or '')}</td>"
            f"<td>{html.escape(r['seniority'] or '')}</td>"
            f"<td>{html.escape(r['location'] or '')}</td>"
            f"<td>{html.escape(r['company'] or '')}</td></tr>"
            for r in rows
        )
        table = (
            "<table class='mini'><thead><tr>"
            + "".join(f"<th>{h}</th>" for h in header)
            + "</tr></thead><tbody>"
            + body
            + "</tbody></table>"
        )
        st.markdown(table, unsafe_allow_html=True)
