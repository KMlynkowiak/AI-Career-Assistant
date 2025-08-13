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

def query_jobs(q=None, loc=None, sen=None, limit=300):
    """
    Szukanie:
      - q   -> tytuł (LIKE %fragment%, case-insensitive)
      - loc -> lokalizacja (LIKE %fragment%, case-insensitive)
      - sen -> exact 'junior/mid/senior' (case-insensitive)
    Zwraca: title, seniority, location, company
    """
    sql = """
        SELECT title, seniority, location, company
        FROM jobs_clean
        WHERE 1=1
    """
    params = {}

    if q:
        sql += " AND lower(title) LIKE :q "
        params["q"] = f"%{q.lower().strip()}%"

    if loc:
        sql += " AND lower(location) LIKE :loc "
        params["loc"] = f"%{loc.lower().strip()}%"

    if sen:
        sql += " AND lower(seniority) = :sen "
        params["sen"] = sen.lower().strip()

    # Brak wymagania na kolumnę posted_at — wspiera istniejącą bazę
    sql += " ORDER BY rowid DESC LIMIT :limit "
    params["limit"] = int(limit)

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
    return rows

# — UI: formularz (nic nie pokazujemy, dopóki nie klikniesz 'Szukaj') —
st.title("🔎 Jobs – Minimal search")

with st.form("search"):
    c1, c2, c3 = st.columns(3)
    with c1:
        loc_in = st.text_input("Lokalizacja", placeholder="np. warsz / wro / zdalnie")
    with c2:
        sen_in = st.text_input("Seniority", placeholder="Junior / Mid / Senior")
    with c3:
        ttl_in = st.text_input("Tytuł pracy", placeholder="np. Data")
    submitted = st.form_submit_button("Szukaj")

st.caption(f"Baza: {DB_PATH}")

if not submitted:
    st.info("Uzupełnij dowolne pole i kliknij **Szukaj** – wtedy pokażę wyniki.")
else:
    rows = query_jobs(q=ttl_in or None, loc=loc_in or None, sen=sen_in or None)
    st.write(f"Wyniki: **{len(rows)}**")
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
