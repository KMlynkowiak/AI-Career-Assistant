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

# --- meta: unikalne wartości do selectów (cache'owane) ---
@st.cache_data(ttl=300)
def get_distinct(col: str, limit: int = 200):
    if col not in {"title", "location", "seniority"}:
        return []
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = lambda c, r: r[0]
            rows = conn.execute(
                f"SELECT DISTINCT {col} FROM jobs_clean WHERE {col} IS NOT NULL ORDER BY {col} LIMIT ?",
                (limit,),
            ).fetchall()
        # usuń puste/None i zwróć listę stringów
        return [str(x) for x in rows if x]
    except Exception:
        return []

def query_jobs(title_eq=None, loc_eq=None, sen_eq=None, limit: int = 500):
    """
    Filtry exact (bo wybieramy z listy):
      - title_eq -> dokładny tytuł
      - loc_eq   -> dokładna lokalizacja
      - sen_eq   -> dokładne seniority (Junior/Mid/Senior)
    Zwraca: title, seniority, location, company
    """
    sql = """
        SELECT title, seniority, location, company
        FROM jobs_clean
        WHERE 1=1
    """
    params = {}
    if title_eq:
        sql += " AND lower(title) = :t "
        params["t"] = title_eq.lower()
    if loc_eq:
        sql += " AND lower(location) = :l "
        params["l"] = loc_eq.lower()
    if sen_eq:
        sql += " AND lower(seniority) = :s "
        params["s"] = sen_eq.lower()

    sql += " ORDER BY rowid DESC LIMIT :limit "
    params["limit"] = int(limit)

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
    return rows

# — UI: formularz; nic nie pokazujemy dopóki nie klikniesz "Szukaj" —
st.title("🔎 Jobs – Minimal search")

titles = ["(Dowolna)"] + get_distinct("title")
locations = ["(Dowolna)"] + get_distinct("location")
seniorities = ["(Dowolna)", "Junior", "Mid", "Senior"]

with st.form("search"):
    c1, c2, c3 = st.columns(3)
    with c1:
        loc_choice = st.selectbox("Lokalizacja", locations, index=0)
    with c2:
        sen_choice = st.selectbox("Seniority", seniorities, index=0)
    with c3:
        title_choice = st.selectbox("Tytuł pracy", titles, index=0)
    submitted = st.form_submit_button("Szukaj")

st.caption(f"Baza: {DB_PATH}")

if not submitted:
    st.info("Wybierz z listy i kliknij **Szukaj**.")
else:
    loc_val = None if loc_choice == "(Dowolna)" else loc_choice
    sen_val = None if sen_choice == "(Dowolna)" else sen_choice
    title_val = None if title_choice == "(Dowolna)" else title_choice

    rows = query_jobs(title_eq=title_val, loc_eq=loc_val, sen_eq=sen_val)
    st.write(f"Wyniki: **{len(rows)}**")

    if not rows:
        st.warning("Brak rekordów. Spróbuj innego wyboru lub zostaw „(Dowolna)”.")
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
