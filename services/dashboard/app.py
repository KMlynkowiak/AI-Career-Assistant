# services/dashboard/app.py
import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# --- Ustawienia strony i drobny cleanup UI ---
st.set_page_config(page_title="Jobs – Minimal", layout="centered")
st.markdown(
    """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .block-container {padding-top: 2rem; padding-bottom: 2rem;}
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Baza ---
DB_PATH = os.getenv("DB_PATH", "data/ai_jobs.db")
engine = create_engine(f"sqlite:///{DB_PATH}", future=True)

def search_jobs(q: str | None, location: str | None, seniority: str | None, limit: int = 300) -> pd.DataFrame:
    """
    Proste wyszukiwanie:
    - q          -> tytuł (contains, case-insensitive)
    - location   -> lokalizacja (contains, case-insensitive)
    - seniority  -> exact (Junior/Mid/Senior, case-insensitive; puste = brak filtra)
    Zwraca 4 kolumny: title, seniority, location, company
    """
    sql = """
        SELECT title, seniority, location, company
        FROM jobs_clean
        WHERE 1=1
    """
    params: dict[str, str | int] = {}

    if q:
        sql += " AND lower(title) LIKE :q "
        params["q"] = f"%{q.lower().strip()}%"

    if location:
        sql += " AND lower(location) LIKE :loc "
        params["loc"] = f"%{location.lower().strip()}%"

    if seniority:
        sql += " AND lower(seniority) = :sen "
        params["sen"] = seniority.lower().strip()

    sql += " ORDER BY rowid DESC LIMIT :limit "
    params["limit"] = int(limit)

    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    df = pd.DataFrame(rows, columns=["title", "seniority", "location", "company"])
    return df


# --- UI (minimal) ---
st.title("🔎 Jobs – Minimal search")

col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    location_in = st.text_input("Lokalizacja", placeholder="np. Warszawa / wro / zdalnie", help="Filtrowanie zawiera, bez rozróżniania wielkości liter.")
with col2:
    seniority_in = st.text_input("Seniority", placeholder="Junior / Mid / Senior", help="Wpisz dokładnie Junior, Mid lub Senior (bez rozróżniania wielkości liter).")
with col3:
    title_in = st.text_input("Tytuł pracy", placeholder="np. Data", help="Np. Data → znajdzie Data Engineer / Data Scientist itp.")

# Wyniki
df = search_jobs(q=title_in or None, location=location_in or None, seniority=seniority_in or None)

st.caption(f"Baza: {DB_PATH}")
st.write(f"Wyniki: **{len(df)}**")

if df.empty:
    st.info("Brak rekordów dla podanych filtrów. Spróbuj zostawić pola puste lub wpisz krótszy fragment (np. 'wro', 'data').")
else:
    # schludne nagłówki po PL
    df = df.rename(columns={"title": "Tytuł", "seniority": "Seniority", "location": "Lokalizacja", "company": "Firma"})
    st.dataframe(df, use_container_width=True, hide_index=True)
