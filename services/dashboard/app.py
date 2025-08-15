import os, sqlite3
import pandas as pd, streamlit as st
from unicodedata import normalize

st.set_page_config(page_title="AI Job Finder", layout="wide")
st.markdown("<style>div.block-container{padding-top:1rem;}</style>", unsafe_allow_html=True)
st.title("AI Job Finder — NoFluffJobs")

DB_PATH = os.getenv("DB_PATH", "data/ai_jobs.db")

def no_accents(s: str) -> str:
    if not isinstance(s, str):
        return s
    nfkd = normalize("NFKD", s)
    return "".join(ch for ch in nfkd if ord(ch) < 128)

@st.cache_data(show_spinner=False, ttl=60)
def load_df():
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT title, company, location, seniority, url, posted_at "
        "FROM jobs_clean "
        "ORDER BY COALESCE(posted_at,'') DESC, rowid DESC",
        con,
    )
    con.close()
    for col in ["title", "company", "location"]:
        df[f"_{col}_na"] = df[col].map(no_accents).str.lower()
    return df

# ------- Formularz wyszukiwania (Enter uruchamia submit) -------
with st.form("search"):
    c1, c2, c3 = st.columns([2, 2, 1])
    ttl = c1.text_input("Tytuł (np. data, python, analityk)", "")
    loc = c2.text_input("Lokalizacja (np. Poznań / Poznan / Zdalnie)", "")
    sen = c3.text_input("Seniority (puste = wszystkie)", "")
    st.form_submit_button("Szukaj (Enter)")

# ------- Limit poza formularzem (działa natychmiast) -------
limit = st.selectbox("Limit wyników", [50, 100, 200, 500, 1000], index=2)

# ------- Filtrowanie + wyświetlanie -------
df = load_df()
base = len(df)

if ttl.strip():
    df = df[df["_title_na"].str.contains(no_accents(ttl).lower(), na=False)]
if loc.strip():
    df = df[df["_location_na"].str.contains(no_accents(loc).lower(), na=False)]
if sen.strip():
    df = df[df["seniority"].fillna("").str.lower().str.contains(sen.lower(), na=False)]

st.caption(f"{len(df)} ofert (z {base} w bazie)")

# Zastosuj limit TU — przed renderem
df = df.head(limit).copy()

if df.empty:
    st.info("Brak wyników dla podanych filtrów.")
else:
    rows_html = []
    for _, r in df.iterrows():
        title = (r["title"] or "").strip()
        url = (r["url"] or "").strip()
        company = (r["company"] or "").strip()
        location = (r["location"] or "").strip()
        seniority = (r["seniority"] or "").strip()
        posted = (r["posted_at"] or "").strip()
        rows_html.append(
            f'<div style="padding:6px 0;border-bottom:1px solid rgba(255,255,255,0.06)">'
            f'<a href="{url}" target="_blank" style="text-decoration:none;">{title}</a>'
            f' &nbsp;—&nbsp; <span style="opacity:.9">{company}</span>'
            f' &nbsp;•&nbsp; <span style="opacity:.8">{location}</span>'
            f' &nbsp;•&nbsp; <span style="opacity:.8">{seniority}</span>'
            f' &nbsp;•&nbsp; <span style="opacity:.6">{posted}</span>'
            f'</div>'
        )
    st.markdown("\n".join(rows_html), unsafe_allow_html=True)
