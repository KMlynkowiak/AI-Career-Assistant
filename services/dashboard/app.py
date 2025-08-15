import os, sqlite3
import pandas as pd, streamlit as st
from unicodedata import normalize

st.set_page_config(page_title="AI Job Finder", layout="wide")
st.markdown("<style>div.block-container{padding-top:1rem;}</style>", unsafe_allow_html=True)
st.title("AI Job Finder — NoFluffJobs")

DB_PATH = os.getenv("DB_PATH", "data/ai_jobs.db")
BOX_H = int(os.getenv("UI_BOX_HEIGHT", "560"))  # wysokość scrollowanego boksu (px)

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

# ------- Formularz (Enter uruchamia submit) -------
with st.form("search"):
    c1, c2, c3 = st.columns([2, 2, 1])
    ttl = c1.text_input("Tytuł (np. data, python, analityk)", "")
    loc = c2.text_input("Lokalizacja (np. Poznań / Poznan / Zdalnie)", "")
    sen = c3.text_input("Seniority (puste = wszystkie)", "")
    st.form_submit_button("Szukaj (Enter)")

# ------- Limit poza formularzem (działa natychmiast) -------
limit = st.selectbox("Limit wyników", [50, 100, 200, 500, 1000], index=2)

# ------- Filtrowanie -------
df = load_df()
base_total = len(df)

if ttl.strip():
    df = df[df["_title_na"].str.contains(no_accents(ttl).lower(), na=False)]
if loc.strip():
    df = df[df["_location_na"].str.contains(no_accents(loc).lower(), na=False)]
if sen.strip():
    df = df[df["seniority"].fillna("").str.lower().str.contains(sen.lower(), na=False)]

filtered_total = len(df)
displayed_df = df.head(limit).copy()
showing = len(displayed_df)

st.caption(f"Pokazuję {showing} z {filtered_total} wyników (w bazie: {base_total})")

# ------- Render „tabeli” w scrollowanym boksie -------
if displayed_df.empty:
    st.info("Brak wyników dla podanych filtrów.")
else:
    # zbuduj wiersze HTML (tytuł jako link)
    rows_html = []
    # Nagłówek tabeli (sticky)
    header_html = (
        '<div class="result-row header">'
        '<div>Tytuł</div><div>Firma</div><div>Lokalizacja</div><div>Seniority</div><div>Data</div>'
        "</div>"
    )
    for _, r in displayed_df.iterrows():
        title = (r["title"] or "").strip()
        url = (r["url"] or "").strip()
        company = (r["company"] or "").strip()
        location = (r["location"] or "").strip()
        seniority = (r["seniority"] or "").strip()
        posted = (r["posted_at"] or "").strip()
        rows_html.append(
            '<div class="result-row">'
            f'<div><a href="{url}" target="_blank" style="text-decoration:none;">{title}</a></div>'
            f'<div>{company}</div>'
            f'<div>{location}</div>'
            f'<div>{seniority}</div>'
            f'<div>{posted}</div>'
            "</div>"
        )

    st.markdown(
        f"""
        <div class="results-box">
          {header_html}
          {''.join(rows_html)}
        </div>

        <style>
          .results-box {{
            max-height: {BOX_H}px;
            overflow-y: auto;
            border: 1px solid rgba(255,255,255,0.12);
            border-radius: 10px;
            padding: 6px 10px;
            background: rgba(255,255,255,0.02);
          }}
          .results-box::-webkit-scrollbar {{ width: 10px; }}
          .results-box::-webkit-scrollbar-thumb {{
            background: rgba(255,255,255,0.15);
            border-radius: 10px;
          }}
          .result-row {{
            display: grid;
            grid-template-columns: 4fr 2fr 2fr 1fr 1fr;
            gap: 12px;
            align-items: center;
            padding: 8px 2px;
            border-bottom: 1px solid rgba(255,255,255,0.06);
            font-size: 0.96rem;
            line-height: 1.2rem;
          }}
          .result-row:last-child {{ border-bottom: none; }}
          .result-row.header {{
            position: sticky; top: 0;
            background: rgba(0,0,0,0.45);
            backdrop-filter: blur(3px);
            font-weight: 600;
            border-bottom: 1px solid rgba(255,255,255,0.25);
            z-index: 5;
          }}
          .result-row a {{ color: inherit; }}
        </style>
        """,
        unsafe_allow_html=True,
    )
