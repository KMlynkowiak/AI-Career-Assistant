import os
import pandas as pd
import altair as alt
import streamlit as st
from sqlalchemy import create_engine

DB_PATH = os.getenv("DB_PATH","data/ai_jobs.db")
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)

st.set_page_config(page_title="AI Career Assistant", layout="wide")

@st.cache_data
def load_jobs():
    return pd.read_sql("SELECT * FROM jobs_clean", engine)

df = load_jobs()
st.title("AI Career & Job Market Assistant (demo)")

with st.sidebar:
    st.markdown("## Filtry")
    loc = st.selectbox("Lokalizacja", ["(wszystkie)"] + sorted(df["location"].dropna().unique().tolist()))
    sen = st.selectbox("Poziom", ["(wszystkie)"] + sorted(df["seniority"].dropna().unique().tolist()))
    q = st.text_input("Szukaj (tytuł/skill)", "")

mask = pd.Series([True]*len(df))
if loc != "(wszystkie)":
    mask &= (df["location"]==loc)
if sen != "(wszystkie)":
    mask &= (df["seniority"]==sen)
if q.strip():
    mask &= df["title"].str.contains(q, case=False) | df["skills"].str.contains(q, case=False)

view = df[mask]

st.subheader("Oferty (demo)")
st.dataframe(view[["id","title","company","location","seniority","skills"]], use_container_width=True)

st.subheader("Top umiejętności")
from collections import Counter
c = Counter()
for s in view["skills"].dropna():
    for sk in s.split(","):
        if sk.strip():
            c[sk.strip()] += 1

skills_df = pd.DataFrame([{"skill":k,"count":v} for k,v in c.items()]).sort_values("count", ascending=False)
if not skills_df.empty:
    chart = alt.Chart(skills_df.head(15)).mark_bar().encode(
        x="count:Q",
        y=alt.Y("skill:N", sort='-x')
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("Brak danych do wyświetlenia. Uruchom ETL.")

st.caption(f"Baza: {DB_PATH}")
