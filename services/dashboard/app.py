import os
import pandas as pd
import altair as alt
import streamlit as st
from sqlalchemy import create_engine

DB_PATH = os.getenv("DB_PATH", "data/ai_jobs.db")
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)

st.set_page_config(page_title="AI Career Assistant", layout="wide")

@st.cache_data
def load_jobs():
    try:
        return pd.read_sql("SELECT * FROM jobs_clean", engine)
    except Exception:
        return pd.DataFrame()

df = load_jobs()

st.title("AI Career & Job Market – Dashboard")
st.caption(f"Baza: {DB_PATH}")

# Szybkie metryki
st.subheader("Szybkie metryki")
c1, c2, c3 = st.columns(3)
c1.metric("Liczba ogłoszeń", len(df))
c2.metric("Liczba firm", int(df["company"].nunique()) if "company" in df.columns and not df.empty else 0)
c3.metric("Źródła", int(df["source"].nunique()) if "source" in df.columns and not df.empty else 0)

st.divider()

# Filtry
st.subheader("Filtry")
colf1, colf2, colf3 = st.columns([1,1,2])
sources = sorted(df["source"].dropna().unique().tolist()) if "source" in df.columns else []
source_pick = colf1.multiselect("Źródło", sources, default=sources)
cities = sorted(df["location"].dropna().unique().tolist()) if "location" in df.columns else []
city_pick = colf2.multiselect("Miasto", cities, default=cities)
q = colf3.text_input("Szukaj w tytule/opisie", "")

df_f = df.copy()
if source_pick:
    df_f = df_f[df_f["source"].isin(source_pick)]
if city_pick:
    df_f = df_f[df_f["location"].isin(city_pick)]
if q.strip():
    ql = q.lower()
    def _m(s):
        s = str(s or "")
        return ql in s.lower()
    df_f = df_f[df_f["title"].apply(_m) | df_f["desc"].apply(_m)]

st.write(f"Wynik po filtrach: **{len(df_f)}** rekordów")
st.dataframe(df_f.head(50), use_container_width=True)

st.subheader("Top umiejętności")
if not df_f.empty and "skills" in df_f.columns:
    from collections import Counter
    c = Counter()
    for s in df_f["skills"].dropna():
        for sk in s.split(","):
            sk = sk.strip()
            if sk:
                c[sk] += 1
    skills_df = pd.DataFrame([{"skill": k, "count": v} for k, v in c.items()]).sort_values("count", ascending=False)
    if not skills_df.empty:
        chart = alt.Chart(skills_df.head(20)).mark_bar().encode(
            x="count:Q",
            y=alt.Y("skill:N", sort='-x'),
            tooltip=["skill", "count"]
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Brak danych o umiejętnościach.")
else:
    st.info("Brak danych o umiejętnościach (uruchom ETL).")
