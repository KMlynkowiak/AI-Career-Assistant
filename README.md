# AI Career & Job Market Assistant

End-to-end projekt łączący **Data Engineering, NLP i rekomendacje**.
Zbiera oferty pracy, czyści dane, ekstrahuje umiejętności, liczy trendy i wystawia **API** + **Dashboard**.

## Szybki start (bez Dockera)
```bash
make venv
make dev
make etl          # ETL demo (3 oferty)
make api          # http://localhost:8000/docs
make dashboard    # http://localhost:8501
```

## Struktura
- `services/worker/etl` – pobieranie (demo), czyszczenie, transformacja → SQLite
- `services/api` – FastAPI (lista ofert, trendy umiejętności)
- `services/dashboard` – Streamlit (tabela i top skille)
- `tests/` – przykładowy test deduplikacji
- `docker-compose.yml` – worker + API + dashboard

## TODO (Backlog)
- [ ] Wpiąć prawdziwe źródła (API/scraping) i walidacje (Great Expectations)
- [ ] Embeddingi + dopasowanie CV→oferty (pgvector/FAISS)
- [ ] Harmonogram (Prefect/Airflow) + monitoring
- [ ] Tygodniowy raport (HTML/PDF)
