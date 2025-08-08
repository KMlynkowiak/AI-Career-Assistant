FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV DB_PATH=/app/data/ai_jobs.db
EXPOSE 8000
CMD ["python","app.py"]
