# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app

# Config via env (with sensible defaults)
ENV CASSANDRA_CONTACT_POINTS=cassandra \
    CASSANDRA_PORT=9042 \
    CASSANDRA_KEYSPACE=market_ks \
    API_HOST=0.0.0.0 \
    API_PORT=8000

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
