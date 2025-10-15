FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY services/rest/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt

# copy the whole package so "services.rest.main" is importable
COPY services /app/services

ENV PORT=8080
EXPOSE 8080
CMD ["bash", "-lc", "uvicorn services.rest.main:app --host 0.0.0.0 --port ${PORT}"]
