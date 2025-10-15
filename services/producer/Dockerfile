# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the producer code (includes util, wsclient, kafkaio, local schema + replay data)
COPY . /app

# Optional: a simple health probe could be added later via docker-compose
# HEALTHCHECK CMD python -c "import socket; exit(0)"

# Run the producer; mode is controlled by env PRODUCER_MODE=live|replay (default live).
CMD ["python", "app.py"]
