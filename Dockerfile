FROM python:3.11.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl git && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY . /app
ENV SPARK_LOCAL_IP=127.0.0.1 DBT_PROFILES_DIR=/app/dbt PYTHONPATH=/app/kestra/scripts
