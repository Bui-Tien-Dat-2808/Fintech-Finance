FROM apache/airflow:2.10.0-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io docker-compose-plugin openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
