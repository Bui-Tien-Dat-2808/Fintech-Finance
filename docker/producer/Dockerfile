# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /opt/project

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "/opt/project/scripts/run_producer.py"]
