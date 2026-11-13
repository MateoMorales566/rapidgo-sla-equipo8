FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt ./
# Instala solo el grupo base: Airflow y PySpark no se usan en este contenedor.
RUN grep -vE '^(apache-airflow|pyspark)' requirements.txt > /tmp/requirements-runtime.txt \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements-runtime.txt

COPY sla ./sla
COPY producer ./producer
COPY docs/contrato_evento.json ./docs/contrato_evento.json

CMD ["python", "-m", "producer.producer", "--count", "0", "--interval", "1.0"]
