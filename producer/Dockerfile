FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY sla ./sla
COPY producer ./producer
COPY docs/contrato_evento.json ./docs/contrato_evento.json

CMD ["python", "-m", "producer.producer", "--count", "0", "--interval", "1.0"]
