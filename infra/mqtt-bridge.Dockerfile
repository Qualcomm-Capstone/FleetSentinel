FROM python:3.12-slim
WORKDIR /app
COPY infra/mqtt-bridge.requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY schemas /app/schemas
COPY exploration/fleetsentinel_ingest /app/exploration/fleetsentinel_ingest
COPY exploration/scripts/mqtt_ingest.py /app/exploration/scripts/mqtt_ingest.py
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 10001:10001
ENTRYPOINT ["python", "/app/exploration/scripts/mqtt_ingest.py"]
