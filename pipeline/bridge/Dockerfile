# Imagen del microservicio bridge (MQTT -> Kafka).
#
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    paho-mqtt==2.1.0 \
    confluent-kafka[avro]==2.15.0 \
    kafka-python==2.0.2 \
    python-dotenv==1.0.1

COPY common/ ./common/
COPY bridge/mqtt_kafka_bridge.py ./bridge/mqtt_kafka_bridge.py
COPY schemas/ ./schemas/

WORKDIR /app/bridge
ENTRYPOINT ["python", "mqtt_kafka_bridge.py"]
