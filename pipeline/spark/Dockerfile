# Imagen de Spark Structured Streaming. Esta
# imagen NO forma parte del `docker compose up -d` por defecto: se activa con el
# overlay docker-compose.spark.yml y sirve para demostrar que el job esta listo
# para contenedorizar

FROM eclipse-temurin:21-jre-jammy AS jre

FROM python:3.11-slim

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"
COPY --from=jre /opt/java/openjdk /opt/java/openjdk

# procps: Spark invoca `ps` en algunos scripts de arranque.
RUN apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    pyspark==4.2.0 \
    pandas==2.3.3 \
    pyarrow==22.0.0 \
    psycopg2-binary==2.9.9 \
    python-dotenv==1.0.1 \
    requests==2.32.3 \
    "confluent-kafka[avro]==2.15.0"

COPY common/ ./common/
COPY spark/stream_processing.py spark/database_writers.py spark/monitoring.py ./spark/

# Pre-resolver los conectores JVM (Kafka, Avro, JDBC PostgreSQL) de Maven en
# tiempo de build, para que el primer arranque no dependa de la red. Quedan en
# la cache de Ivy dentro de la imagen.
RUN python -c "from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]') \
.config('spark.jars.packages', \
'org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.spark:spark-avro_2.13:4.2.0,org.postgresql:postgresql:42.7.13') \
.getOrCreate().stop()"

WORKDIR /app/spark
ENTRYPOINT ["python", "stream_processing.py"]
