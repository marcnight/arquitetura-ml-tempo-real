# Imagem única usada pelo mestre, pelos workers, pelo driver do streaming e
# pela ponte de métricas. Python e bibliotecas iguais aos do host, pois o
# PySpark exige a mesma versão menor de Python no driver e nos executores.
FROM python:3.14-slim-trixie

ARG UID=1000
ARG GID=1000

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt

ENV SPARK_HOME=/usr/local/lib/python3.14/site-packages/pyspark \
    SPARK_CONF_DIR=/opt/spark-conf \
    PYSPARK_PYTHON=python \
    PYTHONUNBUFFERED=1
ENV PATH="${SPARK_HOME}/bin:${PATH}"

COPY metrics.properties /opt/spark-conf/metrics.properties

# O Hadoop exige que o UID do processo exista em /etc/passwd.
RUN groupadd -g ${GID} app && useradd -m -u ${UID} -g ${GID} app \
    && mkdir -p /app/checkpoint /app/medicoes/raw && chown -R app:app /app
USER app
WORKDIR /app

# Baixa o conector Kafka (mesma versão do PySpark) para o cache Ivy da imagem,
# evitando depender do Maven Central a cada inicialização.
RUN python -c "import pyspark; from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]') \
.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:' + pyspark.__version__) \
.getOrCreate().stop()"
