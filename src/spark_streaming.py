import sys
import os
import json
import time
import threading
import joblib
import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config

cfg = Config()

try:
    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import to_json, struct, col, from_json
    from pyspark.sql.types import (
        StructType, StructField, DoubleType, IntegerType, StringType, LongType
    )
except ImportError:
    print("PySpark não instalado. Instale com: pip install pyspark")
    sys.exit(1)


MODEL_PATH = os.path.join(cfg.models_dir, "best_model.pkl")
SCALER_PATH = os.path.join(cfg.models_dir, "scaler.pkl")

# Conector Kafka na mesma versão do PySpark instalado (Scala 2.13 no Spark 4.x).
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "checkpoint_output")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
ENABLE_CONSOLE_SINK = os.getenv("ENABLE_CONSOLE_SINK", "1") == "1"
PROGRESS_LOG = os.getenv("PROGRESS_LOG", "")

FEATURE_COLS = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]

# Campos de rastreio que atravessam o fluxo sem entrar no modelo:
# "timestamp" (instante de envio pelo produtor) permite medir a latência
# ponta a ponta; "tx_id"/"run_id" permitem contar perdas e duplicatas.
SCHEMA = StructType(
    [StructField(c, DoubleType(), True) for c in FEATURE_COLS]
    + [
        StructField("timestamp", DoubleType(), True),
        StructField("tx_id", LongType(), True),
        StructField("run_id", StringType(), True),
    ]
)

PREDICTION_SCHEMA = StructType([
    StructField("Time", DoubleType()),
    StructField("Amount", DoubleType()),
    StructField("prediction", IntegerType()),
    StructField("fraud_probability", DoubleType()),
    StructField("alert", StringType()),
    StructField("timestamp", DoubleType()),
    StructField("tx_id", LongType()),
    StructField("run_id", StringType()),
])


def load_model_and_scaler():
    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    return model, scaler


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName(cfg.spark_app_name)
        .master(cfg.spark_master)
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.streaming.checkpointLocation", "checkpoint")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.ui.prometheus.enabled", "true")
    )
    for env, key in [
        ("SPARK_EXECUTOR_MEMORY", "spark.executor.memory"),
        ("SPARK_DRIVER_MEMORY", "spark.driver.memory"),
        ("SPARK_DRIVER_HOST", "spark.driver.host"),
        ("SPARK_CORES_MAX", "spark.cores.max"),
    ]:
        if os.getenv(env):
            builder = builder.config(key, os.getenv(env))
    if os.getenv("SPARK_CORES_MAX"):
        # Só agenda tarefas depois que todos os núcleos esperados registraram
        # executores; sem isso o primeiro micro-lote pode rodar em um único worker.
        builder = (
            builder
            .config("spark.scheduler.minRegisteredResourcesRatio", "1.0")
            .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "60s")
        )
    return builder.getOrCreate()


def log_progress(query, path):
    """Grava cada progresso de micro-lote (JSON por linha) para as medições."""
    seen = set()
    with open(path, "a", buffering=1) as f:
        while query.isActive:
            for p in query.recentProgress:
                key = (str(p.runId), p.batchId)
                if key not in seen:
                    seen.add(key)
                    f.write(json.dumps(json.loads(p.json)) + "\n")
            time.sleep(1)


def main():
    print("=" * 60)
    print("SPARK STREAMING - Detecção de Fraudes em Tempo Real")
    print("=" * 60)

    model, scaler = load_model_and_scaler()
    feature_cols = FEATURE_COLS
    scale_cols = cfg.scale_cols

    print(f"Modelo carregado: {MODEL_PATH}")
    print(f"Scaler carregado: {SCALER_PATH}")
    print(f"Mestre Spark: {cfg.spark_master}")
    print(f"Conector Kafka: {KAFKA_PACKAGE}")
    print(f"Consumindo do Kafka em '{cfg.kafka_bootstrap_servers}'")
    print(f"Tópico de entrada: '{cfg.kafka_topic}'")
    print(f"Tópico de saída (predições): '{cfg.kafka_predictions_topic}'")
    print("-" * 60)

    spark = create_spark_session()

    model_bc = spark.sparkContext.broadcast(model)
    scaler_bc = spark.sparkContext.broadcast(scaler)

    spark.sparkContext.setLogLevel("WARN")

    def predict_df(iterator):
        model_local = model_bc.value
        scaler_local = scaler_bc.value
        for pdf in iterator:
            if pdf.empty:
                continue
            pdf_scaled = pdf.copy()
            pdf_scaled[scale_cols] = scaler_local.transform(pdf_scaled[scale_cols])
            X = pdf_scaled[feature_cols].values
            preds = model_local.predict(X)
            probas = model_local.predict_proba(X)[:, 1]

            out = pdf[["Time", "Amount"]].copy()
            out["prediction"] = preds
            out["fraud_probability"] = probas
            out["alert"] = np.where(preds == 1, "FRAUDE", "NORMAL")
            out["timestamp"] = pdf["timestamp"]
            out["tx_id"] = pdf["tx_id"].astype("Int64")
            out["run_id"] = pdf["run_id"]
            yield out

    df_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("subscribe", cfg.kafka_topic)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        df_stream
        .select(from_json(col("value").cast("string"), SCHEMA).alias("data"))
        .select("data.*")
    )

    predictions = parsed.mapInPandas(predict_df, schema=PREDICTION_SCHEMA)

    output_sink = (
        predictions
        .select(
            to_json(struct(
                "Time", "Amount", "prediction",
                "fraud_probability", "alert",
                "timestamp", "tx_id", "run_id"
            )).alias("value")
        )
        .writeStream
        .queryName("predictions_sink")
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("topic", cfg.kafka_predictions_topic)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )

    if PROGRESS_LOG:
        threading.Thread(
            target=log_progress, args=(output_sink, PROGRESS_LOG), daemon=True
        ).start()

    if ENABLE_CONSOLE_SINK:
        console_sink = (
            predictions
            .writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 10)
            .trigger(processingTime="5 seconds")
            .start()
        )

    print("\nStreaming em execução. Predições enviadas para Kafka:")
    print(f"  bootstrap: {cfg.kafka_bootstrap_servers}")
    print(f"  tópico:    {cfg.kafka_predictions_topic}")
    print("Pressione Ctrl+C para parar.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
