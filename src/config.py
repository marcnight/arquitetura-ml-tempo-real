import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class Config:
    raw_data_path: str = "base_fraudes/creditcard.csv"
    models_dir: str = "models"
    data_dir: str = "data"
    processed_data_file: str = "data/processed_transactions.csv"

    random_state: int = 42
    test_size: float = 0.2
    fraud_ratio: float = 0.0017

    features_to_drop: List[str] = field(default_factory=lambda: ["Class"])
    target_column: str = "Class"
    scale_cols: List[str] = field(default_factory=lambda: ["Time", "Amount"])

    # No host o Kafka é alcançado por localhost:9092; dentro do compose, por kafka:29092.
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic: str = "transactions"
    kafka_predictions_topic: str = "predictions"

    spark_app_name: str = "FraudDetectionStreaming"
    spark_master: str = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    hdfs_output_path: str = "hdfs://localhost:9000/data/fraud_detection"

    mlflow_experiment_name: str = "FraudDetectionCreditCard"
    mlflow_tracking_uri: str = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
