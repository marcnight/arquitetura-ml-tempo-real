import time
import threading
from typing import Optional

from prometheus_client import (
    REGISTRY, Counter, Gauge, Histogram, start_http_server,
)

TRANSACTIONS_TOTAL = Counter(
    "transactions_total", "Total de transações processadas",
    ["status"]
)

FRAUD_TOTAL = Counter(
    "fraud_total", "Total de fraudes detectadas"
)

FRAUD_RATE = Gauge(
    "fraud_rate_percent", "Taxa de fraude em tempo real (%)"
)

INFERENCE_LATENCY = Histogram(
    "inference_latency_seconds", "Latência da inferência (segundos)",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
             2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
)

AMOUNT_HISTOGRAM = Histogram(
    "transaction_amount", "Valor das transações",
    buckets=[10, 50, 100, 200, 500, 1000, 5000, 10000],
    labelnames=["status"]
)

MODEL_PREDICTION = Counter(
    "model_prediction_total", "Distribuição das predições do modelo",
    ["prediction"]
)

CONFIDENCE_SCORE = Gauge(
    "fraud_confidence_score", "Score de confiança da última predição",
    ["type"]
)

STREAM_LAG = Gauge(
    "stream_lag_messages", "Lag estimado do stream (mensagens)"
)

_last_fraud_time = Gauge(
    "last_fraud_timestamp_seconds", "Timestamp da última fraude detectada"
)


def _counter_value(name: str, **labels) -> float:
    try:
        return float(REGISTRY.get_sample_value(name, labels) or 0.0)
    except (KeyError, ValueError):
        return 0.0


class MetricsServer:
    def __init__(self, port: int = 8000):
        self.port = port
        self._server_thread: Optional[threading.Thread] = None
        self._running = False

    def start(self):
        if self._running:
            return

        def _serve():
            start_http_server(self.port)

        self._server_thread = threading.Thread(target=_serve, daemon=True)
        self._server_thread.start()
        self._running = True
        print(f"  -> Prometheus metrics em http://localhost:{self.port}/metrics")

    def record_prediction(self, prediction: int, probability: float, amount: float, latency: float):
        status = "fraud" if prediction == 1 else "normal"

        TRANSACTIONS_TOTAL.labels(status=status).inc()
        MODEL_PREDICTION.labels(prediction=str(prediction)).inc()
        INFERENCE_LATENCY.observe(latency)
        AMOUNT_HISTOGRAM.labels(status=status).observe(amount)

        if prediction == 1:
            FRAUD_TOTAL.inc()
            _last_fraud_time.set(time.time())
            CONFIDENCE_SCORE.labels(type="fraud").set(probability)
        else:
            CONFIDENCE_SCORE.labels(type="normal").set(1 - probability)

        total_transactions = (
            _counter_value("transactions_total", status="fraud")
            + _counter_value("transactions_total", status="normal")
        )
        if total_transactions > 0:
            fraud_count = _counter_value("transactions_total", status="fraud")
            FRAUD_RATE.set((fraud_count / total_transactions) * 100)

    def set_stream_lag(self, lag: int):
        STREAM_LAG.set(lag)


metrics = MetricsServer()