import sys
import os
import json
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config
from metrics_exporter import metrics

cfg = Config()

LATENCY_CSV = os.getenv("LATENCY_CSV", "")

try:
    from kafka import KafkaConsumer
except ImportError:
    print("kafka-python não instalado. Instale com: pip install kafka-python")
    sys.exit(1)


def format_rate(total: int, seconds: float) -> float:
    return total / seconds if seconds > 0 else 0.0


def main():
    print("=" * 60)
    print("KAFKA -> PROMETHEUS BRIDGE")
    print("Consome predições do Spark e publica métricas")
    print("=" * 60)

    consumer = KafkaConsumer(
        cfg.kafka_predictions_topic,
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="metrics-bridge",
        # Mantém cada resposta de fetch abaixo do limite de frame (1 MB) do
        # kafka-python; sem isso a ponte cai com "Invalid frame length" quando
        # há acúmulo de predições em mais de uma partição.
        fetch_max_bytes=512 * 1024,
        max_partition_fetch_bytes=256 * 1024,
    )

    metrics.start()

    print(f"Assinando tópico '{cfg.kafka_predictions_topic}' em {cfg.kafka_bootstrap_servers}")
    print(f"Métricas: http://localhost:8000/metrics")
    print("Aguardando predições do Spark... (Ctrl+C para parar)")

    processed = 0
    frauds = 0
    t0 = time.time()

    # Registro bruto opcional (uma linha por predição) usado nas medições.
    raw_log = open(LATENCY_CSV, "a", buffering=1) if LATENCY_CSV else None
    if raw_log and raw_log.tell() == 0:
        raw_log.write("run_id,tx_id,ts_produzido,ts_publicado,ts_metrica,prediction,fraud_probability\n")

    try:
        for msg in consumer:
            rec = msg.value
            prediction = int(rec["prediction"])
            probability = float(rec["fraud_probability"])
            amount = float(rec["Amount"])

            # Latência ponta a ponta: agora - instante em que o produtor enviou.
            now = time.time()
            produced = rec.get("timestamp")
            latency = max(0.0, now - produced) if produced is not None else 0.0

            metrics.record_prediction(
                prediction=prediction,
                probability=probability,
                amount=amount,
                latency=latency,
            )

            if raw_log:
                # msg.timestamp: instante (ms) em que o Spark publicou a predição.
                raw_log.write(
                    f"{rec.get('run_id')},{rec.get('tx_id')},{produced!r},"
                    f"{msg.timestamp / 1000.0!r},{now!r},{prediction},{probability!r}\n"
                )

            processed += 1
            frauds += prediction

            if processed % 100 == 0:
                elapsed = time.time() - t0
                rate = format_rate(processed, elapsed)
                print(
                    f"[{time.strftime('%H:%M:%S')}] {processed} predições | "
                    f"{frauds} fraudes | {rate:.0f} msg/s"
                )
    except KeyboardInterrupt:
        print("\nBridge encerrado.")


if __name__ == "__main__":
    main()