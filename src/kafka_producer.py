import sys
import os
import json
import time
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config

cfg = Config()

try:
    from kafka import KafkaProducer
except ImportError:
    print("kafka-python não instalado. Instale com: pip install kafka-python")
    sys.exit(1)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def send_transactions(producer, df, delay=0.01):
    print(f"Enviando {len(df)} transações para Kafka tópico '{cfg.kafka_topic}'...")
    print(f"Delay entre mensagens: {delay}s")
    print("-" * 50)

    sent = 0
    start = time.time()

    for idx, row in df.iterrows():
        message = row.to_dict()
        message["timestamp"] = time.time()
        message["tx_id"] = int(idx)

        producer.send(cfg.kafka_topic, value=message)

        sent += 1
        if sent % 10000 == 0:
            elapsed = time.time() - start
            rate = sent / elapsed if elapsed > 0 else 0
            print(f"  Enviadas {sent} transações... ({rate:.0f} msg/s)")

        if delay > 0:
            time.sleep(delay)

    producer.flush()
    elapsed = time.time() - start
    print(f"\nConcluído! {sent} transações enviadas em {elapsed:.1f}s")
    print(f"Taxa média: {sent/elapsed:.0f} msg/s")


def send_batch(producer, df, batch_size=1000, batch_delay=0.5):
    print(f"Enviando {len(df)} transações em lotes de {batch_size}...")
    total_sent = 0
    start = time.time()

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        for idx, row in batch.iterrows():
            message = row.to_dict()
            message["timestamp"] = time.time()
            message["tx_id"] = int(idx)
            producer.send(cfg.kafka_topic, value=message)

        producer.flush()
        total_sent += len(batch)
        print(f"  Lote {i//batch_size + 1}: {len(batch)} enviadas (total: {total_sent})")
        time.sleep(batch_delay)

    elapsed = time.time() - start
    print(f"\nConcluído! {total_sent} transações em {elapsed:.1f}s")


if __name__ == "__main__":
    df = pd.read_csv(cfg.raw_data_path)
    print(f"Dataset carregado: {df.shape[0]} transações")

    producer = create_producer()

    print("\nEscolha o modo de envio:")
    print("1 - Tempo real (com delay, simulando streaming)")
    print("2 - Batch (lotes rápidos)")
    choice = input("Opção (1/2): ").strip()

    if choice == "2":
        send_batch(producer, df)
    else:
        try:
            delay = float(input("Delay entre mensagens (segundos, ex: 0.01): ") or 0.01)
        except ValueError:
            delay = 0.01
        send_transactions(producer, df, delay=delay)

    producer.close()
