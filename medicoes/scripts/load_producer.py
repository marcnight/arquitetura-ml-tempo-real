"""Gerador de carga com taxa controlada para o tópico 'transactions'.

Uso: venv/bin/python medicoes/scripts/load_producer.py --rate 1000 --duration 60 --run-id X
Cada mensagem é uma linha real do creditcard.csv (percorrido em ordem, com
retorno ao início) acrescida de "timestamp" (time.time() no envio), "tx_id"
(sequencial na execução) e "run_id". Imprime um resumo JSON ao final.
"""
import argparse, json, sys, time, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
import pandas as pd
from kafka import KafkaProducer
from config import Config

cfg = Config()
ap = argparse.ArgumentParser()
ap.add_argument("--rate", type=float, required=True)
ap.add_argument("--duration", type=float, default=60.0)
ap.add_argument("--count", type=int, default=0, help="envia exatamente N mensagens (ignora --duration)")
ap.add_argument("--run-id", required=True)
ap.add_argument("--topic", default=cfg.kafka_topic)
ap.add_argument("--offset", type=int, default=0, help="linha inicial do dataset")
ap.add_argument("--tx-offset", type=int, default=0, help="primeiro tx_id (para produtores em paralelo)")
a = ap.parse_args()

total = a.count if a.count else int(round(a.rate * a.duration))
df = pd.read_csv(cfg.raw_data_path)
n = len(df)
rows = df.iloc[[(a.offset + i) % n for i in range(min(total, n))]].to_dict("records")
del df
# Pré-serializa a parte fixa de cada mensagem; só o rastreio é montado no envio.
prefixes = [json.dumps(r)[:-1] for r in rows]
suffix = ', "run_id": "%s"}' % a.run_id

producer = KafkaProducer(bootstrap_servers=cfg.kafka_bootstrap_servers, acks="all", retries=3, linger_ms=5)
errors = []
sent = 0
t0 = time.time()
while sent < total:
    alvo = min(total, int((time.time() - t0) * a.rate) + 1)
    while sent < alvo:
        msg = '%s, "timestamp": %r, "tx_id": %d%s' % (prefixes[sent % len(prefixes)], time.time(), a.tx_offset + sent, suffix)
        producer.send(a.topic, msg.encode("utf-8")).add_errback(lambda e: errors.append(repr(e)))
        sent += 1
    time.sleep(0.002)
t_fim_envio = time.time()
producer.flush(timeout=120)  # sem timeout o cliente pode bloquear indefinidamente
t1 = time.time()
producer.close()
print(json.dumps({
    "run_id": a.run_id, "taxa_nominal": a.rate, "enviadas": sent, "erros_envio": len(errors),
    "t_inicio": t0, "t_fim": t1, "duracao_s": t1 - t0,
    "taxa_efetiva": sent / (t1 - t0), "amostra_erros": errors[:3],
}))
