"""Item B - inferência em processo único (caminho Pandas) na base completa.

Uso: venv/bin/python medicoes/scripts/B_inferencia_processo_unico.py <reps> <metricas:0|1> <saida.csv>
Mede apenas pipeline.predict_batch(df) (a leitura do CSV e a carga do modelo
ficam fora do tempo, como em FraudDetectionPipeline.run_batch).
"""
import sys, time, csv, os
sys.path.insert(0, "src")
import pandas as pd
from inference_pipeline import FraudDetectionPipeline

reps, metricas, out = int(sys.argv[1]), sys.argv[2] == "1", sys.argv[3]
df = pd.read_csv("base_fraudes/creditcard.csv")
pipeline = FraudDetectionPipeline(enable_metrics=metricas)
novo = not os.path.exists(out)
with open(out, "a", newline="") as f:
    w = csv.writer(f)
    if novo:
        w.writerow(["repeticao", "registro_metricas", "transacoes", "tempo_s", "tx_por_s", "fraudes_preditas"])
    for r in range(1, reps + 1):
        t0 = time.perf_counter()
        res = pipeline.predict_batch(df)
        dt = time.perf_counter() - t0
        w.writerow([r, int(metricas), len(df), f"{dt:.6f}", f"{len(df)/dt:.1f}", int(res["prediction"].sum())])
        f.flush()
        print(r, f"{dt:.3f}s", f"{len(df)/dt:.0f} tx/s", flush=True)
