"""Item C - equivalência entre o caminho Pandas e o caminho Spark.

Uso: venv/bin/python medicoes/scripts/C_equivalencia.py
As 10.000 primeiras transações do creditcard.csv passam (i) por
FraudDetectionPipeline.predict_batch no host e (ii) pelo fluxo real
Kafka -> Spark Structured Streaming (cluster do compose) -> Kafka -> ponte.
O pareamento é feito por tx_id (= índice da linha no CSV).
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import *
sys.path.insert(0, "src")
from inference_pipeline import FraudDetectionPipeline

N = 10000
OUT = "medicoes/C_equivalencia"
os.makedirs(OUT, exist_ok=True)
base = pd.read_csv("base_fraudes/creditcard.csv").iloc[:N]
pandas_res = FraudDetectionPipeline(enable_metrics=False).predict_batch(base)

b = fsize(BRIDGE_CSV)
r = produce("C_equivalencia", 500, count=N)
ok, dt = wait_drain(b, "C_equivalencia", N, 300)
sp = read_bridge(b)
sp = sp[sp["run_id"] == "C_equivalencia"]
dup = len(sp) - sp["tx_id"].nunique()
sp = sp.drop_duplicates("tx_id").set_index("tx_id").sort_index()

cmp = pd.DataFrame({
    "tx_id": base.index,
    "classe_real": base["Class"].values,
    "prob_pandas": pandas_res["fraud_probability"].values,
    "rotulo_pandas": pandas_res["prediction"].values,
}).set_index("tx_id")
cmp["prob_spark"] = sp["fraud_probability"]
cmp["rotulo_spark"] = sp["prediction"]
cmp["dif_abs_prob"] = (cmp["prob_pandas"] - cmp["prob_spark"]).abs()
cmp.to_csv(f"{OUT}/comparacao_10000.csv")
resumo = {
    "transacoes": N, "enviadas": r["enviadas"], "recebidas_do_spark": int(cmp["prob_spark"].notna().sum()),
    "duplicadas": dup, "dreno_completo": int(ok),
    "divergencia_maxima_prob": float(cmp["dif_abs_prob"].max()),
    "divergencia_media_prob": float(cmp["dif_abs_prob"].mean()),
    "tx_com_prob_diferente": int((cmp["dif_abs_prob"] > 0).sum()),
    "rotulos_diferentes": int((cmp["rotulo_pandas"] != cmp["rotulo_spark"]).sum()),
    "fraudes_preditas_pandas": int(cmp["rotulo_pandas"].sum()),
    "fraudes_preditas_spark": int(cmp["rotulo_spark"].sum()),
}
pd.DataFrame([resumo]).to_csv(f"{OUT}/resumo.csv", index=False)
print(json.dumps(resumo, indent=1))
