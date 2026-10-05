"""Itens D e F - carga ponta a ponta em patamares, com docker stats em paralelo.

Uso: venv/bin/python medicoes/scripts/D_ponta_a_ponta.py [taxas separadas por vírgula] [repetições] [duração_s]
Configuração-base: 1 worker Spark (2 núcleos), tópicos com 3 partições.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import *

taxas = [int(x) for x in (sys.argv[1] if len(sys.argv) > 1 else "100,500,1000,2000,5000").split(",")]
reps = int(sys.argv[2]) if len(sys.argv) > 2 else 5
dur = int(sys.argv[3]) if len(sys.argv) > 3 else 60
procs = int(sys.argv[4]) if len(sys.argv) > 4 else 1  # processos do gerador de carga em paralelo
OUT = "medicoes/D_ponta_a_ponta"
os.makedirs(f"{OUT}/brutos", exist_ok=True)
os.makedirs("medicoes/F_recursos", exist_ok=True)

reset_pipeline(partitions=3, workers=1)
for taxa in taxas:
    for rep in range(1, reps + 1):
        run_id = f"D_t{taxa}_r{rep}"
        log("início", run_id)
        res, pg, df = run_and_measure(run_id, taxa, dur, "medicoes/F_recursos/docker_stats_D.csv", f"{OUT}/lag_por_segundo.csv", procs=procs)
        res = {"repeticao": rep, **res}
        append_csv(f"{OUT}/execucoes.csv", res)
        if len(pg):
            pg.insert(0, "run_id", run_id)
            pg.to_csv(f"{OUT}/progresso_microlotes.csv", mode="a", index=False, header=not os.path.exists(f"{OUT}/progresso_microlotes.csv"))
        df.to_csv(f"{OUT}/brutos/{run_id}.csv.gz", index=False)
        log({k: res[k] for k in ("taxa_efetiva_produtor", "vazao_sustentada_tx_s", "lat_metrica_p50_s", "lat_metrica_p95_s", "lat_metrica_p99_s", "lag_max_msgs", "dreno_completo", "tempo_dreno_s", "duplicadas", "faltantes")})
        if not res["dreno_completo"]:
            reset_pipeline(partitions=3, workers=1)
log("FIM D")
