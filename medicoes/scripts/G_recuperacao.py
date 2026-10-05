"""Item G - reinício do Spark durante a carga; conta perdas e duplicatas por tx_id.

Uso: venv/bin/python medicoes/scripts/G_recuperacao.py <taxa> [repetições]
Cenários: 'driver' = SIGKILL no contêiner spark-streaming (driver) aos 30 s e
novo start; 'worker' = SIGKILL no contêiner spark-worker (executor) aos 30 s e novo start.
A carga dura 90 s. O checkpoint do fluxo fica em volume e é preservado no reinício.
"""
import os, sys, threading
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import *

taxa = int(sys.argv[1])
reps = int(sys.argv[2]) if len(sys.argv) > 2 else 3
OUT = "medicoes/G_recuperacao"
os.makedirs(f"{OUT}/brutos", exist_ok=True)

CEN = {"driver_restart_on_failure": "spark-streaming", "worker": "spark-worker"}
for cenario, svc in CEN.items():
    for rep in range(1, reps + 1):
        run_id = f"G_{cenario}_r{rep}"
        reset_pipeline(partitions=3, workers=1)
        log("início", run_id)
        b = fsize(BRIDGE_CSV)
        ev = {}

        def falha():
            time.sleep(30)
            ev["t_kill"] = time.time()
            sh(f"docker compose kill -s SIGKILL {svc}")
            ev["t_kill_fim"] = time.time()
            sh(f"docker compose start {svc}")
            ev["t_start_fim"] = time.time()

        th = threading.Thread(target=falha)
        th.start()
        r = produce(run_id, taxa, 90)
        th.join()
        ok, dreno = wait_drain(b, run_id, r["enviadas"], 420)
        time.sleep(30)  # margem: duplicatas podem antecipar o critério de dreno por contagem
        df = read_bridge(b)
        df = df[df["run_id"] == run_id]
        df.to_csv(f"{OUT}/brutos/{run_id}.csv.gz", index=False)
        cont = df["tx_id"].value_counts()
        pub = df.sort_values("ts_publicado")["ts_publicado"].to_numpy()
        depois = pub[pub > ev["t_kill"]]
        antes = pub[pub <= ev["t_kill"]]
        lat = (df.drop_duplicates("tx_id")["ts_metrica"] - df.drop_duplicates("tx_id")["ts_produzido"])
        res = {
            "cenario": cenario, "repeticao": rep, "run_id": run_id, "taxa_nominal": taxa,
            "enviadas": r["enviadas"], "erros_envio": r["erros_envio"],
            "predicoes_recebidas": len(df), "tx_unicas": int(cont.size),
            "perdidas": r["enviadas"] - int(cont.size),
            "duplicadas_mensagens_extras": int(len(df) - cont.size),
            "tx_com_duplicata": int((cont > 1).sum()),
            "dreno_completo": int(ok), "tempo_dreno_s": round(dreno, 1),
            "indisponibilidade_s": round(float(depois.min() - ev["t_kill"]), 2) if len(depois) else float("nan"),
            "ultima_publicacao_antes_s": round(float(ev["t_kill"] - antes.max()), 2) if len(antes) else float("nan"),
            "lat_max_s": round(float(lat.max()), 2) if len(lat) else float("nan"),
            "reinicios_do_driver": int(sh("docker inspect tcc-spark-streaming-1 --format '{{.RestartCount}}'", check=False).strip() or -1),
            "t_kill": ev["t_kill"], "t_inicio": r["t_inicio"], "t_fim": r["t_fim"],
        }
        append_csv(f"{OUT}/execucoes.csv", res)
        log(res)
log("FIM G")
