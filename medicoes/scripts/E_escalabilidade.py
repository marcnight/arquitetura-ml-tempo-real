"""Item E - vazão máxima do Spark por configuração (workers x partições).

Uso: venv/bin/python medicoes/scripts/E_escalabilidade.py [mensagens] [repetições]
Como o Spark não saturou nos patamares do item D, a capacidade é medida por
acúmulo: com o fluxo parado, N mensagens são gravadas no tópico 'transactions';
em seguida o fluxo é iniciado com startingOffsets=earliest e mede-se a taxa em
que as predições aparecem no tópico 'predictions' (offsets amostrados a cada 0,5 s).
A vazão é a inclinação entre 25% e 95% das predições publicadas (exclui a
partida do executor e a carga do modelo). A ponte de métricas fica parada.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import *

N = int(sys.argv[1]) if len(sys.argv) > 1 else 300000
reps = int(sys.argv[2]) if len(sys.argv) > 2 else 3
OUT = "medicoes/E_escalabilidade"
os.makedirs(OUT, exist_ok=True)
KT = "docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092"


def preparar(parts, workers):
    sh("docker compose stop spark-streaming metrics-bridge")
    for t in ("transactions", "predictions"):
        sh(f"{KT} --delete --if-exists --topic {t}")
    for _ in range(30):
        if not set(sh(f"{KT} --list").split()) & {"transactions", "predictions"}:
            break
        time.sleep(1)
    for t in ("transactions", "predictions"):
        sh(f"{KT} --create --topic {t} --partitions {parts} --replication-factor 1")
    sh("docker compose run --rm --no-deps -T --entrypoint bash spark-streaming -c 'rm -rf /app/checkpoint/*'")
    sh(f"docker compose up -d --no-recreate --scale spark-worker={workers} spark-master spark-worker")
    wait_workers(workers)


for workers in [int(x) for x in os.getenv("E_WORKERS", "1,2,3").split(",")]:
    for parts in (1, 3, 6):
        for rep in range(1, reps + 1):
            run_id = f"E_w{workers}_p{parts}_r{rep}"
            if os.path.exists(f"{OUT}/execucoes.csv") and run_id in set(pd.read_csv(f"{OUT}/execucoes.csv")["run_id"]):
                continue  # já medida em uma passada anterior
            try:
                log("preparando", run_id)
                preparar(parts, workers)
                pr = produce(run_id, 12000, count=N, procs=4)
                c = KafkaConsumer(bootstrap_servers=BOOT)
                assert end_offsets(c, "transactions") == N, "tópico de entrada incompleto"
                off = fsize(PROGRESS)
                t_start = time.time()
                sh(f"STARTING_OFFSETS=earliest SPARK_CORES_MAX={2 * workers} docker compose up -d --no-deps --scale spark-worker={workers} spark-streaming")
                serie = []
                stats = []
                while time.time() - t_start < 420:
                    t = time.time()
                    n = end_offsets(c, "predictions")
                    serie.append((t, n))
                    if n >= N:
                        break
                    time.sleep(max(0, 0.2 - (time.time() - t)))
                c.close()
                st = sh("docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}'", check=False)
                s = pd.DataFrame(serie, columns=["ts", "predicoes_publicadas"])
                s.insert(0, "run_id", run_id)
                s.to_csv(f"{OUT}/serie_offsets.csv", mode="a", index=False, header=not os.path.exists(f"{OUT}/serie_offsets.csv"))
                completo = int(s["predicoes_publicadas"].iloc[-1] >= N)
                a = s[s["predicoes_publicadas"] >= 0.25 * N].iloc[0] if completo else None
                b = s[s["predicoes_publicadas"] >= 0.95 * N].iloc[0] if completo else None
                prim = s[s["predicoes_publicadas"] > 0]
                time.sleep(3)
                # quantos executores receberam tarefas (API REST do driver)
                try:
                    app = json.load(urllib.request.urlopen("http://localhost:4040/api/v1/applications", timeout=5))[0]["id"]
                    ex = json.load(urllib.request.urlopen(f"http://localhost:4040/api/v1/applications/{app}/executors", timeout=5))
                    ex = [x for x in ex if x["id"] != "driver"]
                    n_exec, n_exec_tarefas = len(ex), sum(1 for x in ex if x["totalTasks"] > 0)
                except Exception:
                    n_exec = n_exec_tarefas = ""
                pg = progress_window(t_start - 5, time.time() + 5)
                pg = pg[pg["numInputRows"] > 0] if len(pg) else pg
                res = {
                    "workers": workers, "particoes": parts, "repeticao": rep, "run_id": run_id, "mensagens": N,
                    "completo": completo, "executores": n_exec, "executores_com_tarefas": n_exec_tarefas,
                    "vazao_primeira_a_ultima_tx_s": round(N / (s["ts"].iloc[-1] - prim["ts"].iloc[0]), 1) if completo and len(prim) else "",
                    "vazao_25_95_tx_s": round((b["predicoes_publicadas"] - a["predicoes_publicadas"]) / (b["ts"] - a["ts"]), 1) if completo and b["ts"] > a["ts"] else "",
                    "tempo_ate_primeira_predicao_s": round(prim["ts"].iloc[0] - t_start, 1) if len(prim) else "",
                    "tempo_total_s": round(s["ts"].iloc[-1] - t_start, 1),
                    "tempo_primeira_a_ultima_s": round(s["ts"].iloc[-1] - prim["ts"].iloc[0], 1) if len(prim) else "",
                    "n_microlotes": len(pg),
                    "numInputRows_total": int(pg["numInputRows"].sum()) if len(pg) else "",
                    "processedRowsPerSecond_ponderado": round(float((pg["processedRowsPerSecond"] * pg["numInputRows"]).sum() / pg["numInputRows"].sum()), 1) if len(pg) else "",
                    "taxa_carga_previa_tx_s": round(pr["taxa_efetiva"], 1),
                    "predicoes_no_topico": int(s["predicoes_publicadas"].iloc[-1]),
                }
                append_csv(f"{OUT}/execucoes.csv", res)
                with open(f"{OUT}/docker_stats_fim_execucao.csv", "a") as f:
                    for line in st.splitlines():
                        if line.startswith("tcc-"):
                            f.write(f"{run_id},{line}\n")
                log(res)
            except Exception as e:
                log("FALHA", run_id, repr(e)[:600])
                estado = sh("docker inspect tcc-kafka-1 --format 'status={{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} fim={{.State.FinishedAt}}'", check=False).strip()
                append_csv(f"{OUT}/falhas.csv", {"run_id": run_id, "erro": repr(e)[:1500] + " | kafka: " + estado})
                if "status=running" not in estado:
                    with open(f"medicoes/logs/kafka_queda_{run_id}.log", "w") as f:
                        f.write(estado + "\n" + sh("docker logs --tail 200 tcc-kafka-1 2>&1", check=False) + "\n" + sh("free -m", check=False))
                    sh("docker compose up -d kafka", check=False)
                    time.sleep(20)
# volta à configuração-base (1 worker, 3 partições, startingOffsets=latest)
sh("docker compose stop spark-streaming")
sh("docker compose rm -f spark-streaming")
log("FIM E")
