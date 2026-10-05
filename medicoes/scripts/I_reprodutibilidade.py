"""Item I - docker compose down -v e up do zero; tempo até tudo saudável.

Uso: venv/bin/python medicoes/scripts/I_reprodutibilidade.py [repetições]
As imagens já estão baixadas/construídas (o tempo não inclui pull nem build).
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import *

reps = int(sys.argv[1]) if len(sys.argv) > 1 else 3
OUT = "medicoes/I_reprodutibilidade"
os.makedirs(OUT, exist_ok=True)


def saude():
    out = sh("docker compose ps -a --format json", check=False)
    d = [json.loads(l) for l in out.splitlines() if l.strip().startswith("{")]
    return {x["Service"]: (x["State"], x.get("Health", "")) for x in d}


def alvos():
    try:
        d = json.load(urllib.request.urlopen("http://localhost:9090/api/v1/targets", timeout=3))
        return [(t["labels"]["job"], t["health"]) for t in d["data"]["activeTargets"]]
    except Exception:
        return []


for rep in range(1, reps + 1):
    t = time.time()
    sh("docker compose down -v --remove-orphans")
    t_down = time.time() - t
    off = fsize(PROGRESS)
    t0 = time.time()
    r = subprocess.run("docker compose up -d", shell=True, capture_output=True, text=True)
    t_up = time.time() - t0
    with open(f"{OUT}/up_rep{rep}.log", "w") as f:
        f.write(r.stdout + r.stderr)
    t_saud = t_alvos = None
    while time.time() - t0 < 600 and (t_saud is None or t_alvos is None):
        s = saude()
        ok_s = all((st == "running" and h in ("healthy", "")) or (svc == "kafka-init" and st == "exited") for svc, (st, h) in s.items()) and len(s) >= 9
        if t_saud is None and ok_s:
            t_saud = time.time() - t0
        a = alvos()
        if t_alvos is None and len(a) >= 5 and all(h == "up" for _, h in a):
            t_alvos = time.time() - t0
        time.sleep(0.5)
    # primeira linha de progresso = consulta de streaming do Spark efetivamente ativa
    while fsize(PROGRESS) <= off and time.time() - t0 < 600:
        time.sleep(0.5)
    t_consulta = time.time() - t0
    # prova funcional: uma carga curta precisa atravessar o fluxo inteiro
    rid = f"I_r{rep}"
    b = fsize(BRIDGE_CSV)
    pr = produce(rid, 100, 10)
    ok, dt = wait_drain(b, rid, pr["enviadas"], 240)
    res = {"repeticao": rep, "codigo_retorno_up": r.returncode, "tempo_down_v_s": round(t_down, 1),
           "tempo_comando_up_s": round(t_up, 1),
           "tempo_ate_todos_saudaveis_s": round(t_saud, 1) if t_saud else "",
           "tempo_ate_alvos_prometheus_up_s": round(t_alvos, 1) if t_alvos else "",
           "tempo_ate_consulta_spark_ativa_s": round(t_consulta, 1),
           "teste_funcional_enviadas": pr["enviadas"], "teste_funcional_completo": int(ok),
           "tempo_ate_fim_teste_funcional_s": round(time.time() - t0, 1),
           "estado_final": json.dumps(saude()), "alvos": json.dumps(alvos())}
    append_csv(f"{OUT}/execucoes.csv", res)
    log(res)
log("FIM I")
