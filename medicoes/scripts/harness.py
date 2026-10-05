"""Funções comuns das medições D, E, F e G (executar a partir da raiz do projeto)."""
import csv, io, json, os, subprocess, sys, threading, time, urllib.parse, urllib.request
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, TopicPartition

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
os.chdir(ROOT)
PY = os.path.join(ROOT, "venv/bin/python")
RAW = "medicoes/raw"
BRIDGE_CSV = f"{RAW}/latencias_bridge.csv"
PROGRESS = f"{RAW}/spark_progress.jsonl"
BOOT = "localhost:9092"
COLS = ["run_id", "tx_id", "ts_produzido", "ts_publicado", "ts_metrica", "prediction", "fraud_probability"]


def log(*a):
    print(time.strftime("[%H:%M:%S]"), *a, flush=True)


def sh(cmd, check=True, quiet=True):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"falhou: {cmd}\n{r.stdout}\n{r.stderr}")
    return r.stdout


def fsize(p):
    return os.path.getsize(p) if os.path.exists(p) else 0


def end_offsets(consumer, topic):
    parts = consumer.partitions_for_topic(topic) or set()
    tps = [TopicPartition(topic, p) for p in parts]
    return sum(consumer.end_offsets(tps).values()) if tps else 0


class Sampler:
    """Amostra, durante uma execução, o lag do fluxo (offsets) e o docker stats."""

    def __init__(self, run_id, stats_csv, lag_csv):
        self.run_id, self.stats_csv, self.lag_csv = run_id, stats_csv, lag_csv
        self.stop = threading.Event()
        self.lag = []
        self.threads = [threading.Thread(target=self._lag), threading.Thread(target=self._stats)]

    def __enter__(self):
        self.c = KafkaConsumer(bootstrap_servers=BOOT)
        self.base = (end_offsets(self.c, "transactions"), end_offsets(self.c, "predictions"))
        for t in self.threads:
            t.start()
        return self

    def __exit__(self, *a):
        self.stop.set()
        for t in self.threads:
            t.join()
        self.c.close()

    def _lag(self):
        novo = not os.path.exists(self.lag_csv)
        with open(self.lag_csv, "a", newline="") as f:
            w = csv.writer(f)
            if novo:
                w.writerow(["ts", "run_id", "transacoes_produzidas", "predicoes_publicadas", "lag_mensagens"])
            while not self.stop.is_set():
                t = time.time()
                a = end_offsets(self.c, "transactions") - self.base[0]
                b = end_offsets(self.c, "predictions") - self.base[1]
                self.lag.append((t, a, b, a - b))
                w.writerow([f"{t:.3f}", self.run_id, a, b, a - b])
                f.flush()
                self.stop.wait(max(0, 1.0 - (time.time() - t)))

    def _stats(self):
        novo = not os.path.exists(self.stats_csv)
        with open(self.stats_csv, "a", newline="") as f:
            w = csv.writer(f)
            if novo:
                w.writerow(["ts", "run_id", "conteiner", "cpu_pct", "mem_uso", "mem_limite", "mem_pct"])
            while not self.stop.is_set():
                t = time.time()
                out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"],
                                     capture_output=True, text=True).stdout
                for line in out.splitlines():
                    try:
                        d = json.loads(line)
                    except ValueError:
                        continue
                    if not d["Name"].startswith("tcc-"):
                        continue
                    uso, _, lim = d["MemUsage"].partition(" / ")
                    w.writerow([f"{t:.3f}", self.run_id, d["Name"], d["CPUPerc"].rstrip("%"), uso, lim, d["MemPerc"].rstrip("%")])
                f.flush()
                self.stop.wait(0.2)


def read_bridge(offset):
    """Lê as linhas novas do CSV bruto da ponte a partir de um deslocamento em bytes."""
    with open(BRIDGE_CSV, "rb") as f:
        f.seek(offset)
        data = f.read()
    data = data[: data.rfind(b"\n") + 1]
    if not data:
        return pd.DataFrame(columns=COLS)
    df = pd.read_csv(io.BytesIO(data), names=COLS, header=None, dtype=str)
    df = df[df["run_id"] != "run_id"]
    for c in COLS[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def count_run(offset, run_id):
    with open(BRIDGE_CSV, "rb") as f:
        f.seek(offset)
        return f.read().count(b"\n" + run_id.encode() + b",") + 0


def produce(run_id, rate, duration=60, count=0, offset=0, procs=1):
    """Dispara o gerador de carga; com procs>1 divide a taxa entre processos em paralelo."""
    total = count if count else int(round(rate * duration))
    ps = []
    for i in range(procs):
        n_i = total // procs + (1 if i < total % procs else 0)
        cmd = [PY, "medicoes/scripts/load_producer.py", "--rate", str(rate / procs), "--count", str(n_i),
               "--run-id", run_id, "--offset", str(offset + i * (total // procs)),
               "--tx-offset", str(i * (total // procs + 1))]
        ps.append(subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
    rs = []
    limite = time.time() + 3 * total / rate + 300
    for p in ps:
        try:
            out, err = p.communicate(timeout=max(1, limite - time.time()))
        except subprocess.TimeoutExpired:
            for q in ps:
                q.kill()
            raise RuntimeError("produtor travou (tempo-limite excedido)")
        if p.returncode != 0:
            for q in ps:
                q.kill()
            raise RuntimeError("produtor falhou: " + err[-2000:])
        rs.append(json.loads(out.strip().splitlines()[-1]))
    t0, t1 = min(r["t_inicio"] for r in rs), max(r["t_fim"] for r in rs)
    env = sum(r["enviadas"] for r in rs)
    return {"run_id": run_id, "taxa_nominal": rate, "enviadas": env, "erros_envio": sum(r["erros_envio"] for r in rs),
            "t_inicio": t0, "t_fim": t1, "duracao_s": t1 - t0, "taxa_efetiva": env / (t1 - t0), "processos_produtor": procs}


def wait_drain(offset, run_id, sent, timeout):
    """Espera a ponte registrar todas as predições da execução. Retorna (ok, segundos)."""
    t0 = time.time()
    last, last_change = -1, time.time()
    while time.time() - t0 < timeout:
        df = read_bridge(offset)
        n = int((df["run_id"] == run_id).sum())
        if n >= sent:
            return True, time.time() - t0
        if n != last:
            last, last_change = n, time.time()
        time.sleep(1)
    return False, time.time() - t0


def prom(query, t=None):
    q = {"query": query}
    if t:
        q["time"] = f"{t:.3f}"
    url = "http://localhost:9090/api/v1/query?" + urllib.parse.urlencode(q)
    return json.load(urllib.request.urlopen(url, timeout=10))["data"]["result"]


def prom_quantis(t_ini, t_fim):
    """p50/p95/p99 da latência a partir do histograma no Prometheus, na janela da execução."""
    w = int(t_fim - t_ini) + 1
    out = {}
    for q in (0.5, 0.95, 0.99):
        r = prom(f"histogram_quantile({q}, sum(increase(inference_latency_seconds_bucket[{w}s])) by (le))", t_fim)
        out[q] = float(r[0]["value"][1]) if r else float("nan")
    return out


def progress_window(t_ini, t_fim):
    rows = []
    if not os.path.exists(PROGRESS):
        return pd.DataFrame()
    for line in open(PROGRESS):
        try:
            p = json.loads(line)
        except ValueError:
            continue
        ts = pd.Timestamp(p["timestamp"]).timestamp()
        if t_ini <= ts <= t_fim:
            rows.append({
                "ts": ts, "batchId": p["batchId"], "numInputRows": p["numInputRows"],
                "inputRowsPerSecond": p.get("inputRowsPerSecond"),
                "processedRowsPerSecond": p.get("processedRowsPerSecond"),
                "triggerExecution_ms": p.get("durationMs", {}).get("triggerExecution"),
            })
    return pd.DataFrame(rows)


def reset_pipeline(partitions=3, workers=1, warmup=True):
    """Reinicia o fluxo do zero: tópicos recriados, checkpoint limpo, N workers."""
    log(f"reset: {workers} worker(s), {partitions} partição(ões)")
    sh("docker compose stop spark-streaming metrics-bridge")
    for t in ("transactions", "predictions"):
        sh(f"docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --delete --if-exists --topic {t}")
    for _ in range(30):
        if not set(sh("docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list").split()) & {"transactions", "predictions"}:
            break
        time.sleep(1)
    for t in ("transactions", "predictions"):
        sh(f"docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --topic {t} --partitions {partitions} --replication-factor 1")
    sh("docker compose run --rm --no-deps -T --entrypoint bash spark-streaming -c 'rm -rf /app/checkpoint/*'")
    sh(f"docker compose up -d --no-recreate --scale spark-worker={workers} spark-master spark-worker")
    wait_workers(workers)
    start_stream(warmup)


def wait_workers(n, timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            d = json.load(urllib.request.urlopen("http://localhost:8080/json/", timeout=5))
            if d.get("aliveworkers") == n:
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError(f"mestre não chegou a {n} workers vivos")


def start_stream(warmup=True, timeout=240):
    off = fsize(PROGRESS)
    sh("docker compose up -d --no-recreate --no-deps spark-streaming metrics-bridge")
    sh("docker compose start spark-streaming metrics-bridge", check=False)
    t0 = time.time()
    while fsize(PROGRESS) <= off:
        if time.time() - t0 > timeout:
            raise RuntimeError("consulta de streaming não iniciou: " + sh("docker compose logs --tail 40 spark-streaming", check=False))
        time.sleep(1)
    log(f"consulta ativa em {time.time()-t0:.0f}s")
    if warmup:
        rid = f"aquecimento_{int(time.time())}"
        b = fsize(BRIDGE_CSV)
        r = produce(rid, 200, 15)
        ok, dt = wait_drain(b, rid, r["enviadas"], 60)
        log(f"aquecimento: {r['enviadas']} msgs, completo={ok}, {dt:.0f}s")
        if not ok:
            # a ponte (offset 'latest') pode ter entrado no grupo depois das primeiras predições
            rid, b = rid + "b", fsize(BRIDGE_CSV)
            r = produce(rid, 200, 15)
            ok, dt = wait_drain(b, rid, r["enviadas"], 180)
            log(f"aquecimento (2ª tentativa): {r['enviadas']} msgs, completo={ok}, {dt:.0f}s")
        if not ok:
            raise RuntimeError("aquecimento não completou")


def run_and_measure(run_id, rate, duration, stats_csv, lag_csv, drain_timeout=300, procs=1):
    """Executa um patamar de carga e devolve o dicionário de resultados."""
    b = fsize(BRIDGE_CSV)
    c = KafkaConsumer(bootstrap_servers=BOOT)
    with Sampler(run_id, stats_csv, lag_csv) as s:
        base_pred = end_offsets(c, "predictions")
        r = produce(run_id, rate, duration, procs=procs)
        # Predições publicadas pelo Spark até o fim do envio (offsets do tópico de saída).
        pub_offsets = end_offsets(c, "predictions") - base_pred
        c.close()
        lag_fim_janela = r["enviadas"] - pub_offsets
        ok, dreno = wait_drain(b, run_id, r["enviadas"], drain_timeout)
        t_dreno = time.time()
        lag_max = max(x[3] for x in s.lag) if s.lag else float("nan")
    df = read_bridge(b)
    df = df[df["run_id"] == run_id]
    uniq = df.drop_duplicates("tx_id")
    lat_pub = (uniq["ts_publicado"] - uniq["ts_produzido"]).to_numpy()
    lat_met = (uniq["ts_metrica"] - uniq["ts_produzido"]).to_numpy()
    pub_janela = int((uniq["ts_publicado"] <= r["t_fim"]).sum())
    met_janela = int((uniq["ts_metrica"] <= r["t_fim"]).sum())
    time.sleep(6)  # garante ao menos uma coleta do Prometheus após o fim
    try:
        pq = prom_quantis(r["t_inicio"], t_dreno + 6)
    except Exception as e:
        log("prometheus falhou:", e)
        pq = {0.5: float("nan"), 0.95: float("nan"), 0.99: float("nan")}
    pg = progress_window(r["t_inicio"], r["t_fim"])
    pg = pg[pg["numInputRows"] > 0] if len(pg) else pg
    pct = lambda a, q: float(np.percentile(a, q)) if len(a) else float("nan")
    res = {
        "run_id": run_id, "taxa_nominal": rate, "enviadas": r["enviadas"], "erros_envio": r["erros_envio"],
        "processos_produtor": procs, "duracao_s": round(r["duracao_s"], 3), "taxa_efetiva_produtor": round(r["taxa_efetiva"], 1),
        "publicadas_na_janela": pub_offsets, "vazao_sustentada_tx_s": round(pub_offsets / r["duracao_s"], 1),
        "metricas_na_janela": met_janela, "vazao_ponte_tx_s": round(met_janela / r["duracao_s"], 1),
        "recebidas_total": len(df), "unicas": len(uniq), "duplicadas": len(df) - len(uniq),
        "faltantes": r["enviadas"] - len(uniq), "dreno_completo": int(ok), "tempo_dreno_s": round(dreno, 1),
        "lat_publicacao_p50_s": pct(lat_pub, 50), "lat_publicacao_p95_s": pct(lat_pub, 95), "lat_publicacao_p99_s": pct(lat_pub, 99),
        "lat_metrica_p50_s": pct(lat_met, 50), "lat_metrica_p95_s": pct(lat_met, 95), "lat_metrica_p99_s": pct(lat_met, 99),
        "lat_metrica_max_s": float(lat_met.max()) if len(lat_met) else float("nan"),
        "prom_p50_s": pq[0.5], "prom_p95_s": pq[0.95], "prom_p99_s": pq[0.99],
        "lag_max_msgs": lag_max, "lag_fim_janela_msgs": lag_fim_janela,
        "n_microlotes": len(pg),
        "inputRowsPerSecond_media": float(pg["inputRowsPerSecond"].mean()) if len(pg) else float("nan"),
        "processedRowsPerSecond_media": float(pg["processedRowsPerSecond"].mean()) if len(pg) else float("nan"),
        "duracao_microlote_ms_media": float(pg["triggerExecution_ms"].mean()) if len(pg) else float("nan"),
        "t_inicio": r["t_inicio"], "t_fim": r["t_fim"],
    }
    return res, pg, df


def append_csv(path, row):
    novo = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if novo:
            w.writeheader()
        w.writerow(row)
