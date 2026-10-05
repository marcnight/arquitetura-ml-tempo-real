#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate

python3 -c "
import sys, time, signal
sys.path.insert(0, 'src')
from inference_pipeline import FraudDetectionPipeline
from metrics_exporter import metrics
import pandas as pd

# Ignore Ctrl+C in background
signal.signal(signal.SIGINT, signal.SIG_IGN)

df = pd.read_csv('base_fraudes/creditcard.csv')
pipeline = FraudDetectionPipeline(enable_metrics=True)
metrics.start()

df_shuffled = df.sample(frac=1, random_state=42).reset_index(drop=True)
n = len(df_shuffled)
i = 0
batch_size = 100

print('Stream contínuo iniciado. Métricas em http://localhost:8000/metrics')
print('Pressione Ctrl+C para parar.')

while True:
    batch = df_shuffled.iloc[i:i + batch_size]
    if len(batch) == 0:
        i = 0
        continue

    t0 = time.time()
    results = pipeline.predict_batch(batch)
    elapsed = time.time() - t0

    frauds = results[results['prediction'] == 1]
    ts = time.strftime('%H:%M:%S')
    print(f'[{ts}] Batch {i//batch_size + 1}: {len(batch)} trans | {len(frauds)} fraudes | {elapsed*1000:.0f}ms')

    for _, res in frauds.iterrows():
        metrics.record_prediction(
            prediction=res['prediction'],
            probability=res['fraud_probability'],
            amount=res['Amount'],
            latency=elapsed / len(batch)
        )
    metrics.set_stream_lag(n - i)
    i = (i + batch_size) % n
    time.sleep(1)
" > /tmp/metrics_stream.log 2>&1 &

echo "Stream PID: $!"
echo "Log: /tmp/metrics_stream.log"
echo "Métricas: http://localhost:8000/metrics"
