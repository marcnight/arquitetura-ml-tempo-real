"""Item H - capturas de tela (Chrome do sistema via Playwright).

Uso: venv/bin/python medicoes/scripts/H_capturas.py
"""
import os, json, urllib.request
from playwright.sync_api import sync_playwright

OUT = "medicoes/H_capturas"
os.makedirs(OUT, exist_ok=True)
with sync_playwright() as p:
    b = p.chromium.launch(executable_path="/usr/bin/google-chrome-stable", headless=True)
    pg = b.new_page(viewport={"width": 1920, "height": 1200})

    pg.goto("http://localhost:9090/targets", wait_until="load")
    pg.wait_for_timeout(3000)
    pg.screenshot(path=f"{OUT}/prometheus_targets.png", full_page=True)

    pg.goto("http://localhost:3000/login", wait_until="load")
    pg.fill('input[name="user"]', "admin")
    pg.fill('input[name="password"]', "admin")
    pg.click('button[type="submit"]')
    pg.wait_for_timeout(3000)
    pg.goto("http://localhost:3000/d/fraud-detection/?orgId=1&from=now-15m&to=now&kiosk", wait_until="load")
    pg.wait_for_timeout(8000)
    pg.screenshot(path=f"{OUT}/grafana_dashboard.png", full_page=True)

    pg.goto("http://localhost:5000/", wait_until="load")
    pg.wait_for_timeout(4000)
    pg.screenshot(path=f"{OUT}/mlflow_inicio.png", full_page=True)
    exps = json.load(urllib.request.urlopen(urllib.request.Request(
        "http://localhost:5000/api/2.0/mlflow/experiments/search", data=b'{"max_results": 50}',
        headers={"Content-Type": "application/json"})))["experiments"]
    eid = [e["experiment_id"] for e in exps if e["name"] == "FraudDetectionCreditCard"][0]
    pg.goto(f"http://localhost:5000/#/experiments/{eid}/runs", wait_until="load")
    pg.wait_for_timeout(6000)
    pg.screenshot(path=f"{OUT}/mlflow_experimento.png", full_page=True)

    pg.goto("http://localhost:8080/", wait_until="load")
    pg.wait_for_timeout(3000); pg.screenshot(path=f"{OUT}/spark_master.png", full_page=True)
    b.close()
print(os.listdir(OUT))
