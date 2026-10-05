import warnings
warnings.filterwarnings("ignore")

import os
import sys
import joblib
import numpy as np
import pandas as pd
from datetime import date

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config

cfg = Config()

from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix

MODEL_PATH = os.path.join(cfg.models_dir, "best_model.pkl")
SCALER_PATH = os.path.join(cfg.models_dir, "scaler.pkl")


def load_model_and_test_set():
    df = pd.read_csv(cfg.raw_data_path)
    X = df.drop(columns=cfg.features_to_drop)
    y = df[cfg.target_column]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=cfg.test_size, random_state=cfg.random_state, stratify=y
    )

    scaler = joblib.load(SCALER_PATH)
    X_test_raw = X_test[["Amount", "Time"]].copy()
    X_test = X_test.copy()
    X_test[cfg.scale_cols] = scaler.transform(X_test[cfg.scale_cols])
    return X_test, y_test, X_test_raw


def analyze(cost_fp: float = 5.00, cost_fn_ratio: float = 0.50):
    print("=" * 60)
    print("ANÁLISE DE CUSTO - Fraude em Cartão de Crédito")
    print("=" * 60)

    model = joblib.load(MODEL_PATH)
    X_test, y_test, X_test_raw = load_model_and_test_set()

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    amounts = X_test_raw["Amount"].values

    tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()

    cost_fp_total = fp * cost_fp
    fn_amounts = amounts[(y_test == 1) & (y_pred == 0)]
    cost_fn_total = float(fn_amounts.sum()) * cost_fn_ratio

    total_expected_cost = cost_fp_total + cost_fn_total

    n_transactions_day = 1_000_000
    fraud_rate = float(y_test.mean())
    fraud_daily_base = n_transactions_day * fraud_rate
    anomaly_daily = n_transactions_day * (fp / (fp + tn + 1e-9))
    fn_share = fn / (fn + tp + 1e-9)
    fn_daily = fraud_daily_base * fn_share
    avg_fraud_amount = float(amounts[y_test == 1].mean())
    daily_fp_cost = anomaly_daily * cost_fp
    daily_fn_cost = fn_daily * avg_fraud_amount * cost_fn_ratio
    daily_expected_cost = daily_fp_cost + daily_fn_cost

    print(f"\nModelo: {type(model).__name__}")
    print(f"Test set: {len(y_test)} transações ({y_test.sum()} fraudes reais)")
    print(f"\nMatriz de Confusão")
    print(f"  TN = {tn:,}  FP = {fp:,}")
    print(f"  FN = {fn:,}  TP = {tp:,}")

    print(f"\nPREMIÇAS DE CUSTO")
    print(f"  Custo por falso positivo (revisão manual): R$ {cost_fp:.2f}")
    print(f"  Custo por fraude não detectada (chargeback): {cost_fn_ratio*100:.0f}% do valor")

    print(f"\nCUSTO NO TEST SET ({len(y_test):,} transações)")
    print(f"  Falsos positivos: {fp} x R$ {cost_fp:.2f} = R$ {cost_fp_total:,.2f}")
    print(f"  Fraudes perdidas: R$ {cost_fn_total:,.2f} ({cost_fn_ratio*100:.0f}% de R$ {fn_amounts.sum():,.2f} em valores não bloqueados)")
    print(f"  Custo esperado total: R$ {total_expected_cost:,.2f}")

    print(f"\nPROJEÇÃO DIÁRIA (hipótese: {n_transactions_day:,} transações/dia)")
    print(f"  Fraudes esperadas/dia: {fraud_daily_base:,.0f}  |  valor médio: R$ {avg_fraud_amount:,.2f}")
    print(f"  Alertas falsos/dia:    {anomaly_daily:,.0f}  -> R$ {daily_fp_cost:,.2f}")
    print(f"  Fraudes não pegas/dia: {fn_daily:,.0f}  -> R$ {daily_fn_cost:,.2f}")
    print(f"  Custo esperado/dia:    R$ {daily_expected_cost:,.2f}")
    print(f"  Custo esperado/mês:    R$ {daily_expected_cost * 30:,.2f}")


if __name__ == "__main__":
    analyze()