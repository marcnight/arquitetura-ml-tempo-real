import warnings
warnings.filterwarnings("ignore")

import os
import sys
import joblib
import numpy as np
import pandas as pd
from time import time

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import (
    classification_report, confusion_matrix,
    roc_auc_score, average_precision_score,
    precision_recall_curve, f1_score, roc_curve
)
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as ImbPipeline

import mlflow
import mlflow.sklearn

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config

cfg = Config()

os.makedirs(cfg.models_dir, exist_ok=True)
os.makedirs(cfg.data_dir, exist_ok=True)



def load_data():
    df = pd.read_csv(cfg.raw_data_path)
    print(f"Dados carregados: {df.shape}")
    print(f"Fraudes: {df[cfg.target_column].sum()} / {len(df)} ({df[cfg.target_column].mean()*100:.4f}%)")
    return df


def prepare_features(df):
    X = df.drop(columns=cfg.features_to_drop)
    y = df[cfg.target_column]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=cfg.test_size, random_state=cfg.random_state, stratify=y
    )

    scaler = StandardScaler()
    X_train[cfg.scale_cols] = scaler.fit_transform(X_train[cfg.scale_cols])
    X_test[cfg.scale_cols] = scaler.transform(X_test[cfg.scale_cols])

    return X_train, X_test, y_train, y_test, scaler


def train_with_gridsearch(X_train, y_train, model_name="rf"):
    if model_name == "lr":
        model = LogisticRegression(random_state=cfg.random_state, max_iter=1000)
        param_grid = {
            "model__C": [0.01, 0.1, 1.0, 10.0],
            "model__class_weight": [None, "balanced"],
            "model__solver": ["liblinear", "lbfgs"],
        }
    elif model_name == "dt":
        model = DecisionTreeClassifier(random_state=cfg.random_state)
        param_grid = {
            "model__max_depth": [5, 10, 15, None],
            "model__min_samples_split": [2, 10, 50],
            "model__class_weight": [None, "balanced"],
        }
    elif model_name == "rf":
        model = RandomForestClassifier(random_state=cfg.random_state, n_jobs=-1)
        param_grid = {
            "model__n_estimators": [100, 200],
            "model__max_depth": [10, 20],
            "model__class_weight": ["balanced", "balanced_subsample"],
        }

    pipeline = ImbPipeline([
        ("sampling", SMOTE(random_state=cfg.random_state, sampling_strategy=0.1)),
        ("model", model),
    ])

    grid = GridSearchCV(
        pipeline, param_grid, cv=3,
        scoring="average_precision", n_jobs=-1, verbose=1
    )
    grid.fit(X_train, y_train)

    return grid.best_estimator_, grid.best_params_, grid.best_score_


def evaluate_model(model, X_test, y_test, model_name):
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    results = {
        "model_name": model_name,
        "roc_auc": roc_auc_score(y_test, y_proba),
        "avg_precision": average_precision_score(y_test, y_proba),
        "f1_score": f1_score(y_test, y_pred),
    }

    print(f"\n=== {model_name} ===")
    print(f"ROC-AUC: {results['roc_auc']:.4f}")
    print(f"Avg Precision: {results['avg_precision']:.4f}")
    print(f"F1-Score: {results['f1_score']:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=["Normal", "Fraude"]))
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    return results, y_pred, y_proba


def main():
    print("=" * 60)
    print("TRAINING PIPELINE - Fraude em Cartão de Crédito")
    print("=" * 60)

    df = load_data()
    X_train, X_test, y_train, y_test, scaler = prepare_features(df)
    print(f"\nTreino: {X_train.shape}, Teste: {X_test.shape}")
    print(f"Fraudes treino: {y_train.sum()}, Fraudes teste: {y_test.sum()}")

    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    mlflow.set_experiment(cfg.mlflow_experiment_name)

    models_to_try = ["lr", "dt", "rf"]
    best_models = {}
    all_results = []

    for model_name in models_to_try:
        print(f"\n{'-'*40}")
        print(f"Treinando: {model_name.upper()}")
        print(f"{'-'*40}")

        with mlflow.start_run(run_name=model_name.upper()):
            mlflow.log_params({"model_type": model_name})

            t0 = time()
            best_model, best_params, best_score = train_with_gridsearch(
                X_train, y_train, model_name
            )
            train_time = time() - t0

            mlflow.log_params(best_params)
            mlflow.log_metric("cv_avg_precision", best_score)
            mlflow.log_metric("train_time_sec", train_time)

            results, y_pred, y_proba = evaluate_model(
                best_model, X_test, y_test, model_name
            )

            for metric_name, metric_value in results.items():
                if metric_name != "model_name":
                    mlflow.log_metric(metric_name, metric_value)

            mlflow.sklearn.log_model(best_model, artifact_path="model")

            model_path = os.path.join(cfg.models_dir, f"{model_name}_best.pkl")
            joblib.dump(best_model, model_path)
            print(f"Modelo salvo em: {model_path}")

            best_models[model_name] = best_model
            all_results.append(results)

    results_df = pd.DataFrame(all_results).set_index("model_name")
    print("\n" + "=" * 60)
    print("RESUMO COMPARATIVO")
    print("=" * 60)
    print(results_df.to_string())

    results_df.to_csv("data/model_comparison.csv")
    print("\nResultados salvos em: data/model_comparison.csv")

    best_model_name = results_df["avg_precision"].idxmax()
    best_model_obj = best_models[best_model_name]

    final_path = os.path.join(cfg.models_dir, "best_model.pkl")
    joblib.dump(best_model_obj, final_path)
    print(f"\nMelhor modelo: {best_model_name} -> salvo em {final_path}")

    scaler_path = os.path.join(cfg.models_dir, "scaler.pkl")
    joblib.dump(scaler, scaler_path)
    print(f"Scaler salvo em: {scaler_path}")


if __name__ == "__main__":
    main()
