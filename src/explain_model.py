import warnings
warnings.filterwarnings("ignore")

import os
import sys
import joblib
import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config

cfg = Config()

from sklearn.model_selection import train_test_split
from sklearn.inspection import permutation_importance

MODEL_PATH = os.path.join(cfg.models_dir, "best_model.pkl")
SCALER_PATH = os.path.join(cfg.models_dir, "scaler.pkl")
OUTPUT_PLOT = "Resultado/feature_importance.png"


def load_model_and_test_set(n_samples: int = 30000):
    df = pd.read_csv(cfg.raw_data_path)

    X = df.drop(columns=cfg.features_to_drop)
    y = df[cfg.target_column]

    X_keep = df.sample(n=n_samples, random_state=42).drop(columns=cfg.features_to_drop)
    y_keep = y.loc[X_keep.index]

    X_train, X_test, y_train, y_test = train_test_split(
        X_keep, y_keep, test_size=0.3, random_state=cfg.random_state, stratify=y_keep
    )

    scaler = joblib.load(SCALER_PATH)
    X_test = X_test.copy()
    X_test[cfg.scale_cols] = scaler.transform(X_test[cfg.scale_cols])
    return X_test, y_test


def main():
    print("=" * 60)
    print("EXPLICABILIDADE - Por que o modelo diz FRAUDE?")
    print("=" * 60)

    model = joblib.load(MODEL_PATH)
    X_test, y_test = load_model_and_test_set()

    if hasattr(model, "feature_importances_"):
        fi = model.feature_importances_.copy()
        names = list(X_test.columns)
    else:
        fi = None
        names = list(X_test.columns)

    print("\nComputando permutation importance (pode levar alguns segundos)...")
    perm = permutation_importance(
        model, X_test, y_test,
        n_repeats=5, scoring="average_precision",
        random_state=cfg.random_state, n_jobs=-1,
        max_samples=min(10000, len(X_test)),
    )

    imp_df = pd.DataFrame({
        "feature": names,
        "imp_mean": perm.importances_mean,
        "imp_std": perm.importances_std,
    }).sort_values("imp_mean", ascending=False)

    print("\nAs 10 features mais importantes (permutation importance):")
    print(imp_df.head(10).to_string(index=False, float_format=lambda v: f"{v:.4f}"))

    fig, axes = plt.subplots(2, 1, figsize=(10, 10))

    if fi is not None:
        fi_df = pd.DataFrame({"feature": names, "importance": fi})
        fi_df = fi_df.sort_values("importance", ascending=True).tail(15)
        axes[0].barh(fi_df["feature"], fi_df["importance"], color="steelblue")
        axes[0].set_title("Importância embutida do modelo (Gini)")
        axes[0].set_xlabel("Importância")

    perm_top = imp_df.sort_values("imp_mean", ascending=True).tail(15)
    axes[1].barh(perm_top["feature"], perm_top["imp_mean"],
                 xerr=perm_top["imp_std"], color="coral")
    axes[1].set_title("Permutation importance (avg precision)")
    axes[1].set_xlabel("Queda na métrica ao embaralhar a feature")

    plt.tight_layout()
    os.makedirs(os.path.dirname(OUTPUT_PLOT), exist_ok=True)
    plt.savefig(OUTPUT_PLOT, dpi=120)
    print(f"\nGráfico salvo em: {OUTPUT_PLOT}")


if __name__ == "__main__":
    main()