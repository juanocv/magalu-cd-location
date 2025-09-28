
import pandas as pd
import numpy as np

def load_features(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def weighted_average_time(df: pd.DataFrame, column: str, weight_col: str = "demand_weight") -> float:
    w = df[weight_col].values
    x = df[column].values
    return float(np.average(x, weights=w))

def coverage_share(df: pd.DataFrame, col_mask: str, weight_col: str = "demand_weight") -> float:
    w = df[weight_col].values
    m = df[col_mask].values
    num = float((w * m).sum())
    den = float(w.sum())
    return num/den if den>0 else 0.0

def percentiles(values, q=[50,90]):
    arr = np.asarray(values)
    return {"p50": float(np.percentile(arr, q[0])), "p90": float(np.percentile(arr, q[1]))}
