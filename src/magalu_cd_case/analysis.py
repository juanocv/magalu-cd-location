
from .utils import load_features, weighted_average_time, coverage_share, percentiles

def summary_metrics(features_csv: str):
    df = load_features(features_csv)
    rec_wavg = weighted_average_time(df, "time_h_from_recife")
    ssa_wavg = weighted_average_time(df, "time_h_from_salvador")
    rec_p = percentiles(df["time_h_from_recife"].values, [50,90])
    ssa_p = percentiles(df["time_h_from_salvador"].values, [50,90])
    cov = {
        "rec_le12": coverage_share(df, "covered_recife_le_12h"),
        "ssa_le12": coverage_share(df, "covered_salvador_le_12h"),
        "rec_le24": coverage_share(df, "covered_recife_le_24h"),
        "ssa_le24": coverage_share(df, "covered_salvador_le_24h"),
    }
    return {
        "weighted_avg_time": {"recife_h": rec_wavg, "salvador_h": ssa_wavg},
        "percentiles": {"recife": rec_p, "salvador": ssa_p},
        "coverage": cov,
    }
