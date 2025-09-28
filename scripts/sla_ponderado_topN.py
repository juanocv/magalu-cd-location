#!/usr/bin/env python3
"""
Recife vs Salvador — SLA ponderado por municípios (Top N)
---------------------------------------------------------

Gera tempos (horas) de Recife e Salvador até os Top N municípios do NE
(com maior demand_weight) e calcula:
- média ponderada
- percentis ponderados 50/80/90

Entradas:
  - data/interim/ibge/municipios_NE_2022.gpkg         (campos: CD_MUN, NM_MUN, geometry)
  - data/processed/ibge/consumo_municipal_NE_2021.csv (campos: code_muni, demand_weight, ...)

Saídas:
  - data/processed/osrm/od_municipios_topN_recife_salvador.csv
      code_muni, nome_muni, sigla, lon, lat, demand_weight, dur_h_Recife-PE, dur_h_Salvador-BA
  - data/processed/osrm/sla_ponderado_topN_summary.csv
      origem, N, tempo_medio_ponderado_h, p50_h, p80_h, p90_h

Requisitos:
  - requests, pandas, geopandas (para ler GPKG), shapely
  - OSRM rodando em http://localhost:5000 (veja docker-compose da etapa anterior)
"""

import argparse
from pathlib import Path
import math
import requests
import pandas as pd
import geopandas as gpd

# ---------- Defaults ----------
OSRM_URL_DEFAULT = "http://localhost:5000"
GPK_MUN_DEFAULT  = "data/interim/ibge/municipios_NE_2022.gpkg"
CSV_SCORE_DEFAULT= "data/processed/ibge/consumo_municipal_NE_2021.csv"

ORIGENS_DEFAULT = {
    "Recife-PE":   (-34.8770, -8.0476),   # (lon, lat)
    "Salvador-BA": (-38.5014, -12.9714),
}

DEST_CHUNK = 100  # nº de destinos por chamada ao /table

# ---------- Utils ----------
def osrm_table(url_base, coords, sources_idx, destinations_idx, annotations="duration"):
    coord_str = ";".join([f"{lon},{lat}" for lon, lat in coords])
    params = {
        "sources": ";".join(map(str, sources_idx)),
        "destinations": ";".join(map(str, destinations_idx)),
        "annotations": annotations,
    }
    url = f"{url_base}/table/v1/driving/{coord_str}"
    r = requests.get(url, params=params, timeout=120)
    r.raise_for_status()
    return r.json()

def weighted_percentiles(values, weights, ps=(0.5, 0.8, 0.9)):
    """Percentis ponderados numéricos; values/weights arrays 1D."""
    df = pd.DataFrame({"v": values, "w": weights}).dropna()
    if df.empty or df["w"].sum() <= 0:
        return {p: math.nan for p in ps}
    df = df.sort_values("v")
    cumw = df["w"].cumsum() / df["w"].sum()
    out = {}
    for p in ps:
        out[p] = float(df.loc[(cumw >= p), "v"].iloc[0]) if (cumw >= p).any() else float(df["v"].iloc[-1])
    return out

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--osrm", default=OSRM_URL_DEFAULT, help="URL base do OSRM (default: http://localhost:5000)")
    ap.add_argument("--gpk",  default=GPK_MUN_DEFAULT,   help="Caminho do GPKG dos municípios NE")
    ap.add_argument("--score",default=CSV_SCORE_DEFAULT, help="CSV de consumo municipal (com demand_weight)")
    ap.add_argument("--N", type=int, default=500,        help="Top N municípios por demand_weight")
    ap.add_argument("--chunk", type=int, default=DEST_CHUNK, help="Tamanho do lote de destinos por chamada /table")
    args = ap.parse_args()

    osrm_url = args.osrm
    gpk_path = Path(args.gpk)
    score_path = Path(args.score)
    N = args.N
    chunk = args.chunk

    out_dir = Path("data/processed/osrm")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_od = out_dir / "od_municipios_topN_recife_salvador.csv"
    out_sum = out_dir / "sla_ponderado_topN_summary.csv"

    # 1) carrega geometrias + score
    gdf = gpd.read_file(gpk_path)
    if "CD_MUN" not in gdf.columns:
        raise RuntimeError("Campo 'CD_MUN' não encontrado no GPKG.")

    gdf["CD_MUN"] = gdf["CD_MUN"].astype(str).str.replace(r"\D","", regex=True).str.zfill(7)
    gdf["centroid"] = gdf.geometry.centroid
    gdf["lon"] = gdf["centroid"].x
    gdf["lat"] = gdf["centroid"].y

    df = pd.read_csv(score_path)
    df["code_muni"] = df["code_muni"].astype(str).str.replace(r"\D","", regex=True).str.zfill(7)

    merged = gdf.merge(
        df[["code_muni","nome_muni","sigla","demand_weight"]],
        left_on="CD_MUN", right_on="code_muni", how="left"
    )
    # ordena por peso e pega top N
    top = merged.sort_values("demand_weight", ascending=False).head(N).copy()
    # renormaliza os pesos no subconjunto
    wsum = top["demand_weight"].sum(skipna=True)
    top["w_norm"] = top["demand_weight"] / wsum if wsum and wsum > 0 else 0.0

    # 2) prepara OSRM /table
    orig_labels = list(ORIGENS_DEFAULT.keys())
    orig_coords = [ORIGENS_DEFAULT[k] for k in orig_labels]  # [(lon,lat), ...]
    dest_coords = list(zip(top["lon"].tolist(), top["lat"].tolist()))

    # Índices no vetor all_points
    all_points = orig_coords + dest_coords
    sources_idx = list(range(len(orig_coords)))  # 0..n_origens-1
    # faremos destinos em chunks relativos ao vetor all_points

    # 3) chama OSRM em chunks (apenas durations)
    dur_h = {lbl: [] for lbl in orig_labels}  # cada origem terá lista de durações (horas) alinhada com top
    # preenche com NaN
    for lbl in orig_labels:
        dur_h[lbl] = [math.nan] * len(dest_coords)

    import itertools
    for start in range(0, len(dest_coords), chunk):
        end = min(start + chunk, len(dest_coords))
        # destinos globais: deslocar por len(orig_coords)
        dest_idx_global = list(range(len(orig_coords) + start, len(orig_coords) + end))
        resp = osrm_table(osrm_url, all_points, sources_idx, dest_idx_global, annotations="duration")

        durations = resp.get("durations", [])
        # durations tem shape [n_sources x n_destinos_chunk]
        for si, lbl in enumerate(orig_labels):
            for local_j, dj in enumerate(range(start, end)):
                sec = durations[si][local_j]
                dur_h[lbl][dj] = (sec / 3600.0) if sec is not None else math.nan

    # 4) monta dataframe OD por município
    df_od = pd.DataFrame({
        "code_muni": top["CD_MUN"].values,
        "nome_muni": top["NM_MUN"].values,
        "sigla":     top["sigla"].values,
        "lon":       top["lon"].values,
        "lat":       top["lat"].values,
        "demand_weight": top["demand_weight"].values,
        "w_norm":         top["w_norm"].values,
    })
    for lbl in orig_labels:
        df_od[f"dur_h_{lbl}"] = dur_h[lbl]

    df_od.to_csv(out_od, index=False)
    print("OK ->", out_od)

    # 5) resumo: média ponderada + percentis ponderados
    rows = []
    for lbl in orig_labels:
        vals = df_od[f"dur_h_{lbl}"].to_numpy()
        ws   = df_od["w_norm"].to_numpy()
        # média ponderada
        mean_h = float((pd.Series(vals) * pd.Series(ws)).sum(skipna=True))
        # percentis ponderados
        p = weighted_percentiles(vals, ws, ps=(0.5, 0.8, 0.9))
        rows.append({
            "origem": lbl,
            "N": N,
            "tempo_medio_ponderado_h": mean_h,
            "p50_h": p[0.5],
            "p80_h": p[0.8],
            "p90_h": p[0.9],
        })

    pd.DataFrame(rows).to_csv(out_sum, index=False)
    print("OK ->", out_sum)

if __name__ == "__main__":
    main()
