#!/usr/bin/env python3
import os
os.environ.setdefault("SHAPE_RESTORE_SHX", "YES")

from pathlib import Path
import argparse
import pandas as pd
import numpy as np
import re
import geopandas as gpd
import unicodedata

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snv-csv", required=True)
    ap.add_argument("--shp-bases", default=None)
    ap.add_argument("--shp-rotas", default=None)
    ap.add_argument("--out-gpkg", required=True)
    ap.add_argument("--km-tol", type=float, default=2.0)
    ap.add_argument("--target-crs", default="EPSG:4674")
    return ap.parse_args()

# ---------- helpers ----------
UF_MAP = {
    "AL": "AL", "ALAGOAS": "AL",
    "BA": "BA", "BAHIA": "BA",
    "CE": "CE", "CEARA": "CE",
    "MA": "MA", "MARANHAO": "MA",
    "PB": "PB", "PARAIBA": "PB",
    "PE": "PE", "PERNAMBUCO": "PE",
    "PI": "PI", "PIAUI": "PI",
    "RN": "RN", "RIO GRANDE DO NORTE": "RN",
    "SE": "SE", "SERGIPE": "SE",
    # fallback: se vier “BRASIL” ou algo fora do NE, vira None
}

def strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )

def normalize_uf_any(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = strip_accents(s)
    # normaliza espaços múltiplos
    s = re.sub(r"\s+", " ", s)
    # mapea direto se for sigla ou por extenso
    if s in UF_MAP:
        return UF_MAP[s]
    # às vezes vem "ESTADO: BAHIA" etc.
    s2 = s.replace("ESTADO:", "").strip()
    if s2 in UF_MAP:
        return UF_MAP[s2]
    # tenta extrair sigla entre parênteses: "Bahia (BA)"
    m = re.search(r"\((AM|PA|AC|AP|RO|RR|TO|MA|PI|CE|RN|PB|PE|AL|SE|BA|MG|ES|RJ|SP|PR|SC|RS|MT|MS|GO|DF)\)", s)
    if m:
        return m.group(1)
    # por último, se já tem 2 letras e parece UF, aceita
    if re.fullmatch(r"[A-Z]{2}", s):
        return s
    return None

def to_num_ptbr(x):
    if pd.isna(x): return np.nan
    s = str(x).replace("\xa0"," ").strip()
    s = re.sub(r"[^\d,.-]", "", s)
    s = s.replace(".", "").replace(",", ".")
    try: return float(s)
    except: return np.nan

def ensure_br_pad_from_any(series):
    s = series.astype(str)
    num = s.str.extract(r"(\d{2,3})")[0]
    return np.where(num.notna(), "BR-"+num, None)

def detect_col(cols, patterns):
    for pat in patterns:
        m = [c for c in cols if re.search(pat, str(c), flags=re.I)]
        if m: return m[0]
    return None

def detect_uf_col(cols):
    return detect_col(cols, [r"^UF$", r"\bUF\b", r"SG[_ ]?UF", r"SIGLA[_ ]?UF", r"\bESTADO\b"])

def detect_br_col(cols):
    return detect_col(cols, [r"^BR$", r"\bRODOV", r"\bRODOVIA\b", r"\bBR_NUM"])

def detect_km_ini_col(cols):
    return detect_col(cols, [r"^KM_INI$", r"KM[_ ]?INICIO", r"KM_BEGIN", r"KM_INIC"])

def detect_km_fim_col(cols):
    return detect_col(cols, [r"^KM_FIM$", r"KM[_ ]?FINAL", r"KM_END", r"KM_FINAL"])

def normalize_snv_df(df):
    req = ["br_pad","uf","km_ini","km_fim","ext_km"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise RuntimeError(f"SNV CSV sem colunas obrigatórias: {missing}")
    out = df.copy()
    out["BR_PAD"] = out["br_pad"]
    out["UF"] = out["uf"].apply(normalize_uf_any)   # <—— normalização robusta
    out["KM_INI_SNV"] = out["km_ini"].astype(float)
    out["KM_FIM_SNV"] = out["km_fim"].astype(float)
    out["KM_INI_ARRED_SNV"] = np.floor(out["KM_INI_SNV"]).astype("Int64")
    return out

def normalize_gdf(gdf, target_crs):
    if gdf.crs is None:
        gdf = gdf.set_crs(target_crs, allow_override=True)
    else:
        gdf = gdf.to_crs(target_crs)
    return gdf

def normalize_shp_attrs(gdf):
    cols = list(gdf.columns)
    uf_col = detect_uf_col(cols)
    if uf_col:
        gdf["UF"] = gdf[uf_col].apply(normalize_uf_any)   # <—— normalização robusta
    else:
        gdf["UF"] = None

    br_col = detect_br_col(cols)
    if br_col:
        gdf["BR_PAD"] = ensure_br_pad_from_any(gdf[br_col])
    else:
        gdf["BR_PAD"] = None

    km_ini_col = detect_km_ini_col(cols)
    km_fim_col = detect_km_fim_col(cols)
    gdf["KM_INI_SHP"] = gdf[km_ini_col].apply(to_num_ptbr) if km_ini_col else np.nan
    gdf["KM_FIM_SHP"] = gdf[km_fim_col].apply(to_num_ptbr) if km_fim_col else np.nan
    gdf["KM_INI_ARRED_SHP"] = np.floor(gdf["KM_INI_SHP"].astype(float)).astype("Int64")
    return gdf

def km_delta_interval(km_ini_snv, km_ini_shp, km_fim_shp):
    a = km_ini_snv
    b = min(km_ini_shp, km_fim_shp)
    c = max(km_ini_shp, km_fim_shp)
    if np.isnan(a) or np.isnan(b) or np.isnan(c):
        return np.nan
    if a < b: return b - a
    if a > c: return a - c
    return 0.0

def best_km_match(merged, km_tol):
    df = merged.copy()
    df["km_delta_int"] = np.vectorize(km_delta_interval)(
        df["KM_INI_SNV"], df["KM_INI_SHP"], df["KM_FIM_SHP"]
    )
    df["km_delta"] = np.where(
        df["km_delta_int"].notna(), df["km_delta_int"],
        np.abs(df["KM_INI_SNV"] - df["KM_INI_SHP"])
    )
    df["km_within_tol"] = df["km_delta"] <= km_tol

    if "__row_id_snv" not in df.columns:
        df["__row_id_snv"] = pd.factorize(list(zip(df["BR_PAD"], df["UF"], df["KM_INI_SNV"], df["KM_FIM_SNV"])))[0]

    df["_km_for_sort"] = df["km_delta"].fillna(np.inf)
    idx = df.sort_values(["__row_id_snv","_km_for_sort"]).groupby("__row_id_snv").head(1).index
    best = df.loc[idx].copy()

    best["join_score"] = np.select(
        [
            best["BR_PAD"].notna() & best["UF"].notna() & best["km_within_tol"],
            best["BR_PAD"].notna() & best["UF"].notna()
        ],
        [2, 1],
        default=0
    )
    return best

def run_join(snv_csv, shp_path, km_tol, target_crs, layer_name):
    snv = pd.read_csv(snv_csv)
    snv_norm = normalize_snv_df(snv)

    gdf = gpd.read_file(shp_path)
    gdf = normalize_gdf(gdf, target_crs)
    gdf = normalize_shp_attrs(gdf)

    keep_plan = [c for c in ["br","br_pad","uf","km_ini","km_fim","ext_km","situacao","pista","classe","sentido","jurisdicao","concessao","data_ref"] if c in snv_norm.columns]

    left = snv_norm.reset_index(drop=True).copy()
    left["__row_id_snv"] = left.index
    merged = left.merge(gdf, how="left", on=["BR_PAD","UF"], suffixes=("_snv","_shp"))

    if ("geometry" not in merged.columns) or merged["geometry"].isna().all():
        return None, merged, gdf  # ainda sem match com geometria

    best = best_km_match(merged, km_tol)

    out_cols = keep_plan + [
        "BR_PAD","UF",
        "KM_INI_SNV","KM_FIM_SNV",
        "KM_INI_SHP","KM_FIM_SHP",
        "km_delta","km_within_tol","join_score",
        "geometry"
    ]
    out_cols = [c for c in out_cols if c in best.columns] + ["geometry"]
    out = gpd.GeoDataFrame(best[out_cols], geometry="geometry", crs=gdf.crs)
    return out, merged, gdf

def save_outputs(layer_name, out_gpkg, gdf_best, df_full, out_dir, diag_prefix):
    diag_csv = out_dir / f"diag_join_{diag_prefix}.csv"
    df_full.drop(columns=["geometry"], errors="ignore").to_csv(diag_csv, index=False)

    if isinstance(gdf_best, gpd.GeoDataFrame) and not gdf_best.empty:
        gdf_best.to_file(out_gpkg, layer=f"snv_{diag_prefix}_join", driver="GPKG")
        unmatched = gdf_best[gdf_best["join_score"]==0].drop(columns=["geometry"], errors="ignore")
        unmatched_csv = out_dir / f"unmatched_{diag_prefix}.csv"
        unmatched.to_csv(unmatched_csv, index=False)
        print(f"  -> GPKG layer: snv_{diag_prefix}_join")
    else:
        unmatched_csv = out_dir / f"unmatched_{diag_prefix}.csv"
        base_cols = [c for c in df_full.columns if c != "geometry"]
        df_full[base_cols].to_csv(unmatched_csv, index=False)
        print(f"  -> Nenhuma feição casou; salvei CSVs para diagnóstico.")

    print("  -> CSV diag:", diag_csv)
    print("  -> CSV unmatched:", unmatched_csv)

def main():
    args = parse_args()
    out_gpkg = Path(args.out_gpkg)
    out_gpkg.parent.mkdir(parents=True, exist_ok=True)
    out_dir = out_gpkg.parent

    # BASES
    if args.shp_bases:
        print("[BASES] processando…")
        gdf_best_bases, df_full_bases, gdf_bases = run_join(
            args.snv_csv, args.shp_bases, args.km_tol, args.target_crs, "bases"
        )
        save_outputs("bases", out_gpkg, gdf_best_bases, df_full_bases, out_dir, "bases")
    else:
        print("[BASES] shapefile não fornecido, pulando.")

    # ROTAS
    if args.shp_rotas:
        print("[ROTAS] processando…")
        gdf_best_rotas, df_full_rotas, gdf_rotas = run_join(
            args.snv_csv, args.shp_rotas, args.km_tol, args.target_crs, "rotas"
        )
        save_outputs("rotas", out_gpkg, gdf_best_rotas, df_full_rotas, out_dir, "rotas")
    else:
        print("[ROTAS] shapefile não fornecido, pulando.")

    print("\nOK ->", out_gpkg)

if __name__ == "__main__":
    main()
