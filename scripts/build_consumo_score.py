#!/usr/bin/env python3
from pathlib import Path
import argparse
import pandas as pd
import numpy as np
import re

BASE = Path(__file__).resolve().parents[1]
INTERIM = BASE / "data" / "interim" / "ibge"
PROCESSED = BASE / "data" / "processed" / "ibge"
PROCESSED.mkdir(parents=True, exist_ok=True)

CSV_PIB   = INTERIM / "pib_municipal_2021.csv"
CSV_POPM  = INTERIM / "populacao_municipal_nordeste_2021.csv"
CSV_RENDA = INTERIM / "renda_per_capita_uf_2024.csv"
GPK_MUN   = INTERIM / "municipios_NE_2022.gpkg"

OUT_CONSUMO_CSV = PROCESSED / "consumo_municipal_NE_2021.csv"
OUT_CONSUMO_GPK = PROCESSED / "consumo_municipal_NE_2021.gpkg"

def read_csv_smart(path: Path) -> pd.DataFrame:
    for enc in ("utf-8","latin1","cp1252"):
        for sep in (",",";","|","\t"):
            try:
                return pd.read_csv(path, encoding=enc, sep=sep)
            except Exception:
                continue
    return pd.read_csv(path, encoding="latin1", engine="python")

def to_strz(x, n):
    s = re.sub(r"[^\d]", "", str(x)) if pd.notna(x) else ""
    return s.zfill(n) if s else None

def parse_ptbr_number(s):
    if pd.isna(s): return np.nan
    t = re.sub(r"[^\d,.-]", "", str(s))  # tira R$, espaços etc.
    t = t.replace(".", "").replace(",", ".")
    try: return float(t)
    except: return np.nan

def detect_col(cols, patterns):
    for pat in patterns:
        rgx = re.compile(pat, re.I)
        for c in cols:
            if rgx.search(str(c)): return c
    return None

def build_score(with_geom=False):
    # 1) PIB municipal
    df_pib = read_csv_smart(CSV_PIB)
    assert "code_muni" in df_pib.columns, "pib_municipal_2021.csv precisa ter 'code_muni'"
    df_pib["code_muni"] = df_pib["code_muni"].apply(lambda x: to_strz(x,7))
    if "sigla" in df_pib.columns: df_pib["sigla"] = df_pib["sigla"].astype(str).str.upper().str.strip()
    if "uf" in df_pib.columns:    df_pib["uf"]    = df_pib["uf"].astype(str).str.strip()
    # garante números
    if "pib_pc_2021_brl" in df_pib.columns:    df_pib["pib_pc_2021_brl"]    = df_pib["pib_pc_2021_brl"].apply(parse_ptbr_number)
    if "pib_total_2021_brl" in df_pib.columns: df_pib["pib_total_2021_brl"] = df_pib["pib_total_2021_brl"].apply(parse_ptbr_number)

    # 2) População municipal (reconstruir code_muni 7 dígitos a partir de code_uf + code_muni)
    df_pop = read_csv_smart(CSV_POPM)
    col_code_m  = detect_col(df_pop.columns, [r"^code_muni$","c[oó]d.*mun"])
    col_code_uf = detect_col(df_pop.columns, [r"^code_uf$","c[oó]d.*uf"])
    col_pop     = detect_col(df_pop.columns, [r"pop.*2021","popula","habit"])
    if col_code_m != "code_muni": df_pop = df_pop.rename(columns={col_code_m:"code_muni"})
    if col_pop    != "pop_2021":  df_pop = df_pop.rename(columns={col_pop:"pop_2021"})
    df_pop["code_muni"] = df_pop.apply(lambda r: to_strz(r[col_code_uf],2) + to_strz(r["code_muni"],5), axis=1)
    df_pop["pop_2021"]  = df_pop["pop_2021"].apply(parse_ptbr_number).astype("Int64")

    # 3) Renda per capita por UF (detectar coluna e converter)
    df_renda = read_csv_smart(CSV_RENDA)
    col_sigla = detect_col(df_renda.columns, [r"^sigla$", r"\buf\b.*sigla", r"sigla"])
    col_renda = detect_col(df_renda.columns, [r"renda.*2024", r"per.?capita", r"nominal"])
    if col_sigla != "sigla": df_renda = df_renda.rename(columns={col_sigla:"sigla"})
    if col_renda != "renda_pc_uf_2024_nominal_brl":
        df_renda = df_renda.rename(columns={col_renda:"renda_pc_uf_2024_nominal_brl"})
    df_renda["sigla"] = df_renda["sigla"].astype(str).str.upper().str.strip()
    df_renda["renda_pc_uf_2024_nominal_brl"] = df_renda["renda_pc_uf_2024_nominal_brl"].apply(parse_ptbr_number)

    # 4) Merges
    df = df_pib.merge(df_pop[["code_muni","pop_2021"]], on="code_muni", how="left")
    df = df.merge(df_renda[["sigla","renda_pc_uf_2024_nominal_brl"]], on="sigla", how="left")

    n, n_pop = len(df), int(df["pop_2021"].notna().sum())
    n_renda  = int(df["renda_pc_uf_2024_nominal_brl"].notna().sum())
    print(f"[QC] linhas PIB: {n} | pop preenchida: {n_pop} | renda preenchida: {n_renda}")

    # 5) Ajuste intra-UF pelo PIB per capita
    uf_avg = (df.groupby("sigla", as_index=False)["pib_pc_2021_brl"]
                .mean()
                .rename(columns={"pib_pc_2021_brl":"pib_pc_uf_avg_2021_brl"}))
    df = df.merge(uf_avg, on="sigla", how="left")
    df["adj_pibpc_vs_uf"] = (df["pib_pc_2021_brl"] / df["pib_pc_uf_avg_2021_brl"]).clip(0.5, 2.0)

    # 6) Proxies e score
    df["income_proxy"]     = df["pop_2021"].astype(float) * df["renda_pc_uf_2024_nominal_brl"].astype(float)
    df["income_proxy_adj"] = df["income_proxy"] * df["adj_pibpc_vs_uf"]
    v = df["income_proxy_adj"].to_numpy()
    v_min, v_max = np.nanmin(v), np.nanmax(v); denom = (v_max - v_min + 1e-9)
    df["score_consumo"]  = (df["income_proxy_adj"] - v_min) / denom
    total = np.nansum(df["income_proxy_adj"])
    df["demand_weight"] = df["income_proxy_adj"] / total if total > 0 else np.nan

    out_cols = ["code_muni","nome_muni","sigla","uf","pop_2021","pib_pc_2021_brl",
                "renda_pc_uf_2024_nominal_brl","adj_pibpc_vs_uf","income_proxy_adj",
                "score_consumo","demand_weight"]
    df[out_cols].to_csv(OUT_CONSUMO_CSV, index=False)
    print(f"OK -> {OUT_CONSUMO_CSV}")

    if with_geom:
        try:
            import geopandas as gpd
            gdf = gpd.read_file(GPK_MUN)
            gdf["CD_MUN"] = gdf["CD_MUN"].astype(str).str.replace(r"\D","", regex=True).str.zfill(7)
            dfx = df[out_cols].copy()
            dfx["code_muni"] = dfx["code_muni"].astype(str).str.replace(r"\D","", regex=True).str.zfill(7)
            gdf2 = gdf.merge(dfx, left_on="CD_MUN", right_on="code_muni", how="left")
            gdf2.to_file(OUT_CONSUMO_GPK, layer="consumo_ne", driver="GPKG")
            print(f"OK (mapa) -> {OUT_CONSUMO_GPK} (layer='consumo_ne')")
        except Exception as e:
            print("[Aviso] Join geométrico não gerado:", e)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--with-geom", action="store_true", help="gera também o GPKG temático (se disponível)")
    args = p.parse_args()
    build_score(with_geom=args.with_geom)

if __name__ == "__main__":
    main()
