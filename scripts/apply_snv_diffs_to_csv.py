#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np
import re
import unicodedata

ATTRS = ["situacao","pista","classe","sentido","administracao","jurisdicao","concessao"]

UF_MAP = {
    "AL":"AL","ALAGOAS":"AL",
    "BA":"BA","BAHIA":"BA",
    "CE":"CE","CEARA":"CE",
    "MA":"MA","MARANHAO":"MA",
    "PB":"PB","PARAIBA":"PB",
    "PE":"PE","PERNAMBUCO":"PE",
    "PI":"PI","PIAUI":"PI",
    "RN":"RN","RIO GRANDE DO NORTE":"RN",
    "SE":"SE","SERGIPE":"SE",
}

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def normalize_uf_any(x):
    if pd.isna(x): return None
    s = strip_accents(str(x).strip().upper())
    s = re.sub(r"\s+"," ", s)
    if s in UF_MAP: return UF_MAP[s]
    s2 = s.replace("ESTADO:", "").strip()
    if s2 in UF_MAP: return UF_MAP[s2]
    m = re.search(r"\((AM|PA|AC|AP|RO|RR|TO|MA|PI|CE|RN|PB|PE|AL|SE|BA|MG|ES|RJ|SP|PR|SC|RS|MT|MS|GO|DF)\)", s)
    if m: return m.group(1)
    if re.fullmatch(r"[A-Z]{2}", s): return s
    return None

def ensure_br_pad_from_any(series):
    s = series.astype(str)
    num = s.str.extract(r"(\d{2,3})")[0]
    return np.where(num.notna(), "BR-" + num, None)

def to_float(x):
    try: return float(x)
    except: return np.nan

def normalize_cols(df, src: str):
    """padroniza nomes e adiciona br_pad/uf/km_ini/km_fim quando possível."""
    out = df.copy()
    # nomes sem espaços e lower
    out.columns = [re.sub(r"\s+","_", str(c).strip().lower()) for c in out.columns]
    # UF
    uf_candidates = [c for c in out.columns if c in ("uf","sg_uf","sigla_uf","estado")]
    if uf_candidates:
        out["uf_norm"] = out[uf_candidates[0]].apply(normalize_uf_any)
    else:
        out["uf_norm"] = None
    # BR
    br_candidates = [c for c in out.columns if c in ("br","vl_br","rodovia_br","no_rodovia","rodovia")]
    if br_candidates:
        out["br_pad_norm"] = ensure_br_pad_from_any(out[br_candidates[0]])
    else:
        out["br_pad_norm"] = None
    # KMs — CSV consolidado: km_ini/km_fim; diff: vl_km_inic/vl_km_fina
    if src == "csv":
        ki = out.get("km_ini", out.get("km_inicial"))
        kf = out.get("km_fim", out.get("km_final"))
    else:
        ki = out.get("vl_km_inic", out.get("km_inic"))
        kf = out.get("vl_km_fina", out.get("km_fim"))
    out["km_ini_norm"] = ki.astype(float) if ki is not None else np.nan
    out["km_fim_norm"] = kf.astype(float) if kf is not None else np.nan
    return out

def list_layers(path):
    try:
        import fiona
        with fiona.Env():
            with fiona.open(path) as src:
                return src.listlayers()
    except Exception:
        import fiona
        return fiona.listlayers(path)

def choose_key(df_cols, preferred=None):
    keys = [k for k in [preferred, "id_trecho", "id_trecho_", "cod", "vl_codigo", "codigo", "id"] if k and k in df_cols]
    return keys[0] if keys else None

def km_delta_interval(a, b, c):
    """distância de a ao intervalo [b,c] (0 se dentro)."""
    if np.isnan(a) or np.isnan(b) or np.isnan(c): return np.nan
    lo, hi = min(b,c), max(b,c)
    if a < lo: return lo - a
    if a > hi: return a - hi
    return 0.0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-in", required=True, help="CSV consolidado (ex.: snv_trechos_NE_2025-07.csv)")
    ap.add_argument("--gpkg-diff", required=True, help="GeoPackage de diferenças (NE filtrado ou completo)")
    ap.add_argument("--csv-out", required=True, help="CSV atualizado com atributos dos diffs quando houver")
    ap.add_argument("--gpkg-out", required=True, help="GPKG com geometria dos trechos casados (quando houver)")
    ap.add_argument("--csv-key", default=None, help="Forçar chave no CSV (ex.: id_trecho)")
    ap.add_argument("--diff-key", default=None, help="Forçar chave no diff (ex.: vl_codigo)")
    ap.add_argument("--km-tol", type=float, default=2.0, help="tolerância (km) no fallback por intervalo")
    args = ap.parse_args()

    # 1) CSV consolidado
    df_csv = pd.read_csv(args.csv_in)
    df_csv_norm = normalize_cols(df_csv, "csv")
    csv_key = choose_key(df_csv_norm.columns, args.csv_key)
    if csv_key is None:
        raise RuntimeError("Não encontrei chave no CSV (tente --csv-key=id_trecho).")
    df_csv_norm[csv_key] = df_csv_norm[csv_key].astype(str)

    # 2) GPKG diffs
    layers = list_layers(args.gpkg_diff)
    print("[INFO] layers no GPKG:", layers)

    updated = df_csv_norm.copy()
    matched_geoms = []

    for lay in layers:
        gdf = gpd.read_file(args.gpkg_diff, layer=lay)
        gdf = gdf.rename(columns={c: re.sub(r"\s+","_", str(c).strip().lower()) for c in gdf.columns})
        diff_key = choose_key(gdf.columns, args.diff_key)  # vl_codigo / id_trecho / cod...
        print(f"[LAYER {lay}] chave detectada no diff: {diff_key}")

        # 2a) Tente merge direto por chave
        merged = None
        if diff_key is not None:
            sub = gdf[[diff_key] + [c for c in ATTRS if c in gdf.columns] + (["geometry"] if "geometry" in gdf.columns else [])].copy()
            sub[diff_key] = sub[diff_key].astype(str)
            merged = updated.merge(sub.drop(columns=["geometry"], errors="ignore"),
                                   how="left", left_on=csv_key, right_on=diff_key, suffixes=("","_diff"))

            # atualiza attrs
            for a in ATTRS:
                col_diff = f"{a}_diff" if f"{a}_diff" in merged.columns else a
                if col_diff in merged.columns:
                    merged[a] = merged[a].where(merged[col_diff].isna(), merged[col_diff])

            updated = merged.drop(columns=[diff_key] + [f"{a}_diff" for a in ATTRS if f"{a}_diff" in merged.columns], errors="ignore")

            # geometrias casadas por chave
            if "geometry" in gdf.columns:
                ok = gdf[gdf[diff_key].astype(str).isin(updated[csv_key].astype(str))]
                if not ok.empty:
                    matched_geoms.append(ok.assign(__src_layer=lay))

        # 2b) Fallback por (BR_PAD, UF) + intervalo de KM (se não houve match por chave ou para complementar)
        # Harmoniza mínimas colunas no diff
        gdf2 = gdf.copy()
        # UF
        uf_col = None
        for c in ["uf","sg_uf","sigla_uf","estado"]:
            if c in gdf2.columns:
                uf_col = c; break
        gdf2["_uf_norm"] = gdf2[uf_col].apply(normalize_uf_any) if uf_col else None
        # BR
        br_col = None
        for c in ["br","vl_br","rodovia_br","no_rodovia","rodovia"]:
            if c in gdf2.columns:
                br_col = c; break
        gdf2["_br_pad_norm"] = ensure_br_pad_from_any(gdf2[br_col]) if br_col else None
        # KMs
        kmi = gdf2["vl_km_inic"] if "vl_km_inic" in gdf2.columns else gdf2.get("km_inic")
        kmf = gdf2["vl_km_fina"] if "vl_km_fina" in gdf2.columns else gdf2.get("km_fim")
        gdf2["_km_ini"] = kmi.astype(float) if kmi is not None else np.nan
        gdf2["_km_fim"] = kmf.astype(float) if kmf is not None else np.nan

        # se temos UF+BR em ambas as bases:
        if updated["uf_norm"].notna().any() and updated["br_pad_norm"].notna().any() and \
           gdf2["_uf_norm"].notna().any() and gdf2["_br_pad_norm"].notna().any():

            # faz merge por BR/UF (amplo)
            m = updated.merge(gdf2, how="left",
                              left_on=["br_pad_norm","uf_norm"],
                              right_on=["_br_pad_norm","_uf_norm"],
                              suffixes=("","_d"))
            if "geometry" not in m.columns and "geometry" in gdf2.columns:
                # garante presença da col geometry para contagem
                m["geometry"] = gdf2.loc[m.index, "geometry"] if len(gdf2) == len(m) else None

            # calcula distância ao intervalo
            m["_km_delta"] = np.vectorize(km_delta_interval)(
                m["km_ini_norm"], m["_km_ini"], m["_km_fim"]
            )
            # escolhe melhor por linha original (index)
            m["_row_id"] = np.arange(len(m))
            # manter menor delta por cada (csv_key, br/uf)
            sort_cols = ["_km_delta"]
            m["_km_for_sort"] = m["_km_delta"].fillna(np.inf)
            # agrupar pelo índice da linha original do CSV
            # truque: cria um id para cada linha do CSV e propaga
            csv_ids = pd.Series(np.arange(len(updated)), index=updated.index, name="_csv_id")
            tmp = updated.copy()
            tmp["_csv_id"] = csv_ids
            m["_csv_id"] = tmp["_csv_id"].reindex(m.index, fill_value=np.nan)
            # ordenar e pegar melhor
            best_idx = m.sort_values(["_csv_id","_km_for_sort"]).groupby("_csv_id").head(1).index
            best = m.loc[best_idx].copy()

            # aplica atualização de atributos (se as colunas existirem no diff)
            for a in ATTRS:
                a_d = f"{a}_d"
                if a_d in best.columns:
                    updated[a] = np.where(best[a_d].notna(), best[a_d], updated[a])

            # geometrias casadas no fallback
            if "geometry" in best.columns and best["geometry"].notna().any():
                g_best = gpd.GeoDataFrame(best[["geometry"] + ([diff_key] if diff_key in best.columns else [])], geometry="geometry", crs="EPSG:4674")
                g_best = g_best.assign(__src_layer=f"{lay}_fallback")
                matched_geoms.append(g_best)

    # salva CSV atualizado (remonta nomes originais)
    # remove colunas auxiliares
    drop_aux = [c for c in updated.columns if c.endswith("_norm") or c.startswith("_")]
    out_csv = updated.drop(columns=drop_aux, errors="ignore")
    # se seu CSV original tinha 'id_trecho', mantenha
    out_csv.to_csv(args.csv_out, index=False, encoding="utf-8")
    print("OK ->", args.csv_out, "| linhas:", len(out_csv))

    # salvar GPKG parcial com as geometrias casadas
    if matched_geoms:
        big = pd.concat(matched_geoms, ignore_index=True)
        # dedup por geometria+chave se houver
        if "geometry" in big.columns:
            gpd.GeoDataFrame(big, geometry="geometry", crs="EPSG:4674").to_file(args.gpkg_out, layer="snv_diffs_geometry_NE", driver="GPKG")
            print("OK ->", args.gpkg_out, "(layer: snv_diffs_geometry_NE)")
    else:
        print("[INFO] Nenhuma geometria casada; GPKG não gerado.")

if __name__ == "__main__":
    main()
