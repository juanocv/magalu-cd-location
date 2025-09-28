#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Inspeciona um GPKG de DIFERENÇAS do SNV (DNIT), lista camadas, campos e exporta
apenas o Nordeste para um GPKG 'filtrado' (opcional), além de um CSV de atributos.

Uso:
  python scripts/snv_gpkg_diffs_inspect.py \
    --gpkg data/raw/dnit/Diferencas_geometrias_SNV_202504A_202507A.gpkg \
    --out-gpkg data/processed/dnit/diffs_NE_202504A_202507A.gpkg \
    --out-csv data/processed/dnit/diffs_NE_202504A_202507A.csv
"""

import argparse
from pathlib import Path
import geopandas as gpd
import pandas as pd
import re

UFS_NE = {"AL","BA","CE","MA","PB","PE","PI","RN","SE"}

def normalize_uf(x: str):
    if x is None: return None
    s = str(x).strip().upper()
    s = re.sub(r"\s+", " ", s)
    m = {
        "ALAGOAS":"AL","BAHIA":"BA","CEARA":"CE","MARANHAO":"MA","PARAIBA":"PB",
        "PERNAMBUCO":"PE","PIAUI":"PI","RIO GRANDE DO NORTE":"RN","SERGIPE":"SE",
    }
    if s in UFS_NE: return s
    if s in m: return m[s]
    # tenta extrair sigla entre parênteses: "Bahia (BA)"
    m2 = re.search(r"\(([A-Z]{2})\)", s)
    if m2: return m2.group(1)
    if re.fullmatch(r"[A-Z]{2}", s): return s
    return None

def list_layers(path):
    # GeoPandas usa fiona ou pyogrio; ambos aceitam listar com gpd.read_file + layer=None? Não.
    # Solução portátil: tente abrir sem layer para falhar com lista; fallback com fiona.
    try:
        import fiona
        with fiona.Env():
            with fiona.open(path) as src:
                return src.listlayers()
    except Exception:
        import fiona
        return fiona.listlayers(path)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gpkg", required=True)
    ap.add_argument("--out-gpkg", default=None)
    ap.add_argument("--out-csv", default=None)
    args = ap.parse_args()

    gpkg = Path(args.gpkg)
    layers = list_layers(str(gpkg))
    print("[INFO] Camadas no GPKG:")
    for lay in layers:
        print("  -", lay)

    collected = []
    for lay in layers:
        try:
            gdf = gpd.read_file(gpkg, layer=lay)
        except Exception as e:
            print(f"[WARN] Falha ao ler layer {lay}: {e}")
            continue

        cols = list(gdf.columns)
        print(f"\n[LAYER] {lay} | linhas={len(gdf)} | cols={cols[:15]}...")

        # tenta detectar campo de UF
        uf_col = None
        for c in cols:
            if re.fullmatch(r"UF|uf|sg_uf|sigla_uf|estado|SG_UF|SIGLA_UF", str(c), flags=re.I):
                uf_col = c
                break
        if uf_col is None:
            # procura qualquer col com 'UF'
            for c in cols:
                if re.search(r"\buf\b", str(c), flags=re.I):
                    uf_col = c; break

        if uf_col:
            gdf["_UF_NE"] = gdf[uf_col].apply(normalize_uf)
            gdf_ne = gdf[gdf["_UF_NE"].isin(UFS_NE)].copy()
        else:
            # sem UF explícita; exporta tudo e você filtra depois por geometria (opcional)
            gdf["_UF_NE"] = None
            gdf_ne = gdf

        print(f"  UF field: {uf_col} | NE linhas: {len(gdf_ne)}")

        # guarda em lista para opcional export
        gdf_ne["__layer"] = lay
        collected.append(gdf_ne)

    if not collected:
        print("\n[INFO] Nada coletado.")
        return

    out_all = pd.concat(collected, ignore_index=True)

    if args.out_gpkg:
        out_path = Path(args.out_gpkg)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        # grava uma camada por 'lay' original
        for lay in out_all["__layer"].unique():
            sub = out_all[out_all["__layer"]==lay].drop(columns=["__layer"])
            if sub.empty: continue
            # se tiver geometria, salva como layer espacial; senão, salva CSV separado
            if "geometry" in sub.columns and sub.geometry.notna().any():
                gpd.GeoDataFrame(sub, geometry="geometry", crs="EPSG:4674").to_file(
                    out_path, layer=f"{lay}_NE", driver="GPKG"
                )
            else:
                csv_fallback = out_path.with_suffix("").as_posix() + f"_{lay}_NE.csv"
                pd.DataFrame(sub.drop(columns=["geometry"], errors="ignore")).to_csv(csv_fallback, index=False, encoding="utf-8")
                print("  -> sem geometria; exportei CSV:", csv_fallback)

        print("OK ->", out_path)

    if args.out_csv:
        out_csv = Path(args.out_csv)
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        df_attr = pd.DataFrame(out_all.drop(columns=["geometry"], errors="ignore"))
        df_attr.to_csv(out_csv, index=False, encoding="utf-8")
        print("OK ->", out_csv)

if __name__ == "__main__":
    main()
