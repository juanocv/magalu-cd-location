#!/usr/bin/env python3
"""
Join do score de consumo municipal (NE) com as geometrias de municípios (NE).

Requisitos:
  - geopandas, shapely, fiona, pyproj (instale via requirements-full.txt)

Uso:
  python scripts/join_consumo_to_geoms.py

Entradas esperadas:
  data/interim/ibge/municipios_NE_2022.gpkg
     • Campos típicos: CD_MUN (código IBGE 7 dígitos), NM_MUN, geometry
  data/processed/ibge/consumo_municipal_NE_2021.csv
     • Campos: code_muni, nome_muni, sigla, uf, pop_2021, ..., score_consumo, demand_weight

Saída:
  data/processed/ibge/consumo_municipal_NE_2021.gpkg  (layer='consumo_ne')
"""

from pathlib import Path
import geopandas as gpd
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
gpk_in = BASE / "data" / "interim" / "ibge" / "municipios_NE_2022.gpkg"
csv_in = BASE / "data" / "processed" / "ibge" / "consumo_municipal_NE_2021.csv"
gpk_out = BASE / "data" / "processed" / "ibge" / "consumo_municipal_NE_2021.gpkg"

def _clean_code(x: str) -> str:
    s = "".join(ch for ch in str(x) if ch.isdigit())
    return s.zfill(7) if s else None

def main():
    if not gpk_in.exists():
        raise FileNotFoundError(f"Não encontrei: {gpk_in}")
    if not csv_in.exists():
        raise FileNotFoundError(f"Não encontrei: {csv_in}")

    # Carrega geometrias (ajuste 'layer' se seu arquivo tiver múltiplas camadas)
    gdf = gpd.read_file(gpk_in)
    if "CD_MUN" not in gdf.columns:
        raise RuntimeError("Campo 'CD_MUN' não encontrado no GPKG de municípios. Ajuste o script conforme seu schema.")
    gdf["CD_MUN"] = gdf["CD_MUN"].astype(str).map(_clean_code)

    # Score de consumo
    df = pd.read_csv(csv_in)
    if "code_muni" not in df.columns:
        raise RuntimeError("Campo 'code_muni' não encontrado no CSV de consumo.")
    df["code_muni"] = df["code_muni"].astype(str).map(_clean_code)

    # Join por atributo (left join nas geometrias)
    gdf2 = gdf.merge(df, left_on="CD_MUN", right_on="code_muni", how="left")

    # Salva GPKG temático
    gdf2.to_file(gpk_out, layer="consumo_ne", driver="GPKG")
    print(f"OK -> {gpk_out} (layer='consumo_ne')")

if __name__ == "__main__":
    main()