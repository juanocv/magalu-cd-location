# scripts/muni_centroids_sample.py
import geopandas as gpd
import pandas as pd
from pathlib import Path

GPK = "data/interim/ibge/municipios_NE_2022.gpkg"
SCORE = "data/processed/ibge/consumo_municipal_NE_2021.csv"
OUT = "data/processed/osrm/municipios_topN.csv"

N = 500  # ajuste

gdf = gpd.read_file(GPK)  # campos: CD_MUN, NM_MUN, geometry
gdf["CD_MUN"] = gdf["CD_MUN"].astype(str).str.replace(r"\D","", regex=True).str.zfill(7)
gdf["centroid"] = gdf.geometry.centroid
gdf["lon"] = gdf["centroid"].x
gdf["lat"] = gdf["centroid"].y

df = pd.read_csv(SCORE)  # tem code_muni, demand_weight
df["code_muni"] = df["code_muni"].astype(str).str.zfill(7)

merged = gdf.merge(df[["code_muni","nome_muni","sigla","demand_weight"]],
                   left_on="CD_MUN", right_on="code_muni", how="left")

top = merged.sort_values("demand_weight", ascending=False).head(N)
top[["CD_MUN","NM_MUN","sigla","lon","lat","demand_weight"]].to_csv(OUT, index=False)
print("OK ->", OUT)
