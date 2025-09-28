
# Magalu — Case CD Nordeste (Recife × Salvador)

Este repositório implementa, ponta-a-ponta, a análise que compara Recife e Salvador como base para um novo Centro de Distribuição. O pipeline cobre **demanda (IBGE) → roteamento/SLA (OSRM/OSM) → infraestrutura (DNIT/SNV)**, com scripts e notebooks reprodutíveis.

## Estrutura
```
magalu-cd-location/
├── data/
│ ├── raw/
│ │ ├── ibge/ # arquivos brutos IBGE
│ │ ├── osrm/ # nordeste-latest.osm.pbf
│ │ └── dnit/ # CSVs SNV + GPKGs de diferenças
│ ├── interim/
│ │ ├── ibge/ # limpezas e joins IBGE
│ │ └── dnit/ # SNV normalizado (sem geometria)
│ └── processed/
│ ├── ibge/
│ ├── osrm/ # OD/SLAs gerados
│ └── dnit/ # summaries e GPKGs parciais
├── notebooks/
│ ├── 05_consumo_score.ipynb
│ ├── 06_sla_vs_consumo.ipynb
│ └── 07_snv_diffs_qc_map.ipynb
├── scripts/
│ ├── prepare_dnit_from_csv.py
│ ├── apply_snv_diffs_to_csv.py
│ ├── summarize_snv_for_case.py
│ ├── osm_extract_br_corridors.py # opcional (OSMnx)
│ ├── build_od_capitais.py
│ ├── muni_centroids_sample.py
│ ├── sla_ponderado_topN.py
│ └── build_case_board_recife_salvador.py
└── requirements-full.txt
```

## Reprodutibilidade — passo a passo

### 0) Preparar ambiente

Windows (PowerShell/CMD) ou Linux/Mac:

```bash
python -m venv .venv
# Windows
.venv\\Scripts\\activate
# Linux/Mac
source .venv/bin/activate

pip install -r requirements-full.txt

### Requisitos
- Básico (demo): `pandas`, `numpy`, `matplotlib`, `nbformat`
- Completo (dados reais): ver **requirements-full.txt**

### Extras para dados reais
- Use `requirements.txt` (básico) para a demo sintética.
- Quando migrar para OSM/H3/OR-Tools e mapas reais, instale também os **extras**:
  - Descomente as linhas no `requirements.txt` **ou** instale tudo de uma vez com:
  ```bash
  pip install -r requirements-full.txt
```

### 1) IBGE → score de consumo

- Entrada (em data/interim/ibge/, já limpos):
  - pib_municipal_2021.csv — year, code_muni, nome_muni, pib_pc_2021_brl
  - populacao_municipal_nordeste_2021.csv — code_muni, pop_2021
  - renda_per_capita_uf_2024.csv — sigla, renda_pc_2024_nominal_brl
  - municipios_NE_2022.gpkg — geometria

- Lógica (notebook 05_consumo_score.ipynb):
  - Score municipal ≈ pop_2021 × pib_pc_2021_brl × ajuste(renda_uf_2024)
  - Normalização min–max para pesos demand_weight.

- Saída esperada:
  - data/processed/ibge/consumo_municipal_NE_2021.csv
  - Se preferir, replique a lógica do notebook num script CLI para automação.

### 2) Roteamento OSRM/OSM → SLA ponderado
OSM (grafo): baixe nordeste-latest.osm.pbf (Geofabrik) em data/raw/osrm/
Rodar OSRM - Docker (recomendado)

```bash
docker pull osrm/osrm-backend

# preparar dados (perfil carro + MLD)
docker run -t -v %cd%/data/raw/osrm:/data osrm/osrm-backend osrm-extract -p /opt/car.lua /data/nordeste-latest.osm.pbf
docker run -t -v %cd%/data/raw/osrm:/data osrm/osrm-backend osrm-partition /data/nordeste-latest.osrm
docker run -t -v %cd%/data/raw/osrm:/data osrm/osrm-backend osrm-customize /data/nordeste-latest.osrm

# servir a API
docker run -t -i -p 5000:5000 -v %cd%/data/raw/osrm:/data osrm/osrm-backend osrm-routed --algorithm=MLD /data/nordeste-latest.osrm
```

Saídas:
- od_capitais_recife_salvador.csv
- municipios_topN.csv
- od_municipios_topN_recife_salvador.csv
- sla_ponderado_topN_summary.csv

### 3) DNIT/SNV → qualidade da malha
O pacote SHP oficial estava sem .dbf; usamos planilhas CSV + GPKGs de diferenças (2025-04→2025-07).

Entrada (data/raw/dnit/):
- SNV_BASES_GEOMETRICAS_202507A.csv (cabeçalho tipado)
- SNV_ROTAS_202507A.csv (cabeçalho tipado)
- SNV_PLANILHA_202507A.csv
- Diferencas_geometrias_SNV_202504A_202507A.gpkg (ou NE filtrado)

Normalizar planilhas SNV → Nordeste
```bash
python scripts/prepare_dnit_from_csv.py
```
Aplicar “diffs” (GPKG) para atualizar atributos e casar geometrias
```bash
# (opcional) inspecionar layers
python scripts/snv_gpkg_diffs_inspect.py \
  --gpkg data/raw/dnit/Diferencas_geometrias_SNV_202504A_202507A.gpkg \
  --out-gpkg data/processed/dnit/diffs_NE_202504A_202507A.gpkg \
  --out-csv  data/processed/dnit/diffs_NE_202504A_202507A.csv

# aplicar no consolidado
python scripts/apply_snv_diffs_to_csv.py \
  --csv-in  data/interim/dnit/snv_trechos_NE_2025-07_from_csv.csv \
  --gpkg-diff data/processed/dnit/diffs_NE_202504A_202507A.gpkg \
  --csv-out data/interim/dnit/snv_trechos_NE_2025-07_updated.csv \
  --gpkg-out data/processed/dnit/snv_NE_2025-07_partial_geom.gpkg
```

### 4) Síntese do case (Recife × Salvador)
Gera um quadro consolidado (SLA, cobertura e infraestrutura) pronto para README/slide:
```bash
python scripts/build_case_board_recife_salvador.py
# → data/processed/case_board_recife_salvador.csv
```

### 5) Notebooks
- 01_consumo_score.ipynb — construção do score de consumo (IBGE)
- 02_sla_vs_consumo.ipynb — análise OD/SLA ponderado e cobertura ≤12 h
- 03_snv_diffs_qc_map.ipynb — QA de diffs DNIT e mapas (NE)

## Resultados esperados (arquivos)

IBGE / Demanda
- ```data/processed/ibge/consumo_municipal_NE_2021.csv```

OSRM / SLA
- ```data/processed/osrm/od_capitais_recife_salvador.csv```
- ```data/processed/osrm/municipios_topN.csv```
- ```data/processed/osrm/od_municipios_topN_recife_salvador.csv```
- ```data/processed/osrm/sla_ponderado_topN_summary.csv```

DNIT / Infraestrutura
- ```data/interim/dnit/snv_trechos_NE_2025-07_updated.csv```
- ```data/processed/dnit/summaries/snv_summary_BR_UF.csv```
- ```data/processed/dnit/summaries/snv_summary_UF.csv```
- ```data/processed/dnit/summaries/snv_top_brs_NE.csv```
- ```data/processed/dnit/snv_NE_2025-07_partial_geom.gpkg```

Síntese
- ```data/processed/case_board_recife_salvador.csv```

## Troubleshooting
1. OSRM não sobe / porta 5000 ocupada: troque a porta -p 5001:5000 e ajuste --osrm http://localhost:5001.
2. pyogrio/fiona “SHX/DBF missing”: para SHP, garanta .shp/.dbf/.shx/.prj juntos. Quando não houver .dbf, use as planilhas CSV e os GPKGs de diferenças (como neste projeto).
3. Encoding de planilhas: se ver caracteres estranhos, tente encoding=\"latin-1\" no read_csv.
4. DNIT — colunas divergentes: os scripts já tentam detectar variações (vl_km_inic, km_inicial, vl_br, rodovia_br etc.). Se surgir um nome novo, ajuste o dicionário no script.
5. Desvios grandes de tempo OSRM: verifique se o recorte do grafo cobre todo o NE; re-extraia o PBF; revise perfil car.lua/speed.
6. QGIS não mostra layer: confirme CRS (EPSG:4674) e se o GPKG tem a layer correta (snv_diffs_geometry_NE).

## Referências
1. PIB dos Municípios (base 2010–2021) – IBGE: base oficial usada para PIB municipal e PIB per capita de 2021. 
2. Rendimento domiciliar per capita por UF (2024) – PNAD Contínua / IBGE (release oficial com valores por UF). 
3. SNV/PNV – DNIT (portal e dados abertos): planilhas do SNV, bases e GPKGs de diferenças (2025) usados para atributos e validação da rede. 
4. OpenStreetMap (recorte Nordeste) – Geofabrik: fonte do nordeste-latest.osm.pbf para construção do grafo de roteamento. 
5. OSRM – Open Source Routing Machine: motor de roteamento usado nas matrizes OD e tempos. 
6. OSMnx (documentação): utilitário para baixar/inspecionar malhas do OSM e apoiar QA do grafo.