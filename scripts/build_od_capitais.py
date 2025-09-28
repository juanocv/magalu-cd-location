#!/usr/bin/env python3
"""
Gera matriz OD (tempo/distância) de Recife e Salvador para as capitais do NE
usando OSRM local (http://localhost:5000).

Saída:
  data/processed/osrm/od_capitais_recife_salvador.csv
"""

import requests
import pandas as pd
from pathlib import Path

OSRM = "http://localhost:5000"  # ajuste se necessário

# Coordenadas (lon, lat) das capitais do NE (aprox, pode refinar depois)
CAPITAIS = {
    "Salvador-BA":    (-38.5014, -12.9714),
    "Recife-PE":      (-34.8770,  -8.0476),
    "Fortaleza-CE":   (-38.5267,  -3.7319),
    "São Luís-MA":    (-44.3028,  -2.5307),
    "Teresina-PI":    (-42.8034,  -5.0919),
    "Natal-RN":       (-35.2110,  -5.7945),
    "João Pessoa-PB": (-34.8610,  -7.1153),
    "Maceió-AL":      (-35.7089,  -9.6499),
    "Aracaju-SE":     (-37.0731, -10.9472),
}

# Origens (candidatos de CD)
ORIGENS = [
    ("Salvador-BA", CAPITAIS["Salvador-BA"]),
    ("Recife-PE",   CAPITAIS["Recife-PE"]),
]

DESTINOS = [(k,v) for k,v in CAPITAIS.items()]  # todas as capitais

def osrm_table(coords, sources_idx, destinations_idx, annotations="duration,distance"):
    """Consulta /table do OSRM."""
    # monta string "lon,lat;lon,lat;..."
    coord_str = ";".join([f"{lon},{lat}" for lon,lat in coords])
    params = {
        "sources": ";".join(map(str, sources_idx)),
        "destinations": ";".join(map(str, destinations_idx)),
        "annotations": annotations,
    }
    url = f"{OSRM}/table/v1/driving/{coord_str}"
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def main():
    processed = Path("data/processed/osrm")
    processed.mkdir(parents=True, exist_ok=True)

    # 1) empilha todas as coordenadas numa única lista
    all_points = [o[1] for o in ORIGENS] + [d[1] for d in DESTINOS]
    # idx das sources e destinations dentro do vetor all_points
    sources_idx = list(range(len(ORIGENS)))  # 0..(n_origens-1)
    destinations_idx = list(range(len(ORIGENS), len(ORIGENS)+len(DESTINOS)))

    # 2) chama OSRM /table
    data = osrm_table(all_points, sources_idx, destinations_idx)

    # 3) monta dataframe longo (origem->destino)
    durations = data.get("durations", [])
    distances = data.get("distances", [])

    rows = []
    for si, (oname, _o) in enumerate(ORIGENS):
        for dj, (dname, _d) in enumerate(DESTINOS):
            dur_s = durations[si][dj] if durations else None
            dist_m = distances[si][dj] if distances else None
            rows.append({
                "origem": oname,
                "destino": dname,
                "dur_s": dur_s,
                "dur_h": (dur_s/3600.0) if dur_s is not None else None,
                "dist_m": dist_m,
                "dist_km": (dist_m/1000.0) if dist_m is not None else None,
            })

    df = pd.DataFrame(rows)
    out = processed / "od_capitais_recife_salvador.csv"
    df.to_csv(out, index=False)
    print("OK ->", out)

if __name__ == "__main__":
    main()