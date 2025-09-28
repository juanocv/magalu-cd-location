#!/usr/bin/env python3
# scripts/prepare_snv_interim.py
from pathlib import Path
import pandas as pd
import numpy as np
import re
from typing import Optional, Dict

BASE = Path(__file__).resolve().parents[1]
IN_CSV = BASE / "data" / "raw" / "dnit" / "tabela_snv_202507A.csv"  # ajuste o caminho se estiver em outro lugar
OUT_CSV = BASE / "data" / "interim" / "dnit" / "snv_trechos_NE_2025-07.csv"
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# ----- (opcional) mapeamento manual para forçar colunas -----
# Preencha com os nomes EXATOS do seu CSV se você já souber quais são:
OVERRIDE_MAP: Dict[str, Optional[str]] = {
    # "id_trecho": "cod",
    # "br": "rodovia_br",
    # "uf": "uf",
    # "trecho_desc": "tipo_trecho",
    # "localidade": "unidade_local",
    # "km_ini": "km_inicial",
    # "km_fim": "km_final",
    # "ext_km": "extensao",
    # "situacao": "SITUACAO",
    # "classe": "CLASSE",
    # "sentido": "SENTIDO",
    # "jurisdicao": "jurisdicao",
    # "concessao": "CONCESSAO",
    # "data_ref": "DATA_REFERENCIA",
}

def read_csv_smart(path: Path) -> pd.DataFrame:
    last_err = None
    for enc in ("utf-8", "utf-8-sig", "latin1", "cp1252"):
        for sep in (",", ";", "|", "\t"):
            try:
                df = pd.read_csv(path, encoding=enc, sep=sep)
                if df.shape[1] == 1 and any(ch in str(df.columns[0]) for ch in [";", "|", "\t"]):
                    continue
                return df
            except Exception as e:
                last_err = e
                continue
    raise RuntimeError(f"Falha ao ler {path}: {last_err}")

def parse_ptbr_number(x):
    if pd.isna(x):
        return np.nan
    s = str(x).replace("\xa0"," ").strip()
    s = re.sub(r"[^\d,.-]", "", s)      # remove letras/unidades
    s = s.replace(".", "").replace(",", ".")  # pt-BR -> float
    try:
        return float(s)
    except Exception:
        return np.nan

def clean_text(x):
    if pd.isna(x): return None
    return str(x).strip()

def find_col(df_cols, patterns):
    """retorna o 1º nome de coluna que casa com uma lista de regex."""
    for pat in patterns:
        rgx = re.compile(pat, re.I)
        for c in df_cols:
            if rgx.search(str(c)):
                return c
    return None

def detect_columns(df: pd.DataFrame) -> dict:
    cols = list(df.columns)
    # se tiver override, respeita
    col = {}
    def pick(key, pats):
        if key in OVERRIDE_MAP and OVERRIDE_MAP[key]:
            return OVERRIDE_MAP[key]
        return find_col(cols, pats)

    col["id_trecho"] = pick("id_trecho", [r"(id.*trecho|identificador|cod|cod.*trecho)"])
    col["br"]        = pick("br", [r"\bBR\b", r"\brodov", r"\brodovia"])
    col["uf"]        = pick("uf", [r"^UF$", r"\bUF\b"])
    col["trecho_desc"]= pick("trecho_desc", [r"(trecho|segmento|descri)"])
    col["localidade"]= pick("localidade", [r"(unidade|municip|localidade|cidade)"])
    col["km_ini"]    = pick("km_ini", [r"(km.*ini|ini.*km|km[_ ]?inic|KM_INI)"])
    col["km_fim"]    = pick("km_fim", [r"(km.*fim|fim.*km|km[_ ]?final|KM_FIM)"])
    col["ext_km"]    = pick("ext_km", [r"(extens|compr|EXT_KM)"])
    col["situacao"]  = pick("situacao", [r"(situ|condic|pav|revest|superficie)"])
    col["classe"]    = pick("classe", [r"(classe|classif)"])
    col["sentido"]   = pick("sentido", [r"(sentid)"])
    col["jurisdicao"]= pick("jurisdicao", [r"(jurisd|administ)"])
    col["concessao"] = pick("concessao", [r"(concess|conced)"])
    col["data_ref"]  = "2025-07"
    return col

def infer_classe_from_br(br_val):
    """
    Infere a classe da rodovia a partir do primeiro dígito do número da BR.
    0xx = Radial
    1xx = Longitudinal
    2xx = Transversal
    3xx = Diagonal
    4xx = Ligação
    """
    if pd.isna(br_val):
        return None
    match = re.search(r"(\d{2,3})", str(br_val))
    if not match:
        return None
    first_digit = match.group(1)[0]
    if first_digit == "0":
        return "Radial"
    elif first_digit == "1":
        return "Longitudinal"
    elif first_digit == "2":
        return "Transversal"
    elif first_digit == "3":
        return "Diagonal"
    elif first_digit == "4":
        return "Ligação"
    else:
        return None

def infer_sentido_from_km(km_ini, km_fim):
    """
    Infere o sentido do trecho:
    - "km_crescente" se km_fim >= km_ini
    - "km_decrescente" caso contrário
    """
    try:
        if pd.isna(km_ini) or pd.isna(km_fim):
            return None
        if float(km_fim) >= float(km_ini):
            return "km_crescente"
        else:
            return "km_decrescente"
    except Exception:
        return None

def infer_concessao_from_administracao(adm_val):
    """
    Retorna 'sim' se administracao contiver 'Concessão' ou 'Convênio', senão 'nao'.
    """
    if pd.isna(adm_val):
        return "nao"
    adm_str = str(adm_val).lower()
    if "concessão" in adm_str or "convênio" in adm_str:
        return "sim"
    return "nao"

def build_interim():
    df_raw = read_csv_smart(IN_CSV)
    colmap = detect_columns(df_raw)

    # log de mapeamento
    print("[MAPEAMENTO DETECTADO]")
    for k,v in colmap.items():
        print(f"  {k:12s} <- {v}")

    # monta dataframe de saída
    out = pd.DataFrame()
    for k, src in colmap.items():
        if src in df_raw.columns:
            out[k] = df_raw[src]
        else:
            out[k] = np.nan if k in ["km_ini","km_fim","ext_km"] else None

    # limpeza de tipos
    for k in ["id_trecho","br","uf","trecho_desc","localidade","situacao","classe","sentido","jurisdicao","concessao","data_ref"]:
        out[k] = out[k].apply(clean_text)
    for k in ["km_ini","km_fim","ext_km"]:
        out[k] = out[k].apply(parse_ptbr_number)

    # BR padronizada e extensão derivada
    out["br_num"] = out["br"].str.extract(r"(\d{2,3})")
    out["br_pad"] = np.where(out["br_num"].notna(), "BR-" + out["br_num"], None)
    m_need_ext = out["ext_km"].isna() & out["km_ini"].notna() & out["km_fim"].notna()
    out.loc[m_need_ext, "ext_km"] = (out.loc[m_need_ext, "km_fim"] - out.loc[m_need_ext, "km_ini"]).abs()

    # Inferir classe a partir do número da BR
    out["classe"] = out["br"].apply(infer_classe_from_br)

    # Inferir sentido a partir de km_ini e km_fim
    out["sentido"] = out.apply(lambda row: infer_sentido_from_km(row["km_ini"], row["km_fim"]), axis=1)

    # Inferir concessao a partir da coluna 'administracao'
    if "administracao" in df_raw.columns:
        out["concessao"] = df_raw["administracao"].apply(infer_concessao_from_administracao)
    else:
        out["concessao"] = "nao"

    # Fixar data_ref em "2025-07"
    out["data_ref"] = "2025-07"

    # filtro Nordeste
    ufs_ne = {"AL","BA","CE","MA","PB","PE","PI","RN","SE"}
    out = out[out["uf"].isin(ufs_ne)]

    # somente BR válida e ext_km > 0
    out = out[out["br_pad"].notna() & out["ext_km"].fillna(0).gt(0)]

    # chave auxiliar
    out["km_ini_arred"] = np.floor(out["km_ini"].astype(float)).astype("Int64")
    out["chave_trecho_hint"] = out["br_pad"].fillna("") + "|" + out["uf"].fillna("") + "|" + out["km_ini_arred"].astype(str)

    # reordena
    cols_out = ["id_trecho","br","br_pad","uf","trecho_desc","localidade","km_ini","km_fim","ext_km",
                "situacao","classe","sentido","jurisdicao","concessao","data_ref","chave_trecho_hint"]
    out = out[cols_out]

    # QC rápido
    print("\n[QC]")
    print("linhas:", len(out))
    for c in ["id_trecho","localidade","situacao","classe","sentido","concessao","data_ref"]:
        nfilled = out[c].notna().sum()
        print(f"preenchidos {c:12s}: {nfilled}")

    out.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print("\nOK ->", OUT_CSV)

if __name__ == "__main__":
    build_interim()
