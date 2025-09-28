#!/usr/bin/env python3
# scripts/summarize_snv_for_case.py
from pathlib import Path
import argparse
import pandas as pd
import numpy as np
import re

def series_text(df, col):
    """Retorna uma série de strings (vazia se a coluna não existir)."""
    if col in df.columns:
        return df[col].fillna("").astype(str).str.strip()
    return pd.Series([""] * len(df), index=df.index)

def ensure_br_pad(s):
    """Gera BR-xxx a partir de 'br_pad' ou 'br' numérico."""
    if "br_pad" in s.columns:
        out = s["br_pad"].astype(str)
        # normalizar para BR-xxx
        out = out.str.extract(r"(\d{2,3})", expand=False).apply(lambda x: f"BR-{x}" if pd.notna(x) else np.nan)
        return out
    if "br" in s.columns:
        return s["br"].astype(str).str.extract(r"(\d{2,3})", expand=False).apply(lambda x: f"BR-{x}" if pd.notna(x) else np.nan)
    return pd.Series([np.nan]*len(s), index=s.index)

def load_df(path_csv: Path):
    df = pd.read_csv(path_csv)
    # normaliza nomes comuns
    # km
    len_col = "extensao" if "extensao" in df.columns else ("ext_km" if "ext_km" in df.columns else None)
    if len_col is None:
        raise RuntimeError("Não encontrei coluna de extensão ('extensao' ou 'ext_km').")
    df["_len_km"] = pd.to_numeric(df[len_col], errors="coerce").fillna(0.0)

    # uf
    if "uf" not in df.columns:
        raise RuntimeError("CSV precisa ter a coluna 'uf'.")
    df["uf"] = df["uf"].astype(str).str.upper().str.strip()

    # br_pad
    df["_br_pad"] = ensure_br_pad(df)

    # flags
    pista_txt = series_text(df, "pista")
    situ_txt  = series_text(df, "situacao")
    desc_txt  = series_text(df, "trecho_desc") + " " + series_text(df, "localidade") + " " + series_text(df, "obras")

    # duplicação: pista contém 'dupl' OU descrição cita 'duplica'
    df["_is_dup"] = (
        pista_txt.str.lower().str.contains("dupl") |
        desc_txt.str.lower().str.contains("duplic")
    ).astype(int)

    # pavimentada: códigos e palavras-chave
    st = situ_txt.str.lower()
    pav_keywords = ("pav", "asf", "asfalto", "concreto", "tst", "revest")
    # muitos conjuntos usam 'PLA' como abreviação — trate como pavimentada
    pav_code = situ_txt.str.upper().str.startswith("P")  # PAV/PLA/etc.
    df["_is_pav"] = (
        pav_code |
        st.str.contains("|".join(pav_keywords))
    ).astype(int)

    # concessão: 'sim', 'conces', 'conced'
    conc_txt = series_text(df, "concessao").str.lower()
    df["_is_conc"] = (
        conc_txt.str.startswith("s") |
        conc_txt.str.contains("conc")
    ).astype(int)

    return df

def summarize(df: pd.DataFrame, by_cols):
    # somas ponderadas por km
    km = df["_len_km"].astype(float)
    safe_km = km.fillna(0.0)

    def wsum(mask):
        return float((mask.astype(int) * safe_km).sum())

    grp = []
    for keys, sub in df.groupby(by_cols, dropna=False):
        sub = sub.copy()
        km_total = float(sub["_len_km"].sum())
        km_dup   = wsum(sub["_is_dup"])
        km_pav   = wsum(sub["_is_pav"])
        km_conc  = wsum(sub["_is_conc"])
        row = dict(zip(by_cols, keys if isinstance(keys, tuple) else (keys,)))
        row.update({
            "km_total": km_total,
            "km_dup": km_dup,
            "km_pav": km_pav,
            "km_conc": km_conc,
            "pct_dup":  (km_dup/km_total) if km_total > 0 else 0.0,
            "pct_pav":  (km_pav/km_total) if km_total > 0 else 0.0,
            "pct_conc": (km_conc/km_total) if km_total > 0 else 0.0,
            "n_trechos": len(sub),
        })
        grp.append(row)
    return pd.DataFrame(grp)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-csv", default="data/interim/dnit/snv_trechos_NE_2025-07_updated.csv")
    ap.add_argument("--out-dir", default="data/processed/dnit/summaries")
    args = ap.parse_args()

    in_csv = Path(args.in_csv)
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)

    df = load_df(in_csv)

    # Resumo BR x UF
    by_br_uf = summarize(df, ["_br_pad","uf"]).rename(columns={"_br_pad":"br_pad"}).sort_values(["br_pad","uf"])
    by_br_uf.to_csv(out_dir/"snv_summary_BR_UF.csv", index=False, encoding="utf-8")

    # Resumo por UF
    by_uf = summarize(df, ["uf"]).sort_values("uf")
    by_uf.to_csv(out_dir/"snv_summary_UF.csv", index=False, encoding="utf-8")

    # Top BRs por extensão no NE
    top_brs = (by_br_uf.groupby("br_pad", as_index=False)
                        .agg(km_total=("km_total","sum"))
                        .sort_values("km_total", ascending=False)
                        .head(20))
    top_brs.to_csv(out_dir/"snv_top_brs_NE.csv", index=False, encoding="utf-8")

    print("OK ->", out_dir)

if __name__ == "__main__":
    main()
