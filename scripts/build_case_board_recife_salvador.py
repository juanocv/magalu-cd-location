#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import numpy as np

SUM_DIR = Path("data/processed/dnit/summaries")
SLA_SUM = Path("data/processed/osrm/sla_ponderado_topN_summary.csv")  # já gerado nos seus passos
OUT     = Path("data/processed/case_board_recife_salvador.csv")

by_uf   = pd.read_csv(SUM_DIR/"snv_summary_UF.csv")        # km_total, pct_dup, pct_pav, pct_conc
by_bruf = pd.read_csv(SUM_DIR/"snv_summary_BR_UF.csv")

# Define UFs de influência primária
near_recife   = ["PE","PB","AL"]
near_salvador = ["BA","SE","AL"]

def agg(ufs):
    sub = by_uf[by_uf["uf"].isin(ufs)]
    w = sub["km_total"].replace(0,np.nan)
    def wavg(col):
        return (sub[col]*w).sum()/w.sum()
    return pd.Series({
        "km_total": sub["km_total"].sum(),
        "pct_dup_w": wavg("pct_dup"),
        "pct_pav_w": wavg("pct_pav"),
        "pct_conc_w": wavg("pct_conc")
    })

rec = agg(near_recife)
sal = agg(near_salvador)

df = pd.DataFrame({
    "indicador": ["km_total (influência)","% duplicada (média ponderada km)","% pavimentada (média ponderada km)","% em concessão (média ponderada km)"],
    "recife":    [rec["km_total"], rec["pct_dup_w"], rec["pct_pav_w"], rec["pct_conc_w"]],
    "salvador":  [sal["km_total"], sal["pct_dup_w"], sal["pct_pav_w"], sal["pct_conc_w"]]
})

# Anexa SLA ponderado (se existir)
try:
    sla = pd.read_csv(SLA_SUM)
    # espera colunas: 'cidade_base' ['Recife','Salvador'], 'sla_ponderado_h'
    sla_pivot = sla.pivot_table(index=None, columns="cidade_base", values="sla_ponderado_h", aggfunc="first")
    df = pd.concat([df, pd.DataFrame({"indicador":["SLA ponderado (h)"],
                                      "recife":[sla_pivot.get("Recife", np.nan)],
                                      "salvador":[sla_pivot.get("Salvador", np.nan)]})], ignore_index=True)
except Exception as e:
    pass

df.to_csv(OUT, index=False, encoding="utf-8")
print("OK ->", OUT)
