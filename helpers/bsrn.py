'''
Helper functions for generate BSRN files
author: Vinicius Roggério da Rocha
e-mail: vinicius.rocha@inpe.br
version: 0.0.1
date: 2026-03-05
'''

import pandas as pd
from pathlib import Path

# Query para pesquisar informações em base solarimétrica
QUERY_SD = """
SELECT
    timestamp,

    glo_avg AS global_mean,
    glo_std AS global_std,
    glo_min AS global_min,
    glo_max AS global_max,

    dir_avg AS direct_mean,
    dir_std AS direct_std,
    dir_min AS direct_min,
    dir_max AS direct_max,

    dif_avg AS diffuse_mean,
    dif_std AS diffuse_std,
    dif_min AS diffuse_min,
    dif_max AS diffuse_max,

    lw_calc_avg AS lw_mean,
    lw_calc_std AS lw_std,
    lw_calc_min AS lw_min,
    lw_calc_max AS lw_max

FROM '{arquivo}'
WHERE acronym = '{est}'
AND year = {ano}
AND EXTRACT(month FROM timestamp) = {mes}
ORDER BY timestamp
"""

# Query para pesquisar informações em base meteorológica
QUERY_MD = """
SELECT
    timestamp,
    tp_sfc AS air_temperature,
    humid_sfc AS relative_humidity,
    press AS pressure
FROM '{arquivo}'
WHERE acronym = '{est}'
AND year = {ano}
AND EXTRACT(month FROM timestamp) = {mes}
ORDER BY timestamp
"""

def get_SD(con, arquivo, est, ano, mes):
    """
    Consulta o arquivo solarimétrico (.parquet) usando DuckDB e retorna
    um DataFrame com as variáveis radiométricas da estação, ano e mês
    especificados. Os campos são renomeados para nomes padronizados
    usados no processamento posterior.
    """

    q = QUERY_SD.format(
        arquivo=arquivo,
        est=est,
        ano=ano,
        mes=mes
    )

    return con.execute(q).df()

def get_MD(con, arquivo, est, ano, mes):
    """
    Consulta o arquivo meteorológico (.parquet) usando DuckDB e retorna
    um DataFrame com temperatura do ar, umidade relativa e pressão
    para a estação, ano e mês especificados.
    """

    q = QUERY_MD.format(
        arquivo=arquivo,
        est=est,
        ano=ano,
        mes=mes
    )

    return con.execute(q).df()

def fmt_i4(v):
    """
    Formata um valor inteiro no formato I4 (4 colunas) usado no
    arquivo final. Valores ausentes são substituídos pelo código
    -999.
    """
    if pd.isna(v):
        return f"{-999:4d}"
    return f"{int(round(v)):4d}"

def fmt_f51(v):
    """
    Formata um valor em ponto flutuante no formato F5.1 (5 colunas,
    uma casa decimal) usado no arquivo final. Valores ausentes são
    substituídos pelo código -99.9.
    """
    if pd.isna(v):
        return f"{-99.9:5.1f}"
    return f"{v:5.1f}"

def escrever_linhas(row):
    """
    Converte um registro de dados (linha do DataFrame) em duas linhas
    de texto no formato fixo especificado. A primeira linha contém
    informações de data, minuto do dia, irradiância global e direta.
    A segunda linha contém irradiância difusa, radiação de onda longa
    descendente e variáveis meteorológicas.
    """

    ts = row.timestamp
    day = ts.day
    minute = ts.hour*60 + ts.minute

    # LINE 1
    l1 = (
        f"{day:2d}"
        f"{minute:4d}"
        f"{fmt_f51(row.global_mean)}"
        f"{fmt_f51(row.global_std)}"
        f"{fmt_f51(row.global_min)}"
        f"{fmt_f51(row.global_max)}"
        f"{fmt_f51(row.direct_mean)}"
        f"{fmt_f51(row.direct_std)}"
        f"{fmt_f51(row.direct_min)}"
        f"{fmt_f51(row.direct_max)}"
    )

    # LINE 2
    l2 = (
        f"{'':6}"
        f"{fmt_f51(row.diffuse_mean)}"
        f"{fmt_f51(row.diffuse_std)}"
        f"{fmt_f51(row.diffuse_min)}"
        f"{fmt_f51(row.diffuse_max)}"
        f"{fmt_f51(row.lw_mean)}"
        f"{fmt_f51(row.lw_std)}"
        f"{fmt_f51(row.lw_min)}"
        f"{fmt_f51(row.lw_max)}"
        f"{fmt_f51(row.air_temperature)}"
        f"{fmt_f51(row.relative_humidity)}"
        f"{fmt_i4(row.pressure)}"
    )

    return l1, l2

def gerar_arquivo(df, estacao, ano, mes):
    """
    Gera o arquivo final no formato requerido. O arquivo é composto
    pelo cabeçalho específico da estação seguido pelas duas linhas
    de dados para cada minuto do mês. O nome do arquivo segue o
    padrão: estacaomesano.dat (ex.: ptr0125.dat).
    """

    header = Path(f"helpers/header_{estacao.lower()}.txt").read_text()

    nome = f"out/{estacao.lower()}{mes:02d}{str(ano)[-2:]}.dat"

    with open(nome, "w") as f:

        f.write(header)

        for row in df.itertuples():

            l1, l2 = escrever_linhas(row)

            f.write(l1 + "\n")
            f.write(l2 + "\n")

    print("arquivo gerado:", nome)

def interpolar_md(df_md):
    """
    Interpola os dados meteorológicos de frequência de 10 minutos para
    resolução de 1 minuto utilizando interpolação temporal baseada no
    índice de tempo. O resultado mantém o timestamp como coluna.
    """

    if len(df_md) == 0:
        return df_md

    df = df_md.copy()

    # usar timestamp como índice
    df = df.set_index("timestamp")

    # criar grade de 1 minuto
    idx = pd.date_range(
        start=df.index.min(),
        end=df.index.max(),
        freq="1min"
    )

    df = df.reindex(idx)

    # interpolação temporal
    df["air_temperature"] = df["air_temperature"].interpolate(method="time")
    df["relative_humidity"] = df["relative_humidity"].interpolate(method="time")
    df["pressure"] = df["pressure"].interpolate(method="time")

    df = df.reset_index().rename(columns={"index":"timestamp"})

    return df

def garantir_grade_completa(df, ano, mes):
    """
    Garante que o DataFrame contenha todos os minutos do mês
    (grade temporal contínua de 1 minuto). Minutos ausentes são
    inseridos e preenchidos posteriormente com códigos de dados
    faltantes.
    """

    if len(df) == 0:
        return df

    inicio = pd.Timestamp(year=ano, month=mes, day=1, hour=0, minute=0)

    if mes == 12:
        fim = pd.Timestamp(year=ano+1, month=1, day=1) - pd.Timedelta(minutes=1)
    else:
        fim = pd.Timestamp(year=ano, month=mes+1, day=1) - pd.Timedelta(minutes=1)

    idx = pd.date_range(inicio, fim, freq="1min")

    df = df.set_index("timestamp")
    df = df.reindex(idx)

    df = df.reset_index().rename(columns={"index":"timestamp"})

    return df

def preencher_missing(df):
    """
    Substitui valores ausentes (NaN) pelos códigos de dados faltantes
    definidos no formato do arquivo final:
    -99.9 para variáveis de ponto flutuante
    -999 para variáveis inteiras.
    """

    missing_float = -99.9
    missing_int = -999

    float_cols = [
        'global_mean','global_std','global_min','global_max',
        'direct_mean','direct_std','direct_min','direct_max',
        'diffuse_mean','diffuse_std','diffuse_min','diffuse_max',
        'lw_mean','lw_std','lw_min','lw_max',
        'air_temperature','relative_humidity'
    ]

    int_cols = ['pressure']

    for c in float_cols:
        if c in df.columns:
            df[c] = df[c].fillna(missing_float)

    for c in int_cols:
        if c in df.columns:
            df[c] = df[c].fillna(missing_int)

    return df