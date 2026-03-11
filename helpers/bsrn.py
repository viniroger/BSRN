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
    glo_avg, glo_std, glo_min, glo_max,
    dir_avg, dir_std, dir_min, dir_max,
    dif_avg, dif_std, dif_min, dif_max,
    lw_calc_avg, lw_calc_std, lw_calc_min, lw_calc_max
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
    tp_sfc, humid_sfc, press
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

def test_db(con, arquivo, est, ano, mes):
    """
    Testa conexão e seleções
    """
    QUERY = """
    SELECT *
    FROM '{arquivo}'
    WHERE acronym = '{est}'
    AND year = 2025
    AND day = 3
    AND min = 870
    ORDER BY timestamp
    """
    q = QUERY.format(
        arquivo=arquivo,
        est=est,
        ano=ano,
        mes=mes
    )
    out = con.execute(q).df()
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    print(out)

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

    # LINHA 1
    l1 = (
        f"{day:3d}"
        f"{minute:5d}"
        f"{int(row.glo_avg):7d}"
        f"{row.glo_std:6.1f}"
        f"{int(row.glo_min):5d}"
        f"{int(row.glo_max):5d}"
        f"{int(row.dir_avg):7d}"
        f"{row.dir_std:6.1f}"
        f"{int(row.dir_min):5d}"
        f"{int(row.dir_max):5d}"
    )

    # LINHA 2
    l2 = (
        f"{'':9}"
        f"{int(row.dif_avg):6d}"
        f"{row.dif_std:6.1f}"
        f"{int(row.dif_min):5d}"
        f"{int(row.dif_max):5d}"
        f"{int(row.lw_calc_avg):7d}"
        f"{row.lw_calc_std:6.1f}"
        f"{int(row.lw_calc_min):5d}"
        f"{int(row.lw_calc_max):5d}"
        f"{row.tp_sfc:9.1f}"
        f"{row.humid_sfc:6.1f}"
        f"{int(row.press):5d}"
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
    # Atualizar ano e mês do header - o resto é constante
    linhas = header.splitlines()
    # pegar valores existentes da linha original
    partes = linhas[1].split()
    cod = int(partes[0])
    ultimo = int(partes[3])
    # reconstruir preservando colunas
    linhas[1] = f"{cod:3d}{mes:3d}{ano:5d}{ultimo:3d}"
    header = "\n".join(linhas) + "\n"

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
    Interpola dados meteorológicos de 10 min para 1 min.
    A interpolação só ocorre entre pontos consecutivos de 10 min.
    Gaps maiores não são interpolados.
    """
    if len(df_md) == 0:
        return df_md
    df = df_md.copy()
    df = df.set_index("timestamp")
    # criar grade de 1 minuto
    idx = pd.date_range(
        start=df.index.min(),
        end=df.index.max(),
        freq="1min"
    )
    df = df.reindex(idx)
    # só interpola até 9 minutos consecutivos
    cols = ["tp_sfc", "humid_sfc", "press"]
    for c in cols:
        df[c] = df[c].interpolate(method="time", limit=9)
    df = df.reset_index().rename(columns={"index":"timestamp"})
    return df

def ajustar_final_md(df):
    """
    Preenche os minutos finais do dia (até 23:59) copiando o último valor
    válido das variáveis MD se ele estiver a até 10 minutos do final.
    Zerar valores negativos de direta
    """

    colunas_md = ["tp_sfc", "humid_sfc", "press"]

    if len(df) == 0:
        return df

    df = df.copy()

    fim = df["timestamp"].max()

    # último registro válido das variáveis MD
    ultima_linha = df[colunas_md].dropna(how="all").last_valid_index()

    if ultima_linha is None:
        return df

    ultimo_ts = df.loc[ultima_linha, "timestamp"]

    diff = (fim - ultimo_ts).total_seconds() / 60

    # só completa se faltar até 10 min
    if 0 < diff <= 10:
        mask = (df["timestamp"] > ultimo_ts) & (df["timestamp"] <= fim)

        for c in colunas_md:
            df.loc[mask, c] = df.loc[ultima_linha, c]

    # radiação direta não pode ser negativa
    for c in ["dir_avg", "dir_min", "dir_max"]:
        if c in df.columns:
            df.loc[df[c] < 0, c] = 0

    return df

def garantir_grade_completa(df, ano, mes):
    """
    Garante que o DataFrame contenha todos os minutos do mês
    (grade temporal contínua de 1 minuto).
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
    # salva último dado real antes do reindex
    ultimo_ts = df.index.max()

    df = df.reindex(idx)

    # -------- regra especial do final do mês --------
    ultimo_dia = fim.normalize()

    ts_2350 = ultimo_dia + pd.Timedelta(hours=23, minutes=50)

    if ultimo_ts == ts_2350:
        valor_2350 = df.loc[ts_2350]

        idx_extra = pd.date_range(
            ts_2350 + pd.Timedelta(minutes=1),
            fim,
            freq="1min"
        )

        for t in idx_extra:
            df.loc[t] = valor_2350
    # -----------------------------------------------

    df = df.reset_index().rename(columns={"index":"timestamp"})

    return df

def preencher_missing(df):
    """
    Substitui valores ausentes (NaN) pelos códigos de dados faltantes
    definidos no formato BSRN:
    -99.9 para variáveis float (F5.1)
    -999 para variáveis inteiras (I4).
    """

    missing_float = -99.9
    missing_int = -999

    float_cols = [
        'glo_std',
        'dir_std',
        'dif_std',
        'lw_calc_std',
        'tp_sfc',
        'humid_sfc'
    ]

    int_cols = [
        'glo_avg','glo_min','glo_max',
        'dir_avg','dir_min','dir_max',
        'dif_avg','dif_min','dif_max',
        'lw_calc_avg','lw_calc_min','lw_calc_max',
        'press'
    ]

    for c in float_cols:
        if c in df.columns:
            df[c] = df[c].fillna(missing_float)

    for c in int_cols:
        if c in df.columns:
            df[c] = df[c].fillna(missing_int).round().astype(int)

    return df