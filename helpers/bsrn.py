'''
Helper functions for generate BSRN files
author: Vinicius Roggério da Rocha
e-mail: vinicius.rocha@inpe.br
version: 0.0.1
date: 2026-03-05
'''

import sys
import pandas as pd
from pathlib import Path

def read_args():
    """
    Receber argumentos em linha de comando:
    estação, ano e mês
    """
    if len(sys.argv) != 4:
        print("Uso: python script.py EST ANO MES")
        sys.exit(1)
    est = sys.argv[1]
    ano = int(sys.argv[2])
    mes = int(sys.argv[3])
    
    return est, ano, mes

def get_csv(arquivo, est, sep=","):
    """
    Lê um CSV cuja primeira linha é o cabeçalho e
    os dados válidos são apenas as linhas que começam com 'est'.
    """
    with open(arquivo, "r", encoding="utf-8") as f:
        linhas = f.readlines()
    header = linhas[0].strip()
    # filtra apenas linhas de dados
    dados = [l for l in linhas[1:] if l.strip().startswith(est)]
    # monta um "csv temporário" em memória
    conteudo = "\n".join([header] + dados)
    from io import StringIO
    df = pd.read_csv(StringIO(conteudo), sep=sep)
    return df

def merge_sd_md(SD, MD, est, ano, mes):
    """
    Junta SD e MD garantindo todos os minutos do mês.
    """
    SD["timestamp"] = pd.to_datetime(SD["timestamp"])
    MD["timestamp"] = pd.to_datetime(MD["timestamp"])
    # remove duplicatas
    SD = SD.drop_duplicates("timestamp")
    MD = MD.drop_duplicates("timestamp")
    # início e fim do mês
    inicio = pd.Timestamp(ano, mes, 1, 0, 0)
    fim = (inicio + pd.offsets.MonthEnd(1)).replace(hour=23, minute=59)
    # todos os minutos do mês
    timestamps = pd.date_range(inicio, fim, freq="1min")
    # dataframe base
    base = pd.DataFrame({
        "timestamp": timestamps,
        "acronym": est
    })
    # adiciona colunas temporais
    base["year"] = base["timestamp"].dt.year
    base["day"] = base["timestamp"].dt.dayofyear
    base["min"] = base["timestamp"].dt.hour * 60 + base["timestamp"].dt.minute
    # merge SD
    df = base.merge(SD, on="timestamp", how="left", suffixes=("", "_sd"))
    # apenas colunas novas de MD
    cols_novas = MD.columns.difference(df.columns)
    df = df.merge(MD[["timestamp"] + list(cols_novas)], on="timestamp", how="left")
    return df

def fill_missing(df):
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

def write_rows(row):
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

def create_file(df, estacao, ano, mes):
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
    # criar diretório de saída, se não houver
    output_dir = Path('out')
    output_dir.mkdir(parents=True, exist_ok=True)
    # escrever arquivo com newline="\n" para garantir que o caractere
    # de end-of-line esteja no formato exigido ("just LF")
    nome = f"{output_dir}/{estacao.lower()}{mes:02d}{str(ano)[-2:]}.dat"
    with open(nome, "w", newline="\n") as f:
        f.write(header)
        for row in df.itertuples():
            l1, l2 = write_rows(row)
            f.write(l1 + "\n")
            f.write(l2 + "\n")
    print(f"Arquivo salvo: {nome}")
