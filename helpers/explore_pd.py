# Script para explorar arquivo parquet usando pandas

import pandas as pd

path = "/home/vinicius/Documentos/labren/sonda-translator/output/sonda-banco-dados/"
arquivo = f"{path}/Meteorologica.parquet"
#arquivo = f"{path}/Solarimetrica.parquet"

print("\n=== Lendo arquivo parquet ===")
df = pd.read_parquet(arquivo)

print("\n=== Estrutura do dataframe ===")
print(df.info())

print("\n=== Primeiras 10 linhas ===")
print(df.head(10))

print("\n=== Nomes das colunas ===")
print(df.columns.tolist())

print("\n=== Número de registros ===")
print(len(df))

print("\n=== Intervalo temporal ===")
print(df["timestamp"].min(), "->", df["timestamp"].max())

print("\n=== Estatísticas básicas ===")
print(df.describe())