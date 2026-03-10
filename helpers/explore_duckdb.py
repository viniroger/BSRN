# Script para explorar arquivo parquet usando duckdb

import duckdb

path = "/home/vinicius/Documentos/labren/sonda-translator/output/sonda-banco-dados/"
arquivo = f"{path}/Meteorologica.parquet"
#arquivo = f"{path}/Solarimetrica.parquet"

# cria conexão duckdb (em memória)
con = duckdb.connect()

print("\n=== Estrutura do arquivo (schema) ===")
schema = con.execute(f"""
DESCRIBE SELECT * FROM '{arquivo}'
""").fetchall()

for col in schema:
    print(col)

print("\n=== Número de registros ===")
n = con.execute(f"""
SELECT COUNT(*) FROM '{arquivo}'
""").fetchone()[0]

print(f"Total de linhas: {n}")

print("\n=== Primeiras 10 linhas ===")
dados = con.execute(f"""
SELECT * FROM '{arquivo}'
LIMIT 10
""").fetchall()

for linha in dados:
    print(linha)

print("\n=== Nomes das colunas ===")
colunas = [c[0] for c in schema]
print(colunas)

print("\nArquivo carregado. Agora você pode fazer consultas SQL usando:")
print("con.execute(\"SELECT ... FROM 'Meteorologica.parquet'\")")
