# Script para ler parquet do sonda translator
# e gerar arquivos formato BSRN

import duckdb
from helpers.bsrn import *

path = "/home/vinicius/Documentos/labren/sonda-translator/output/sonda-banco-dados/"
arquivo_solar = f"{path}/Solarimetrica.parquet"
arquivo_meteo = f"{path}/Meteorologica.parquet"

estacoes = ['PTR', 'BRB', 'SMS']

con = duckdb.connect()

for ano in range(2019, 2027):
    for mes in range(1, 13):
        for est in estacoes:
            ano = 2025
            mes = 1
            print(f"ESTAÇÃO {est}  |  ANO {ano}  |  MES {mes:02d}")
            #test_db(con, arquivo_solar, est, ano, mes)
            #exit()
            SD = get_SD(con, arquivo_solar, est, ano, mes)
            MD = get_MD(con, arquivo_meteo, est, ano, mes)
            # checa se tem dados
            if len(SD)==0 and len(MD)==0:
                print('Sem dados para esse período/estação')
                continue
            # interpolar para 1 minuto
            MD = interpolar_md(MD)
            # Juntar dataframes MD e SD
            df = pd.merge(
                SD,
                MD,
                on="timestamp",
                how="outer"
            ).sort_values("timestamp")
            # Ajustes
            df = ajustar_final_md(df)
            df = garantir_grade_completa(df, ano, mes)
            df = preencher_missing(df)
            # Ordenar e gerar arquivo
            df = df.sort_values("timestamp")
            gerar_arquivo(df, est, ano, mes)
            exit()
