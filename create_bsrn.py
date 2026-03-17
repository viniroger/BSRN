# Script para ler arquivos formatados do sonda translator
# e gerar arquivos formato BSRN

from helpers.bsrn import *

# Caminho absoluto - alterar para o seu caso
path = "/home/vinicius/Documentos/labren/sonda-translator/output/sonda-formatados/"

# Receber estação, ano e mês como parâmetros
est, ano, mes = ler_argumentos()
print(f"ESTAÇÃO {est}  |  ANO {ano}  |  MES {mes:02d}")

# Nomes dos arquivos a serem usados
arquivo_solar = f"{path}/{est}/Solarimetricos/{ano}/{est}_{ano}_{mes:02d}_SD_formatado.csv"
arquivo_meteo = f"{path}/{est}/Meteorologicos/{ano}/{est}_{ano}_{mes:02d}_MD_formatado.csv"

# Leitura dos dados
SD = get_csv(arquivo_solar, est)
MD = get_csv(arquivo_meteo, est)

# Juntar dataframes MD e SD
df = juntar_sd_md(SD, MD, est, ano, mes)
# Ajustes
df = preencher_missing(df)
# Gerar arquivo
gerar_arquivo(df, est, ano, mes)
