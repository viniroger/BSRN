# Script para ler arquivos formatados do sonda translator
# e gerar arquivos formato BSRN

from helpers.bsrn import *

# Caminho absoluto de onde estão os dados a serem utilizados (formatados)
path = Path("/home/vinicius/Documentos/labren/sonda-translator/output/sonda-formatados/")

# Receber estação, ano e mês como parâmetros
est, ano, mes = read_args()
print(f"Gerando arquivo para {est} {ano}-{mes}...")

# Nomes dos arquivos a serem usados
arquivo_solar = f"{path}/{est}/Solarimetricos/{ano}/{est}_{ano}_{mes:02d}_SD_formatado.csv"
arquivo_meteo = f"{path}/{est}/Meteorologicos/{ano}/{est}_{ano}_{mes:02d}_MD_formatado.csv"

# Leitura dos dados
SD = get_csv(arquivo_solar, est)
MD = get_csv(arquivo_meteo, est)

# Juntar dataframes MD e SD
df = merge_sd_md(SD, MD, est, ano, mes)
# Ajustes
df = fill_missing(df)
# Gerar arquivo
create_file(df, est, ano, mes)
