# Formatação BSRN

- O manual GCOS-174 explica o formato do arquivo.
- O manual WMO-1274 explica detalhes sobre a rede BSRN.
- O código fcheck.c serve para checar se o formato do arquivo está adequado.
- O formato do station-to-archive também está explicado de maneira resumida neste [link](https://bsrn.awi.de/data/station-to-archive-file-format)
 - Arquivos no [Gdrive](https://drive.google.com/drive/folders/1txJNvXiqJJItdo5I4M-hYXSuo1cYBHj-?usp=drive_link)

Ação: utilizar os arquivos SONDA formatados que estão no FTP /restricted/dados/sonda/dados_formatados/[EST]/solarimetricos, ONDE EST = BRB, PTR ou SMS. Escolher um mês para desenvolver o código e depois André ajusta e processa os demais meses.

## Planejamento

Fazer desenvolvimento e testes com BRB 03 2019

- Entender a tal saída do SONDA translator (como pegar e formato) para servir de entrada pro código
- Gerar script que lê os campos da entrada e salva com o formato da saída (linha 2 do cabeçalho tem que mudar pq tem mês e ano do próprio arquivo; SDT deixa em HL ou UTC? Na BSRN acho que é UTC)
- Compilar arquivo verificador em C no linux e checar arquivo gerado
- Testar para outros meses e para todas as estações
- Fechar scripts e documentação
- Ver como atualizar base de dados do SONDA translator para poder gerar arquivos todo mês

## Dados SONDA translator

Preciso usar os dados formatados pelo SONDA translator (sdt) - [Documentação](https://github.com/labren/sonda-translator/). Fluxograma de funcionamento do sdt:

ASCII (.dat) locais
        ↓
scan_ftp.py  → cria json/arquivos_ftp.json
        ↓
__main__.py lê JSON
        ↓
processaDado.py → gera sonda-formatados/
        ↓
gerar_base.py → cria .db + .parquet
        ↓
(gerar_web.py opcional)

1. Montar lista de arquivos .dat (ASCII) com os dados (organizados como estação/ano):

python -m sdt -scan_ftp -ftp_dir /caminho/dos/dados

O script percorre recursivamente o diretório -ftp_dir, identifica arquivos .dat, extrai metadados (estação, tipo, ano, caminho completo) e gera arquivo índice: sdt/json/arquivos_ftp.json

Se não rodar isso, o __main__.py abre o arquivo já gerado anteriormente.

2. Gerar sonda-formatados

Script processaDado.py (método processarArquivo() chamado indiretamente via __main__.py) lê o arquivo ASCII, identifica cabeçalho, padroniza nomes de colunas, converte dados para formato tabular, aplica pré-qualificação (prequalificarDado) e separa dados válidos e dados inválidos.

sonda-formatados/
   ├── Meteorologica/
   ├── Solarimetrica/
   └── Anemometrica/

3. Criar banco de dados

Script gerar_base.py (chamado via opção no __main__.py por parâmetro --gerar_base) percorre sonda-formatados, agrupa dados por tipo, cria banco DuckDB (sonda-banco-dados), cria tabela correspondente, insere dados e exporta também para Parquet.

- Meteorologica.db e .parquet
- Solarimetrica.db e .parquet
- Anemometrica.db e .parquet

Obs: clonei e criei ambiente virtual, mas teve umas diferenças pra rodar com relação à documentação oriignal:

mkdir labren
cd labren
git clone https://github.com/labren/sonda-translator.git
cd sonda-translator
conda deactivate
conda create -n sdt python=3.11 -y
conda activate sdt
pip install -r requirements.txt
(Daqui pra frente, se der "python -m sdt" não carrega módulos, então fiz como segue)
cd sdt
python scan_ftp.py -ftp_dir ~/dados/historico (para gerar arquivo json, tive que mover pra pasta certa)
python __main__.py -scan_ftp -ftp_dir ~/dados/historico

Para funcionar igual a documentação, tive que transformar os imports internos em imports relativos de pacotes, alterando os seguintes arquivos (colocar um ponto na frente dos imports dos arquivos .py que tem na pasta, ex.: from .logger import setup_logger):

__main__.py
prequalificaDado.py
processaDado.py
tratar_quarentena.py

Então, o comando para criar o json ficou:

python -m sdt -scan_ftp -ftp_dir ~/dados/historico

No entanto, quando uso: 

python -m sdt -estacao cgr -tipo SD -ftp_dir ~/dados/historico

Fica assim:

Sonda-Translator - Processando arquivos com os seguintes parâmetros:
--------------------------------------------------
-ESTACAO: ['cgr']
-ANO: 
-TIPO: SD
-PARALLEL: False
-OUTPUT: output/sonda-formatados/
-FORMATAR: False
-OVERWRITE: False
-FTP_DIR: /home/vinicius/dados/historico
-SCAN_FTP: False
-QUARENTENA_TRATADO: False
-GERAR_BASE: False
-GERAR_WEB: False
--------------------------------------------------
Iniciando processamento...

Empty DataFrame
Columns: [id, estacao, tipo, ano, is_historico, caminho]
Index: []