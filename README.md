# Formatação BSRN

Os dados coletados nas estações automáticas da rede SONDA são salvos no servidor FTP após rotinas de controle e gerados arquivos mensais meteorológicos, solarimétricos e anemométricos, conforme a estação. Depois, deve-se executar o SONDA Translator (sdt) para organizar os dados em uma base formatada.

Após atualização da base usando o sdt, os dados das estações PTR, BRB e SMS devem ser consultados dessa base formatada e enviados para a [BSRN](https://www.monolitonimbus.com.br/bsrn) no formato "station-to-archive". Esse procedimento é o objetivo desses scripts.

## SONDA Translator

### Sobre

A [documentação do SONDA Translator](https://github.com/labren/sonda-translator/) está nesse link. Fluxograma de funcionamento do sdt:

```
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
```

### Execução local e alterações no código

Para execução local, deve-se clonar o código do github, criar um ambiente virtual e instalar as dependências:

```bash
mkdir labren
cd labren
git clone https://github.com/labren/sonda-translator.git
cd sonda-translator
conda deactivate
conda create -n sdt python=3.11 -y
conda activate sdt
pip install -r requirements.txt
```

Depois, tiveram algumas diferenças de execução com relação à documentação original. Para funcionar parecido, tive que transformar os imports internos em imports relativos de pacotes (colocar um ponto na frente dos imports dos arquivos .py que tem na pasta, ex.: from .logger import setup_logger), alterando os seguintes arquivos:

- `__main__.py`
- prequalificaDado.py
- processaDado.py
- tratar_quarentena.py

Outras alterações no código para gerar listas de arquivos com todos os campos preenchidos adequadamente:

- caminho absoluto/relativo para os arquivos .py quando importados
- (scan_ftp) antes: '_AMB_' e '_RAD_', agora tirei último underline
- (scan_ftp) nova função para extrair estação

Comparando com o original, em termos de execução, agora tem que usar "-m" antes de sdt e especificar o caminho dos dados através do parâmetro "-ftp_dir". O passo a passo a seguir já inclui essas diferenças.

### Passo a passo (execução local)

1. Montar lista de arquivos

Os nomes com paths dos arquivos .dat (ASCII), com os dados organizados como estação/ano, são salvos em formato JSON:

`python -m sdt -scan_ftp -ftp_dir ~/dados/historico`

O script percorre recursivamente o diretório -ftp_dir, identifica arquivos .dat, extrai metadados (estação, tipo, ano, caminho completo) e gera arquivo índice: sdt/json/arquivos_ftp.json. Se não rodar isso, o __main__.py abre o arquivo já gerado anteriormente.

2. Gerar sonda-formatados

Script processaDado.py (método processarArquivo() chamado indiretamente via __main__.py) lê o arquivo ASCII, identifica cabeçalho, padroniza nomes de colunas, converte dados para formato tabular, aplica pré-qualificação (prequalificarDado) e separa dados válidos e dados inválidos:

```bash
python -m sdt -estacao ptr -formatar -ftp_dir ~/dados/historico
python -m sdt -estacao brb -formatar -ftp_dir ~/dados/historico
python -m sdt -estacao sms -formatar -ftp_dir ~/dados/historico
```

A seguinte estrutura de diretórios é formada (pasta "output"):

```
sonda-formatados/
   ├── Meteorologica/
   ├── Solarimetrica/
   └── Anemometrica/
```

3. Criar banco de dados

Script gerar_base.py (chamado via opção no __main__.py por parâmetro --gerar_base) percorre sonda-formatados, agrupa dados por tipo, cria banco DuckDB (sonda-banco-dados), cria tabela correspondente, insere dados e exporta também para Parquet.

```bash
python -m sdt -tipo MD -gerar_base -ftp_dir ~/dados/historico
python -m sdt -tipo SD -gerar_base -ftp_dir ~/dados/historico
```

Os seguintes arquivos são gerados (tem o Anemometrica também mas não precisa pro BSRN):

```
sonda-formatados/
   ├── dbs
        ├── Meteorologica.db
        └── Solarimetrica.db
   ├── Meteorologica.parquet
   └── Solarimetrica.parquet
```

Obs.: no computador do Helvécio (OU FTP? VERIFICAR), os arquivos a serem consultados ficam em "/restricted/dados/sonda/dados_formatados/[EST]/solarimetricos" onde EST = BRB, PTR ou SMS.

## Leitura sdt e geração de arquivos BSRN

(Fazer desenvolvimento e testes com PTR 01 2019)

LISTA DE TAREFAS

- Gerar script que lê os campos da entrada e salva com o formato da saída (linha 2 do cabeçalho tem que mudar pq tem mês e ano do próprio arquivo; SDT deixa em HL ou UTC? Na BSRN acho que é UTC)
- Compilar arquivo verificador em C no linux e checar arquivo gerado
- Testar para outros meses e para todas as estações
- Fechar scripts e documentação
- Ver como atualizar base de dados do SONDA translator para poder gerar arquivos todo mês

## Mais informações

- O manual GCOS-174 explica o formato do arquivo.
- O manual WMO-1274 explica detalhes sobre a rede BSRN.
- O código fcheck.c serve para checar se o formato do arquivo está adequado.
- O formato do station-to-archive também está explicado de maneira resumida neste [link](https://bsrn.awi.de/data/station-to-archive-file-format).
 - Arquivos no [Gdrive](https://drive.google.com/drive/folders/1txJNvXiqJJItdo5I4M-hYXSuo1cYBHj-?usp=drive_link).
