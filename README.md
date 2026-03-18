# Formatação BSRN

Os dados coletados nas estações automáticas da rede SONDA são salvos (em horário UTC) no servidor FTP, após verificação em rotinas de controle, e são gerados arquivos mensais meteorológicos, solarimétricos e anemométricos, conforme a estação. Depois, deve-se executar o [SONDA Translator](https://github.com/labren/sonda-translator/) (sdt) para organizar os dados em arquivos formatados, particularmente para as estações PTR, BRB e SMS.

O objetivo dos scripts desse repositório está em consultar os arquivos formatados após tratamento com o sdt e gerar arquivos mensais no formato "station-to-archive" para envio à BSRN. Mais informações nos manuais [GCOS-174](https://bsrn.awi.de/fileadmin/user_upload/bsrn.awi.de/Publications/gcos-174.pdf) e [WMO-1274](https://epic.awi.de/id/eprint/45991/1/McArthur.pdf), asism como na documentação da BSRN sobre o formato [station-to-archive](https://bsrn.awi.de/data/station-to-archive-file-format).

# Instalação

Além das bibliotecas padrão do python, é necessário também a biblioteca do pandas. Nesse desenvolvimento, foram usados Python 3.13.11 e pandas 3.0.0, mas se não for uma instalação muito antiga, bem provável funcionar com outras versões.

Com seu ambiente configurado, siga para seu diretório de trabalho em seu terminal e clone esse repositório através do seguinte comando:

`git clone https://github.com/viniroger/bsrn`

Entre no diretório gerado e edite o script principal (create_bsrn.py) para alterar o caminho local de onde estão os dados a serem utilizados (formatados) - variável "path", linha 7. Esse diretório deve ser sempre atualizado com novos dados para gerar novos arquivos, mantendo a estrutura de diretórios originalmente gerada pelo sdt: sonda/formatados/EST/TIPO/ANO/EST_ANO_MES_SIGLATIPO_formatado.csv

# Execução

Dentro do diretório, execute o script principal e os respectivos argumentos de entrada: sigla da estação (3 letras maiúsculas), ano (4 dígitos) e mês (2 dígitos) a serem trabalhados. Exemplo:

`python create_bsrn.py PTR 2020 04`

O que esse script faz:

1. método `get_csv` - lê os arquivos formatados solarimétrico (SD) e meteorológico (MD)
2. método `merge_sd_md` - forma um dataframe com intervalo de 1 minuto com todos os dados disponíveis
3. método `fill_missing` - preenche campos sem dados com os códigos definidos pela documentação BSRN
4. método `gerar_arquivo` - grava arquivo no formato BSRN no subdiretório "out"

Nesse exemplo, o arquivo gerado deve ser "ptr0420.dat".

# Checagem

Depois de gerado, o arquivo deverá passar por checagem através do script em linguagem C `f_check`, disponível no site da BSRN nesse [link](https://bsrn.awi.de/software/). No Linux, deve-se instalar os pacotes necessários e compilar o código para gerar o arquivo executável através dos seguitnes comandos:

```
sudo apt install build-essential
gcc f_check_V3_4.c -o f_check_V3_4.exe
```

Sua execução, ainda considerando o arquivo de exemplo gerado, será:

`./f_check_V3_3.exe out/ptr0420.dat`

A saída deve conter os problemas a serem resolvidos na geração do arquivo ou então um diagnóstico de que o tamanho das linhas (line length), caracteres irregulares (illegal characters) e formato da linha (line format) estão OK.

# SONDA Translator

O [SONDA Translator](https://github.com/labren/sonda-translator/) está disponível nesse link. A documentação a seguir apresenta como rodar localmente a partir da clonagem dos arquivos do Github.

## Fluxograma

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
gerar_web.py
```

## Instalação

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

Para funcionar parecido com a documentação original, devem ser alterados os imports internos para imports relativos de pacotes (colocar um ponto na frente dos imports dos arquivos .py que tem na pasta, ex.: from .logger import setup_logger), alterando os seguintes arquivos:

- `__main__.py`
- prequalificaDado.py
- processaDado.py
- tratar_quarentena.py

Outras alterações no código para gerar listas de arquivos com todos os campos preenchidos adequadamente:

- caminho absoluto/relativo para os arquivos .py quando importados
- (scan_ftp) antes: '_AMB_' e '_RAD_', agora tirei último underline
- (scan_ftp) nova função para extrair estação

Comparando com o original, em termos de execução, agora tem que usar "-m" antes de sdt e especificar o caminho dos dados através do parâmetro "-ftp_dir". O passo a passo a seguir já inclui essas diferenças.

## Execução local

1. Ativar ambiente virtual e ir para diretório dos scripts

```bash
conda deactivate
conda activate sdt
cd ~/Documentos/labren/sonda-translator
```

2. Montar lista de arquivos

Os nomes com paths dos arquivos .dat (ASCII), com os dados organizados como estação/ano, são salvos em formato JSON:

`python -m sdt -scan_ftp -ftp_dir ~/dados/historico`

O script percorre recursivamente o diretório -ftp_dir, identifica arquivos .dat, extrai metadados (estação, tipo, ano, caminho completo) e gera arquivo índice: sdt/json/arquivos_ftp.json. Se não rodar isso, o __main__.py abre o arquivo já gerado anteriormente.

3. Gerar sonda-formatados

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

4. (extra) Criar banco de dados

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

Obs. 1: no computador original, os arquivos a serem consultados ficam em "/restricted/dados/sonda/dados_formatados/[EST]/solarimetricos" onde EST = BRB, PTR ou SMS.

Obs. 2: Na versão original, arquivos sem header (ASCII bruto como BRB e PTR antigo) são desconsiderados (BRB) ou dão erro (PTR). Esses arquivos originais (como PTR_2018_001_a_343.dat e PTR17_255a272.dat) constam da lista JSON original.

Obs. 3: Não existe alteração de valores, filtragens ou coisas do tipo para gerar o db e o parquet, só mudança de formato.
