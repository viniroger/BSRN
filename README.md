# Formatação BSRN

- O manual GCOS-174 explica o formato do arquivo.
- O manual WMO-1274 explica detalhes sobre a rede BSRN.
- O código fcheck.c serve para checar se o formato do arquivo está adequado.
- O formato do station-to-archive também está explicado de maneira resumida neste [link]
(https://bsrn.awi.de/data/station-to-archive-file-format)
 - Arquivos no [Gdrive](https://drive.google.com/drive/folders/1txJNvXiqJJItdo5I4M-hYXSuo1cYBHj-?usp=drive_link)

Ação: utilizar os arquivos SONDA formatados que estão no FTP /restricted/dados/sonda/dados_formatados/[EST]/solarimetricos, ONDE EST = BRB, PTR ou SMS. Escolher um mês para desenvolver o código e depois André ajusta e processa os demais meses.

## Planejamento

Fazer desenvolvimento e testes com BRB 03 2019

- Versionar e subir (como repositório privado) esse código
- Gerar arquivo padrão de cabeçalho para cada estação (considerar que linha 2 precisa mudar em todos os arquivos nos campos mês e ano)
- Entender a tal saída do SONDA translator (como pegar e formato) para servir de entrada pro código
- Gerar script que lê os campos da entrada e salva com o formato da saída (SDT deixa em HL ou UTC? Na BSRN acho que é UTC)
- Compilar arquivo verificador em C no linux e checar arquivo gerado
- Testar para outros meses e para todas as estações
- Fechar scripts e documentação
