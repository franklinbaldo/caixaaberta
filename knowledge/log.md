# Log

## 2026-09-01

- Os CSVs saíram do versionamento. Commitá-los foi um erro de arquitetura: 6,9
  MB por retrato, num histórico que o git guarda para sempre e ninguém
  consulta — até 2,5 GB/ano no pior caso. O Archive já versiona e serve, e
  passou a receber `imoveis_csv_bruto.zip` junto do Parquet. Dado no Archive,
  código no git.

- O anti-bot da Caixa foi contornado, e a descoberta não foi teimosia: o
  bloqueio pune sessão reusada e User-Agent incoerente, não volume. Sessão
  nova por requisição mais cabeçalhos de navegador coerentes levam 27 UFs em
  86 segundos. Os números estão em `concepts/fonte-caixa.md`.

- Com dado de 2026 no lugar do de 2022, o acervo dobra (25.687 imóveis) e a
  geocodificação melhora sozinha: 65,9% ao nível de logradouro, contra 42,8%.
  Endereço da Caixa hoje é mais bem escrito do que era.

- As quatro modalidades documentadas passaram a cobrir 100% do dado. O aviso
  de modalidade desconhecida, que disparava com os CSVs de 2022, silenciou.

- O bundle passou a ser verificado contra o código, e não só contra si mesmo:
  `scripts/check_bundle_contract.py` recusa divergência no identificador do
  Archive (que tinha quatro cópias), nas colunas obrigatórias e nas
  modalidades. A direção é sempre bundle verifica código — o bundle não gera
  código de produção, porque um erro de markdown viraria corrupção de dado.

- O script roda isolado via PEP 723, o que mantém `okf-parser` fora do
  `pyproject.toml`: o pipeline continua instalável em 3.10.

- Tipos de domínio: `Modalidade`, `Armadilha` e `Consulta`. Os seis primeiros
  tipos descreviam a mecânica do pipeline; nenhum dizia nada sobre imóveis.

- As armadilhas saíram de medição, não de intuição: 4.120 imóveis de RO, SP e
  BA baixados em 01/09/2026. Foi assim que apareceu o fato de que 3 das 4
  modalidades anunciam lance mínimo, e não preço.

- `okf-parser init` gerou os specs e os `.schema.sql`; o `check` passou a rodar
  com `--require-spec --normative-spec`, que exige spec para todo tipo — o
  próprio tipo `Spec` incluído.

- Primeiro bundle. Escrito depois que o pipeline passou a baixar os dados da
  Caixa a cada execução: antes disso, descrever a proveniência teria sido
  descrever a republicação de um recorte de 2022.

- A geocodificação ficou como conceito próprio, e não como um parágrafo dentro
  do pipeline, porque é a etapa com cobertura parcial — quem consome o dataset
  precisa poder ler sobre ela isoladamente.
