# Log

## 2026-09-01

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
