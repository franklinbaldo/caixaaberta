---
title: Cadastro Nacional de Obras (CNO)
---

O Caixa Aberta usa o Cadastro Nacional de Obras apenas como fonte pública de enriquecimento. A fonte canônica continua sendo a Receita Federal, catalogada no Portal Brasileiro de Dados Abertos em <https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-de-obras-cno>.

## Ingestão

O endereço físico do recurso não é tratado como contrato permanente. `src/cno_ingest.py` consulta a API pública do catálogo do dados.gov.br, encontra o recurso de dados do conjunto e só então baixa o arquivo. `CNO_SOURCE_URL` existe para teste, recuperação operacional e eventual mudança do catálogo, não como requisito normal de produção.

O snapshot só é aceito quando o ZIP contém as cinco relações documentadas pela Receita: `CNO.CSV`, `CNO_AREAS.CSV`, `CNO_CNAES.CSV`, `CNO_VINCULOS.CSV` e `CNO_TOTAIS.CSV`. Os bytes do ZIP são preservados junto da extração e `source.json` registra o recurso efetivamente usado.

## Semântica

CNO é cadastro de obras e responsáveis, não um acompanhamento físico detalhado da construção. O dado publicado pela Receita é declaratório/cadastral e deve ser apresentado assim. Campos omitidos pela própria fonte não devem ser reconstruídos pelo Caixa Aberta.

A cardinalidade também é parte do dado: `CNO_AREAS` pode ter várias linhas para um mesmo CNO. As etapas seguintes devem preservar isso em vez de achatar silenciosamente a relação.
