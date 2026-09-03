---
type: Procedure
title: Cadastro Nacional de Obras (CNO)
description: Ingere e enriquece imóveis com o cadastro aberto de obras da Receita Federal
provider: Receita Federal / Portal Brasileiro de Dados Abertos
coverage: nacional
---

# Cadastro Nacional de Obras (CNO)

O Caixa Aberta usa o Cadastro Nacional de Obras apenas como fonte pública de enriquecimento. A fonte canônica continua sendo a Receita Federal, catalogada no Portal Brasileiro de Dados Abertos em <https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-de-obras-cno>.

## Ingestão

O endereço físico do recurso não é tratado como contrato permanente. `src/cno_ingest.py` consulta a API pública do catálogo do dados.gov.br, encontra o recurso de dados do conjunto e só então baixa o arquivo. `CNO_SOURCE_URL` existe para teste, recuperação operacional e eventual mudança do catálogo, não como requisito normal de produção.

O snapshot só é aceito quando o ZIP contém as cinco relações documentadas pela Receita: `CNO.CSV`, `CNO_AREAS.CSV`, `CNO_CNAES.CSV`, `CNO_VINCULOS.CSV` e `CNO_TOTAIS.CSV`. Os bytes do ZIP são preservados junto da extração e `source.json` registra o recurso efetivamente usado.

## Semântica

CNO é cadastro de obras e responsáveis, não um acompanhamento físico detalhado da construção. O dado publicado pela Receita é declaratório/cadastral e deve ser apresentado assim. Campos omitidos pela própria fonte não devem ser reconstruídos pelo Caixa Aberta.

A cardinalidade também é parte do dado: `CNO_AREAS` pode ter várias linhas para um mesmo CNO. A normalização preserva essas relações em Parquets separados em vez de achatá-las silenciosamente.

## Matching com os imóveis

O vínculo entre um imóvel da Caixa e um CNO é uma inferência do Caixa Aberta, não um identificador fornecido pelas fontes. O algoritmo primeiro bloqueia candidatos por UF, município e número do endereço; dentro desse bloco exige igualdade do logradouro normalizado, com e sem o tipo de logradouro. Bairro serve apenas para aumentar a força da evidência.

O Parquet principal recebe um CNO somente quando existe um vencedor único com score forte. Empates ficam `ambiguo`; candidatos únicos abaixo do limiar ficam `provavel`; ausência de candidato fica `sem_match`. Nenhum desses estados é convertido silenciosamente em certeza.

Todos os candidatos são gravados em `cno_matches_<data>.parquet`, com score, método e ranking. A publicação diária envia essa tabela ao Internet Archive junto do retrato principal, permitindo auditar por que um vínculo foi ou não aceito.
