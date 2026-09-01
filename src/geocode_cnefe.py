"""Geolocalização por CNEFE, sem rede no caminho crítico.

As coordenadas vêm do Cadastro Nacional de Endereços para Fins Estatísticos
(CNEFE/IBGE), na versão padronizada que o IPEA publica como Parquet para o
pacote R `geocodebr` (https://github.com/ipeaGIT/geocodebr). O pacote é R, mas
todo o seu casamento de endereços é SQL sobre esses Parquets — aqui o dado é
consumido direto pelo DuckDB que este projeto já carrega, sem passar por R.

Substitui a consulta linha a linha ao Nominatim, que resolvia 11,4% dos
endereços em pouco mais de cinco horas por execução.
"""

from __future__ import annotations

import shutil
import urllib.request
from pathlib import Path

import duckdb
import pandas as pd

DATA_RELEASE = "v0.4.1"
DATA_URL = (
    "https://github.com/ipeaGIT/padronizacao_cnefe/releases/download/"
    "{release}/{tabela}.parquet"
)
CACHE_DIR = Path("cnefe_cache")

# Cascata de casamento, da chave mais específica para a mais grosseira. Cada
# etapa só recebe os endereços que a anterior não resolveu, e o rótulo entra no
# Parquet publicado para que ninguém confunda porta com centroide de município.
CASCATA = (
    ("logradouro_localidade", "municipio_logradouro_localidade",
     ("estado", "municipio", "logradouro", "localidade")),
    ("logradouro", "municipio_logradouro_localidade",
     ("estado", "municipio", "logradouro")),
    ("localidade", "municipio_localidade",
     ("estado", "municipio", "localidade")),
    ("municipio", "municipio", ("estado", "municipio")),
)

PRECISIONS = tuple(nome for nome, _, _ in CASCATA)

# Abreviações que a Caixa usa e o CNEFE não. Cada par vale cobertura medida:
# sem elas, o casamento por logradouro cai de 42,6% para 33,6%.
_ABREVIACOES = (
    (r"^AV\.? ", "AVENIDA "),
    (r"^R\.? ", "RUA "),
    (r"^TV\.? ", "TRAVESSA "),
    (r"^PC\.? ", "PRACA "),
    (r"^ROD\.? ", "RODOVIA "),
    (r"^EST\.? ", "ESTRADA "),
    (r"^AL\.? ", "ALAMEDA "),
)


def _normalizado(expressao: str) -> str:
    """SQL que põe um texto na forma que os dois lados do join compartilham."""
    sql = f"upper(strip_accents(trim({expressao})))"
    for padrao, troca in _ABREVIACOES:
        sql = f"regexp_replace({sql}, '{padrao}', '{troca}')"
    return f"regexp_replace({sql}, '\\s+', ' ', 'g')"


def baixar_tabela(tabela: str, cache_dir: Path | str = CACHE_DIR) -> Path:
    """Baixa uma tabela do CNEFE padronizado, reaproveitando o cache local."""
    destino = Path(cache_dir) / DATA_RELEASE / f"{tabela}.parquet"
    if destino.exists():
        return destino

    destino.parent.mkdir(parents=True, exist_ok=True)
    url = DATA_URL.format(release=DATA_RELEASE, tabela=tabela)
    parcial = destino.with_suffix(".parquet.parcial")
    print(f"Baixando CNEFE {tabela} ({DATA_RELEASE})...")
    with urllib.request.urlopen(url) as resposta, parcial.open("wb") as saida:
        shutil.copyfileobj(resposta, saida)
    parcial.replace(destino)
    return destino


def geocodificar(
    df: pd.DataFrame, cache_dir: Path | str = CACHE_DIR, conn=None
) -> pd.DataFrame:
    """Preenche latitude, longitude, precisao e desvio_metros por CNEFE.

    Só toca nas linhas sem latitude. Devolve o DataFrame com as quatro colunas
    preenchidas até onde a cascata alcança; o que nenhuma etapa resolve fica
    nulo, e `precisao` diz por qual chave cada coordenada foi encontrada.
    """
    conn = conn or duckdb.connect()
    resultado = df.copy()
    for coluna in ("latitude", "longitude", "desvio_metros"):
        if coluna not in resultado.columns:
            resultado[coluna] = pd.NA
        resultado[coluna] = pd.to_numeric(resultado[coluna], errors="coerce")
    if "precisao" not in resultado.columns:
        resultado["precisao"] = pd.NA
    resultado["precisao"] = resultado["precisao"].astype("object")

    entrada = resultado.reset_index(names="_linha")
    conn.register("entrada", entrada)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE alvo AS
        SELECT
            _linha,
            {_normalizado('estado')} AS estado,
            {_normalizado('cidade')} AS municipio,
            {_normalizado("split_part(endereco, ',', 1)")} AS logradouro,
            {_normalizado("coalesce(bairro, '')")} AS localidade
        FROM entrada
        WHERE latitude IS NULL
        """
    )

    pendentes = conn.execute("SELECT count(*) FROM alvo").fetchone()[0]
    if not pendentes:
        return resultado

    for precisao, tabela, chaves in CASCATA:
        restantes = conn.execute("SELECT count(*) FROM alvo").fetchone()[0]
        if not restantes:
            break

        caminho = baixar_tabela(tabela, cache_dir)
        colunas = ", ".join(
            f"{_normalizado(chave)} AS {chave}" for chave in chaves
        )
        juncao = " AND ".join(f"a.{chave} = c.{chave}" for chave in chaves)
        agrupamento = ", ".join(chaves)

        achados = conn.execute(
            f"""
            WITH cnefe AS (
                SELECT {colunas}, lat, lon, desvio_metros
                FROM read_parquet('{caminho.as_posix()}')
            ),
            consolidado AS (
                SELECT {agrupamento},
                       avg(lat) AS lat,
                       avg(lon) AS lon,
                       max(desvio_metros) AS desvio_metros
                FROM cnefe
                GROUP BY {agrupamento}
            )
            SELECT a._linha, c.lat, c.lon, c.desvio_metros
            FROM alvo a JOIN consolidado c ON {juncao}
            """
        ).df()

        if not achados.empty:
            posicoes = achados["_linha"].to_numpy()
            resultado.loc[posicoes, "latitude"] = achados["lat"].to_numpy()
            resultado.loc[posicoes, "longitude"] = achados["lon"].to_numpy()
            resultado.loc[posicoes, "desvio_metros"] = achados[
                "desvio_metros"
            ].to_numpy()
            resultado.loc[posicoes, "precisao"] = precisao

            conn.register("achados", achados[["_linha"]])
            conn.execute(
                """
                CREATE OR REPLACE TEMP TABLE alvo AS
                SELECT * FROM alvo
                WHERE _linha NOT IN (SELECT _linha FROM achados)
                """
            )

        print(
            f"CNEFE {precisao}: {len(achados)} endereços resolvidos, "
            f"{restantes - len(achados)} pendentes."
        )

    return resultado


def cobertura(df: pd.DataFrame) -> dict[str, int]:
    """Quantos endereços cada nível de precisão resolveu."""
    if "precisao" not in df.columns:
        return {}
    contagem = df["precisao"].value_counts().to_dict()
    return {nivel: int(contagem.get(nivel, 0)) for nivel in PRECISIONS}
