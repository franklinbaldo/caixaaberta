"""Testes da geocodificação por CNEFE, sem rede.

As tabelas do CNEFE são substituídas por Parquets sintéticos escritos em
tmp_path, com o mesmo esquema e os mesmos nomes de arquivo do release do IPEA.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from geocode_cnefe import CASCATA, DATA_RELEASE, baixar_tabela, cobertura, geocodificar


def _escreve_tabela(cache_dir: Path, nome: str, linhas: list[dict]):
    destino = cache_dir / DATA_RELEASE / f"{nome}.parquet"
    destino.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(linhas).to_parquet(destino, index=False)


@pytest.fixture
def cnefe(tmp_path):
    """Um CNEFE mínimo, com uma linha por nível da cascata."""
    _escreve_tabela(
        tmp_path,
        "municipio_logradouro_localidade",
        [
            {
                "estado": "RO",
                "municipio": "PORTO VELHO",
                "logradouro": "AVENIDA SETE DE SETEMBRO",
                "localidade": "CENTRO",
                "lat": -8.76,
                "lon": -63.90,
                "desvio_metros": 30.0,
            },
            {
                "estado": "RO",
                "municipio": "PORTO VELHO",
                "logradouro": "RUA DAS ACACIAS",
                "localidade": "OUTRO BAIRRO",
                "lat": -8.70,
                "lon": -63.80,
                "desvio_metros": 50.0,
            },
        ],
    )
    _escreve_tabela(
        tmp_path,
        "municipio_localidade",
        [
            {
                "estado": "RO",
                "municipio": "PORTO VELHO",
                "localidade": "TRES MARIAS",
                "lat": -8.75,
                "lon": -63.85,
                "desvio_metros": 900.0,
            }
        ],
    )
    _escreve_tabela(
        tmp_path,
        "municipio",
        [
            {
                "estado": "RO",
                "municipio": "PORTO VELHO",
                "lat": -8.74,
                "lon": -63.88,
                "desvio_metros": 12000.0,
            }
        ],
    )
    return tmp_path


def _imovel(endereco, bairro, cidade="Porto Velho", estado="RO"):
    return {
        "endereco": endereco,
        "bairro": bairro,
        "cidade": cidade,
        "estado": estado,
        "latitude": None,
    }


def test_cascata_atribui_a_precisao_de_cada_nivel(cnefe):
    df = pd.DataFrame(
        [
            _imovel("AVENIDA SETE DE SETEMBRO, N. 100", "CENTRO"),
            _imovel("RUA DAS ACACIAS, N. 20", "BAIRRO QUE NAO EXISTE"),
            _imovel("RUA INEXISTENTE, N. 1", "TRES MARIAS"),
            _imovel("RUA INEXISTENTE, N. 2", "BAIRRO QUE NAO EXISTE"),
        ]
    )

    out = geocodificar(df, cache_dir=cnefe)

    assert out["precisao"].tolist() == [
        "logradouro_localidade",
        "logradouro",
        "localidade",
        "municipio",
    ]
    assert out.loc[0, "latitude"] == pytest.approx(-8.76)
    assert out.loc[0, "desvio_metros"] == pytest.approx(30.0)


def test_abreviacoes_da_caixa_casam_com_o_cnefe(cnefe):
    """A Caixa escreve 'AV.' e 'R.'; o CNEFE escreve por extenso."""
    df = pd.DataFrame(
        [
            _imovel("AV. SETE DE SETEMBRO, N. 100", "CENTRO"),
            _imovel("AV SETE DE SETEMBRO, N. 100", "Centro"),
            _imovel("R. DAS ACACIAS, N. 20", "OUTRO BAIRRO"),
        ]
    )

    out = geocodificar(df, cache_dir=cnefe)

    assert out["precisao"].tolist() == ["logradouro_localidade"] * 3


def test_acentos_e_caixa_nao_impedem_o_casamento(cnefe):
    df = pd.DataFrame([_imovel("Avenida Sete de Setembro, n. 5", "Centro")])

    out = geocodificar(df, cache_dir=cnefe)

    assert out.loc[0, "precisao"] == "logradouro_localidade"


def test_coordenada_existente_e_preservada(cnefe):
    df = pd.DataFrame([_imovel("AVENIDA SETE DE SETEMBRO, N. 100", "CENTRO")])
    df["latitude"] = -1.0
    df["longitude"] = -2.0

    out = geocodificar(df, cache_dir=cnefe)

    assert out.loc[0, "latitude"] == -1.0
    assert pd.isna(out.loc[0, "precisao"])


def test_endereco_fora_do_cnefe_fica_nulo(cnefe):
    df = pd.DataFrame([_imovel("RUA QUALQUER, N. 1", "QUALQUER", cidade="Cidade X")])

    out = geocodificar(df, cache_dir=cnefe)

    assert pd.isna(out.loc[0, "latitude"])
    assert pd.isna(out.loc[0, "precisao"])


def test_cobertura_conta_todos_os_niveis(cnefe):
    df = pd.DataFrame([_imovel("AVENIDA SETE DE SETEMBRO, N. 1", "CENTRO")])

    resultado = cobertura(geocodificar(df, cache_dir=cnefe))

    assert resultado == {
        "logradouro_localidade": 1,
        "logradouro": 0,
        "localidade": 0,
        "municipio": 0,
    }


def test_baixar_tabela_reusa_o_cache_sem_rede(cnefe):
    """Um arquivo já em cache não dispara download."""
    caminho = baixar_tabela("municipio", cache_dir=cnefe)

    assert caminho.exists()
    assert caminho.name == "municipio.parquet"


def test_a_cascata_vai_do_especifico_ao_grosseiro():
    """A ordem é o contrato: uma inversão degradaria toda coordenada."""
    chaves = [len(chaves) for _, _, chaves in CASCATA]

    assert chaves == sorted(chaves, reverse=True)
