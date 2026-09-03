from datetime import date

import pandas as pd

from src.cno_match import match_properties, normalize_number, normalize_street


def _cno_files(tmp_path):
    cno = pd.DataFrame(
        [
            {
                "cno": "000000000001",
                "estado_normalizado": "RO",
                "municipio_normalizado": "PORTO VELHO",
                "numero_normalizado": "10",
                "logradouro_completo_normalizado": "RUA ALFA",
                "logradouro_normalizado": "ALFA",
                "bairro_normalizado": "CENTRO",
                "situacao_descricao": "ATIVA",
                "data_de_inicio_iso": date(2020, 1, 2),
                "area_total_num": 120.5,
                "nome": "OBRA ALFA",
            },
            {
                "cno": "000000000002",
                "estado_normalizado": "RO",
                "municipio_normalizado": "PORTO VELHO",
                "numero_normalizado": "20",
                "logradouro_completo_normalizado": "RUA BETA",
                "logradouro_normalizado": "BETA",
                "bairro_normalizado": "CENTRO",
                "situacao_descricao": "ENCERRADA",
                "data_de_inicio_iso": date(2019, 3, 4),
                "area_total_num": 80.0,
                "nome": "OBRA BETA",
            },
            {
                "cno": "000000000003",
                "estado_normalizado": "RO",
                "municipio_normalizado": "PORTO VELHO",
                "numero_normalizado": "30",
                "logradouro_completo_normalizado": "RUA DUPLA",
                "logradouro_normalizado": "DUPLA",
                "bairro_normalizado": "CENTRO",
                "situacao_descricao": "ATIVA",
                "data_de_inicio_iso": date(2022, 1, 1),
                "area_total_num": 50.0,
                "nome": "OBRA DUPLA A",
            },
            {
                "cno": "000000000004",
                "estado_normalizado": "RO",
                "municipio_normalizado": "PORTO VELHO",
                "numero_normalizado": "30",
                "logradouro_completo_normalizado": "RUA DUPLA",
                "logradouro_normalizado": "DUPLA",
                "bairro_normalizado": "CENTRO",
                "situacao_descricao": "ATIVA",
                "data_de_inicio_iso": date(2022, 2, 2),
                "area_total_num": 51.0,
                "nome": "OBRA DUPLA B",
            },
            {
                "cno": "000000000005",
                "estado_normalizado": "RO",
                "municipio_normalizado": "PORTO VELHO",
                "numero_normalizado": "40",
                "logradouro_completo_normalizado": "RUA GAMMA",
                "logradouro_normalizado": "GAMMA",
                "bairro_normalizado": "OUTRO",
                "situacao_descricao": "ATIVA",
                "data_de_inicio_iso": date(2024, 1, 1),
                "area_total_num": 60.0,
                "nome": "OBRA GAMMA",
            },
        ]
    )
    areas = pd.DataFrame(
        [
            {"cno": "000000000001", "categoria": "0 - Obra Nova", "destinacao": "0 - Residencial unifamiliar", "tipo_de_obra": "0 - Alvenaria"},
            {"cno": "000000000001", "categoria": "1 - Acréscimo", "destinacao": "0 - Residencial unifamiliar", "tipo_de_obra": "0 - Alvenaria"},
        ]
    )
    cno_path = tmp_path / "cno.parquet"
    areas_path = tmp_path / "cno_areas.parquet"
    cno.to_parquet(cno_path, index=False)
    areas.to_parquet(areas_path, index=False)
    return cno_path, areas_path


def test_normalizadores_entendem_formato_da_caixa():
    assert normalize_number("N. 0010") == "0010"
    assert normalize_number("S/N") == "SN"
    assert normalize_street("R. Álfa") == ("RUA ALFA", "ALFA")
    assert normalize_street("AV. Brasil") == ("AVENIDA BRASIL", "BRASIL")


def test_match_forte_enriquece_e_preserva_areas(tmp_path):
    cno_path, areas_path = _cno_files(tmp_path)
    properties = pd.DataFrame(
        [{"link": "cx-1", "estado": "RO", "cidade": "Porto Velho", "bairro": "Centro", "endereco": "R. Álfa, N. 10, AP 2"}]
    )

    enriched, candidates = match_properties(properties, cno_path, areas_path)
    row = enriched.iloc[0]

    assert row["cno_match_status"] == "forte"
    assert row["cno_match_score"] == 100
    assert row["cno_match_candidate_count"] == 1
    assert row["cno"] == "000000000001"
    assert row["cno_situacao"] == "ATIVA"
    assert row["cno_area_total"] == 120.5
    assert row["cno_categorias"] == "0 - Obra Nova | 1 - Acréscimo"
    assert len(candidates) == 1
    assert candidates.iloc[0]["candidate_rank"] == 1


def test_empate_no_mesmo_endereco_fica_ambiguo(tmp_path):
    cno_path, areas_path = _cno_files(tmp_path)
    properties = pd.DataFrame(
        [{"link": "cx-2", "estado": "RO", "cidade": "Porto Velho", "bairro": "Centro", "endereco": "Rua Dupla, 30"}]
    )

    enriched, candidates = match_properties(properties, cno_path, areas_path)
    row = enriched.iloc[0]

    assert row["cno_match_status"] == "ambiguo"
    assert row["cno_match_candidate_count"] == 2
    assert pd.isna(row["cno"])
    assert set(candidates["cno"]) == {"000000000003", "000000000004"}


def test_match_sem_tipo_e_sem_bairro_fica_so_provavel(tmp_path):
    cno_path, areas_path = _cno_files(tmp_path)
    properties = pd.DataFrame(
        [{"link": "cx-3", "estado": "RO", "cidade": "Porto Velho", "bairro": "", "endereco": "Gamma, 40"}]
    )

    enriched, candidates = match_properties(properties, cno_path, areas_path)
    row = enriched.iloc[0]

    assert row["cno_match_status"] == "provavel"
    assert row["cno_match_score"] == 94
    assert pd.isna(row["cno"])
    assert len(candidates) == 1


def test_sem_numero_nao_fabrica_match(tmp_path):
    cno_path, areas_path = _cno_files(tmp_path)
    properties = pd.DataFrame(
        [{"link": "cx-4", "estado": "RO", "cidade": "Porto Velho", "bairro": "Centro", "endereco": "Rua Alfa"}]
    )

    enriched, candidates = match_properties(properties, cno_path, areas_path)

    assert enriched.iloc[0]["cno_match_status"] == "sem_match"
    assert enriched.iloc[0]["cno_match_candidate_count"] == 0
    assert candidates.empty
