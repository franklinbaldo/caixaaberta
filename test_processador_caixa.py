import pytest
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import io
import os
import tempfile
from processador_caixa import processar_imoveis_caixa # Removed unused imports

# Sample CSV header and a data row for valid input
# The first line is skipped by the processor.
SAMPLE_CSV_HEADER = "Esta linha deve ser ignorada pelo leitor CSV\n"
# Columns based on processador_caixa.py's expectations before renaming
# Includes all columns that will be processed or are needed for selection.
# Added 'N° do imóvel', 'UF', 'Cidade', 'Bairro'. Using ';' separator.
SAMPLE_COLUMNS_LINE = "N° do imóvel;UF;Cidade;Bairro;Endereço do imóvel completo;Preço total;Valor de avaliação;Percentual de desconto;Descrição;Modalidade de venda;Link de acesso ao imóvel no portal da CAIXA;Imagem\n"
SAMPLE_DATA_ROW1 = "123;UF;CIDADE EXEMPLO;BAIRRO EXEMPLO;AV TESTE, 123, BAIRRO EXEMPLO, CIDADE EXEMPLO - UF;R$ 100.000,00;R$ 150.000,00;20%;CASA COM 2 QUARTOS;VENDA DIRETA ONLINE;http://example.com/1;http://example.com/img1.jpg\n"
SAMPLE_DATA_ROW2 = "456;XX;OUTRA CIDADE;CENTRO;RUA ABC, 456, CENTRO, OUTRA CIDADE - XX;R$ 50.000,50;R$ 75.000,00;0.15;APARTAMENTO NOVO;VENDA ONLINE;http://example.com/2;http://example.com/img2.jpg\n"
SAMPLE_DATA_ROW3 = "789;YY;ALGUMA CIDADE;ZONA RURAL;ESTRADA XYZ, S/N, ZONA RURAL, ALGUMA CIDADE - YY;;R$ 200.000,00;;TERRENO;LICITAÇÃO ABERTA;http://example.com/3;http://example.com/img3.jpg\n"

VALID_CSV_CONTENT = SAMPLE_CSV_HEADER + SAMPLE_COLUMNS_LINE + SAMPLE_DATA_ROW1 + SAMPLE_DATA_ROW2 + SAMPLE_DATA_ROW3

# Expected columns after renaming and selection, must match colunas_relevantes_renomeadas from processador_caixa.py
EXPECTED_COLUMNS = [
    'N° do imóvel', 'UF', 'Cidade', 'Bairro', 'Endereço', 
    'Preço', 'Valor de avaliação', 'Desconto', 'Descrição', 
    'Modalidade de venda', 'Link de acesso'
]

class TestProcessarImoveisCaixa:
    def test_successful_processing_basic(self):
        # Test with a valid CSV content
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", encoding='latin1') as tmp_csv:
            tmp_csv.write(VALID_CSV_CONTENT)
            tmp_csv_path = tmp_csv.name
        
        try:
            df = processar_imoveis_caixa(tmp_csv_path)
            assert isinstance(df, pd.DataFrame)
            
            # Check column renaming and selection
            assert list(df.columns) == EXPECTED_COLUMNS
            
            # Check data types and values for the first row (SAMPLE_DATA_ROW1)
            # Column names here are the *renamed* ones.
            assert str(df.loc[0, 'N° do imóvel']) == "123" # Convert to str for comparison
            assert df.loc[0, 'UF'] == "UF"
            assert df.loc[0, 'Cidade'] == "CIDADE EXEMPLO"
            assert df.loc[0, 'Bairro'] == "BAIRRO EXEMPLO"
            assert df.loc[0, 'Endereço'] == "AV TESTE, 123, BAIRRO EXEMPLO, CIDADE EXEMPLO - UF"
            assert df.loc[0, 'Preço'] == 100000.0
            assert df.loc[0, 'Valor de avaliação'] == 150000.0
            assert df.loc[0, 'Descrição'] == "CASA COM 2 QUARTOS"
            assert df.loc[0, 'Modalidade de venda'] == "VENDA DIRETA ONLINE"
            assert df.loc[0, 'Link de acesso'] == "http://example.com/1"
            assert df.loc[0, 'Desconto'] == 0.20
            
            # Check data types and values for the second row (SAMPLE_DATA_ROW2)
            assert df.loc[1, 'Preço'] == 50000.50
            assert df.loc[1, 'Valor de avaliação'] == 75000.0
            assert df.loc[1, 'Desconto'] == 0.15
            
            # Check data types and values for the third row (SAMPLE_DATA_ROW3) - empty values
            assert pd.isna(df.loc[2, 'Preço'])
            assert df.loc[2, 'Valor de avaliação'] == 200000.0
            assert pd.isna(df.loc[2, 'Desconto'])

            # Check dtypes (using renamed columns)
            assert df['Preço'].dtype == float
            assert df['Valor de avaliação'].dtype == float
            assert df['Desconto'].dtype == float
            assert df['Endereço'].dtype == object 
            
        finally:
            os.remove(tmp_csv_path)

    def test_various_formats_currency_discount(self):
        # Using ';' separator and including all necessary columns for colunas_relevantes
        cols_for_format_test = "N° do imóvel;UF;Cidade;Bairro;Endereço do imóvel completo;Preço total;Valor de avaliação;Percentual de desconto;Descrição;Modalidade de venda;Link de acesso ao imóvel no portal da CAIXA;Imagem\n"
        csv_content = SAMPLE_CSV_HEADER + cols_for_format_test + \
                      "1;UF;C1;B1;Addr1;R$ 1.234,56;R$ 2.000,00;50%;Desc1;Mod1;Link1;Img1\n" + \
                      "2;UF;C2;B2;Addr2;1234.56;2000.00;0,50;Desc2;Mod2;Link2;Img2\n" + \
                      "3;UF;C3;B3;Addr3;R$1234;2000;0.5;Desc3;Mod3;Link3;Img3\n" + \
                      "4;UF;C4;B4;Addr4;   R$ 1.000.000,00  ; R$ 1.500.000,00  ;  25% ;Desc4;Mod4;Link4;Img4\n" # With spaces

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", encoding='latin1') as tmp_csv:
            tmp_csv.write(csv_content)
            tmp_csv_path = tmp_csv.name
        
        try:
            df = processar_imoveis_caixa(tmp_csv_path)
            # Using renamed columns for assertions
            assert df.loc[0, 'Preço'] == 1234.56
            assert df.loc[0, 'Desconto'] == 0.50
            
            assert df.loc[1, 'Preço'] == 1234.56
            assert df.loc[1, 'Valor de avaliação'] == 2000.00
            assert df.loc[1, 'Desconto'] == 0.50 # "0,50"
            
            assert df.loc[2, 'Preço'] == 1234.00 # "R$1234"
            assert df.loc[2, 'Valor de avaliação'] == 2000.00
            assert df.loc[2, 'Desconto'] == 0.50 # "0.5"

            assert df.loc[3, 'Preço'] == 1000000.00 # Check stripping of spaces
            assert df.loc[3, 'Valor de avaliação'] == 1500000.00
            assert df.loc[3, 'Desconto'] == 0.25
        finally:
            os.remove(tmp_csv_path)

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            processar_imoveis_caixa("non_existent_file.csv")

    def test_missing_relevant_column(self):
        # Original column 'Preço total' (maps to 'Preço') is missing.
        # Need to include other required columns for the CSV to be minimally parsable up to the check.
        csv_content_missing_col = SAMPLE_CSV_HEADER + \
                                  "N° do imóvel;UF;Cidade;Bairro;Endereço do imóvel completo;Valor de avaliação;Percentual de desconto;Descrição;Modalidade de venda;Link de acesso ao imóvel no portal da CAIXA\n" + \
                                  "1;UF;C1;B1;Addr1;R$ 2.000,00;50%;Desc1;Mod1;Link1\n"
        
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", encoding='latin1') as tmp_csv:
            tmp_csv.write(csv_content_missing_col)
            tmp_csv_path = tmp_csv.name

        # Error message from processador_caixa.py: f"Colunas faltantes no CSV: {colunas_faltantes}. Colunas encontradas: {df.columns.tolist()}"
        # We expect 'Preço' to be missing.
        with pytest.raises(ValueError, match=r"Colunas faltantes no CSV: \['Preço'\].*"):
            try:
                processar_imoveis_caixa(tmp_csv_path)
            finally:
                os.remove(tmp_csv_path)
                
    def test_empty_csv_file(self):
        # Case 1: Completely empty file
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", encoding='latin1') as tmp_csv:
            # tmp_csv.write("") # Ensure it's empty
            tmp_csv_path = tmp_csv.name
        
        # pandas raises EmptyDataError, which is caught by the generic Exception in processar_imoveis_caixa
        # The error message from the script is f"Erro ao ler o arquivo CSV: {e}"
        with pytest.raises(Exception, match=r"Erro ao ler o arquivo CSV: No columns to parse from file"):
            try:
                processar_imoveis_caixa(tmp_csv_path)
            finally:
                os.remove(tmp_csv_path)

        # Case 2: File with only the first header line (to be skipped)
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", encoding='latin1') as tmp_csv:
            tmp_csv.write(SAMPLE_CSV_HEADER)
            tmp_csv_path = tmp_csv.name
        
        with pytest.raises(Exception, match=r"Erro ao ler o arquivo CSV: No columns to parse from file"): # Expecting EmptyDataError from pandas, caught by generic Exception
            try:
                processar_imoveis_caixa(tmp_csv_path)
            finally:
                os.remove(tmp_csv_path)


    def test_csv_only_headers_no_data(self):
        csv_content_only_headers = SAMPLE_CSV_HEADER + SAMPLE_COLUMNS_LINE # Using the corrected SAMPLE_COLUMNS_LINE
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", encoding='latin1') as tmp_csv:
            tmp_csv.write(csv_content_only_headers)
            tmp_csv_path = tmp_csv.name
        
        try:
            df = processar_imoveis_caixa(tmp_csv_path)
            assert isinstance(df, pd.DataFrame)
            assert df.empty # No data rows
            assert list(df.columns) == EXPECTED_COLUMNS # Should have correct columns
        finally:
            os.remove(tmp_csv_path)
            
    # Consider adding a test for encoding issues if processador_caixa.py didn't hardcode latin1
    # For now, assuming latin1 is the standard. If it could vary, that's a different test.
    # def test_wrong_encoding(self):
    #     # Create a file with UTF-8 content that would break latin1
    #     utf8_char = "é" # Example: a character not in latin1 or that has a different byte representation
    #     # Ensure all required columns are present for this test too.
    #     cols_for_encoding_test = "N° do imóvel;UF;Cidade;Bairro;Endereço do imóvel completo;Preço total;Valor de avaliação;Percentual de desconto;Descrição;Modalidade de venda;Link de acesso ao imóvel no portal da CAIXA;Imagem\n"
    #     csv_content_utf8 = SAMPLE_CSV_HEADER + \
    #                        cols_for_encoding_test + \
    #                        f"1;UF;C1;B1;Addr1{utf8_char};R$ 100,00;R$100,00;0%;Desc1;Mod1;Link1;Img1\n"
    #     
    #     with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", encoding='utf-8') as tmp_csv:
    #         tmp_csv.write(csv_content_utf8)
    #         tmp_csv_path = tmp_csv.name
    # 
    #     # The script's generic `except Exception` will catch `UnicodeDecodeError` from pandas.
    #     with pytest.raises(Exception, match=r"Erro ao ler o arquivo CSV: 'latin-1' codec can't decode byte"):
    #         try:
    #             processar_imoveis_caixa(tmp_csv_path)
    #         finally:
    #             os.remove(tmp_csv_path)

    def test_na_values_in_numeric_cols(self):
        # Using corrected SAMPLE_COLUMNS_LINE which includes all necessary columns
        csv_content_na = SAMPLE_CSV_HEADER + SAMPLE_COLUMNS_LINE + \
                      "1;UF;C1;B1;Addr1;;R$ 2.000,00;;Desc1;Mod1;Link1;Img1\n" + \
                      "2;UF;C2;B2;Addr2;R$ 1.234,56;;N/A;Desc2;Mod2;Link2;Img2\n" + \
                      "3;UF;C3;B3;Addr3;R$ 1000;R$ 2000;INVÁLIDO;Desc3;Mod3;Link3;Img3\n"

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", encoding='latin1') as tmp_csv:
            tmp_csv.write(csv_content_na)
            tmp_csv_path = tmp_csv.name
        
        try:
            df = processar_imoveis_caixa(tmp_csv_path)
            # Using renamed columns for assertions
            assert pd.isna(df.loc[0, 'Preço'])
            assert df.loc[0, 'Valor de avaliação'] == 2000.0
            assert pd.isna(df.loc[0, 'Desconto'])
            
            assert df.loc[1, 'Preço'] == 1234.56
            assert pd.isna(df.loc[1, 'Valor de avaliação'])
            assert pd.isna(df.loc[1, 'Desconto']) # "N/A"
            
            assert df.loc[2, 'Preço'] == 1000.00
            assert df.loc[2, 'Valor de avaliação'] == 2000.00
            assert pd.isna(df.loc[2, 'Desconto']) # "INVÁLIDO"
        finally:
            os.remove(tmp_csv_path)

# Placeholder for colunas_relevantes_renomeadas if not imported (it is)
# colunas_relevantes_renomeadas = [...] # This is defined by processador_caixa.py
