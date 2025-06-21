# -*- coding: utf-8 -*-
"""
Script para processar arquivos CSV de imóveis da Caixa Econômica Federal.
"""

import pandas as pd
from .utils import converter_valor_monetario_para_float, converter_percentual_para_float

def processar_imoveis_caixa(caminho_arquivo_csv: str) -> pd.DataFrame:
    """
    Lê um arquivo CSV de imóveis da Caixa, processa os dados e retorna um DataFrame.

    Args:
        caminho_arquivo_csv (str): O caminho para o arquivo CSV.

    Returns:
        pd.DataFrame: DataFrame com os dados processados e colunas relevantes.
    """
    # Ignorar a primeira linha (cabeçalho informativo) e usar a segunda como nomes das colunas
    # O encoding 'latin1' é frequentemente necessário para arquivos CSV de órgãos brasileiros.
    try:
        df = pd.read_csv(caminho_arquivo_csv, skiprows=1, header=0, encoding='latin1', sep=';')
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo não encontrado em: {caminho_arquivo_csv}")
    except Exception as e:
        raise Exception(f"Erro ao ler o arquivo CSV: {e}")

    # Nomes das colunas esperadas (com base no exemplo de Rondônia)
    # É importante garantir que o CSV de entrada tenha essas colunas na segunda linha.
    # Se os nomes das colunas puderem variar, uma abordagem mais robusta para identificá-las seria necessária.

    # Colunas relevantes a serem mantidas
    colunas_relevantes = [
        'N° do imóvel',
        'UF',
        'Cidade',
        'Bairro',
        'Endereço do imóvel completo', # Nome da coluna como está no exemplo, ajustarei se necessário
        'Preço total', # Nome da coluna como está no exemplo
        'Valor de avaliação',
        'Percentual de desconto', # Nome da coluna como está no exemplo
        'Descrição',
        'Modalidade de venda',
        'Link de acesso ao imóvel no portal da Caixa' # Nome da coluna como está no exemplo
    ]

    # Renomear colunas para nomes mais curtos e padronizados internamente
    # Isso ajuda a tornar o código mais limpo e menos propenso a erros de digitação
    # com nomes longos de colunas.
    nomes_colunas_map = {
        'Endereço do imóvel completo': 'Endereço',
        'Preço total': 'Preço',
        'Percentual de desconto': 'Desconto',
        'Link de acesso ao imóvel no portal da Caixa': 'Link de acesso'
    }
    df = df.rename(columns=nomes_colunas_map)

    # Atualizar a lista de colunas relevantes com os novos nomes
    colunas_relevantes_renomeadas = [
        'N° do imóvel',
        'UF',
        'Cidade',
        'Bairro',
        'Endereço',
        'Preço',
        'Valor de avaliação',
        'Desconto',
        'Descrição',
        'Modalidade de venda',
        'Link de acesso'
    ]

    # Verificar se todas as colunas relevantes (após renomeação) existem no DataFrame
    colunas_faltantes = [col for col in colunas_relevantes_renomeadas if col not in df.columns]
    if colunas_faltantes:
        raise ValueError(f"Colunas faltantes no CSV: {colunas_faltantes}. Colunas encontradas: {df.columns.tolist()}")

    # Selecionar apenas as colunas relevantes
    df = df[colunas_relevantes_renomeadas]

    # Função auxiliar para converter colunas de valor monetário
    def converter_para_float(valor_str):
        if isinstance(valor_str, (float, int)):
            return float(valor_str)
        if pd.isna(valor_str) or valor_str == '':
            return None
        try:
            # Remove 'R$', pontos de milhar e substitui vírgula decimal por ponto
            valor_str_limpo = str(valor_str).replace('R$', '').replace('.', '').replace(',', '.').strip()
            return float(valor_str_limpo)
        except ValueError:
            # Se a conversão falhar, pode ser um valor não numérico ou formato inesperado
            # print(f"Aviso: Não foi possível converter '{valor_str}' para float.")
            return None # Ou pd.NA para usar o tipo de dados NA do pandas

    # Converter colunas 'Preço' e 'Valor de avaliação' para float
    df['Preço'] = df['Preço'].apply(converter_para_float)
    df['Valor de avaliação'] = df['Valor de avaliação'].apply(converter_para_float)

    # Converter a coluna 'Desconto' de string para float
    # O desconto já vem como '0,50' por exemplo, representando 50%
    def converter_desconto_para_float(valor_str):
        if isinstance(valor_str, (float, int)):
            return float(valor_str)
        if pd.isna(valor_str) or valor_str == '':
            return None
        try:
            # Substitui vírgula decimal por ponto
            valor_str_limpo = str(valor_str).replace(',', '.').strip()
            # Remove o símbolo de porcentagem se houver e divide por 100
            if '%' in valor_str_limpo:
                valor_str_limpo = valor_str_limpo.replace('%', '')
                return float(valor_str_limpo) / 100.0
            return float(valor_str_limpo) # Assume que já está em formato decimal (ex: 0.25 para 25%)
        except ValueError:
            # print(f"Aviso: Não foi possível converter desconto '{valor_str}' para float.")
            return None

    df['Desconto'] = df['Desconto'].apply(converter_desconto_para_float)

    return df

# --- Funções de limpeza reutilizáveis ---

# Functions _converter_valor_monetario_para_float and _converter_percentual_para_float
# were moved to utils.py

def limpar_colunas_financeiras(df: pd.DataFrame, 
                               coluna_preco: str, 
                               coluna_avaliacao: str, 
                               coluna_desconto: str) -> pd.DataFrame:
    """
    Aplica a limpeza e conversão para float nas colunas financeiras especificadas.

    Args:
        df (pd.DataFrame): DataFrame a ser modificado.
        coluna_preco (str): Nome da coluna que contém os preços.
        coluna_avaliacao (str): Nome da coluna que contém os valores de avaliação.
        coluna_desconto (str): Nome da coluna que contém os valores de desconto.

    Returns:
        pd.DataFrame: DataFrame com as colunas financeiras limpas e convertidas.
    """
    if coluna_preco in df.columns:
        df[coluna_preco] = df[coluna_preco].apply(converter_valor_monetario_para_float)
    else:
        # print(f"Aviso: Coluna de preço '{coluna_preco}' não encontrada no DataFrame.")
        pass # Não levantar erro, psa.py pode ter DFs de diferentes fontes/estágios

    if coluna_avaliacao in df.columns:
        df[coluna_avaliacao] = df[coluna_avaliacao].apply(converter_valor_monetario_para_float)
    else:
        # print(f"Aviso: Coluna de avaliação '{coluna_avaliacao}' não encontrada no DataFrame.")
        pass

    if coluna_desconto in df.columns:
        df[coluna_desconto] = df[coluna_desconto].apply(converter_percentual_para_float)
    else:
        # print(f"Aviso: Coluna de desconto '{coluna_desconto}' não encontrada no DataFrame.")
        pass
        
    return df

if __name__ == '__main__':
    # Exemplo de uso (requer um arquivo CSV de exemplo)
    # Crie um arquivo 'exemplo_imoveis.csv' no mesmo diretório ou forneça o caminho correto.
    # O arquivo CSV de exemplo deve ter a primeira linha como lixo,
    # a segunda linha como cabeçalho, e usar ';' como separador.
    # Exemplo de conteúdo para 'exemplo_imoveis.csv':
    """
LINHA DE CABECALHO INFORMATIVO INUTIL QUE SERA IGNORADA;MUITO IMPORTANTE;NAO USAR
N° do imóvel;UF;Cidade;Bairro;Endereço do imóvel completo;Preço total;Valor de avaliação;Percentual de desconto;Descrição;Modalidade de venda;Link de acesso ao imóvel no portal da Caixa;Outra Coluna
12345;RO;Porto Velho;Centro;Rua das Palmeiras, 123, APTO 101, CENTRO, PORTO VELHO - RO - CEP: 76801-058;"100.000,00";"120.000,00";"0,10";"Apartamento com 2 quartos, sala, cozinha, banheiro.";Venda Online;"http://www.caixa.gov.br/imovel/12345";Dado Inutil
67890;RO;Ariquemes;Setor 01;AV. CAPITÃO SILVIO, 1000, SETOR 01, ARIQUEMES - RO - CEP: 76870-000;"R$ 250.550,75";"R$ 300.000,00";"0,15";"Casa térrea, excelente localização.";Leilão - Edital Único;"http://www.caixa.gov.br/imovel/67890";Mais Lixo
11223;AC;Rio Branco;Bosque;ESTRADA DO CALAFATE, 200, BOSQUE, RIO BRANCO - AC - CEP: 69900-000;"75.000,00";"70.000,00";"0,00";"Terreno amplo.";Venda Direta Online;"http://www.caixa.gov.br/imovel/11223";Descartavel
"""
    # Criar um arquivo de exemplo para teste
    conteudo_exemplo_csv = """LINHA DE CABECALHO INFORMATIVO INUTIL QUE SERA IGNORADA;MUITO IMPORTANTE;NAO USAR
N° do imóvel;UF;Cidade;Bairro;Endereço do imóvel completo;Preço total;Valor de avaliação;Percentual de desconto;Descrição;Modalidade de venda;Link de acesso ao imóvel no portal da Caixa;Outra Coluna
12345;RO;Porto Velho;Centro;Rua das Palmeiras, 123, APTO 101, CENTRO, PORTO VELHO - RO - CEP: 76801-058;"100.000,00";"120.000,00";"0,10";"Apartamento com 2 quartos, sala, cozinha, banheiro.";Venda Online;"http://www.caixa.gov.br/imovel/12345";Dado Inutil
67890;RO;Ariquemes;Setor 01;AV. CAPITÃO SILVIO, 1000, SETOR 01, ARIQUEMES - RO - CEP: 76870-000;"R$ 250.550,75";"R$ 300.000,00";"0,15";"Casa térrea, excelente localização.";Leilão - Edital Único;"http://www.caixa.gov.br/imovel/67890";Mais Lixo
11223;AC;Rio Branco;Bosque;ESTRADA DO CALAFATE, 200, BOSQUE, RIO BRANCO - AC - CEP: 69900-000;"75.000,00";"70.000,00";"0,00";"Terreno amplo.";Venda Direta Online;"http://www.caixa.gov.br/imovel/11223";Descartavel
"""
    # Setup basic logging for direct script execution test
    import logging
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s | %(levelname)s | %(module)s:%(lineno)d | %(message)s")
    logger = logging.getLogger(__name__)

    # Create an example CSV file relative to the script's location if run directly
    # This is mainly for ad-hoc testing of this script.
    # In the main pipeline, 'exemplo_imoveis.csv' would be in the project root.
    # For robustness if __file__ is not available (e.g. in some frozen environments):
    try:
        script_dir = Path(__file__).parent
    except NameError:
        script_dir = Path(".") # Fallback to current working directory

    example_csv_path = script_dir / "exemplo_imoveis_proc_test.csv" # Different name to avoid conflict

    with open(example_csv_path, "w", encoding='latin1') as f:
        f.write(conteudo_exemplo_csv)
    logger.info(f"Created example CSV for testing: {example_csv_path}")

    try:
        logger.info(f"Processing example file: {example_csv_path}...")
        df_processado = processar_imoveis_caixa(str(example_csv_path))
        logger.info("\nDataFrame Processed:\n" + df_processado.head().to_string())
        logger.info("\nColumn Data Types:\n" + df_processado.dtypes.to_string())
        # df_processado.info() prints to stdout directly, capture it if needed for logging
        # For now, main info is logged.

    except FileNotFoundError as e:
        logger.error(f"Error: {e}", exc_info=True)
    except ValueError as e:
        logger.error(f"Value error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if example_csv_path.exists():
            example_csv_path.unlink() # Clean up test file
            logger.info(f"Cleaned up example CSV: {example_csv_path}")

"""
Checklist de requisitos:
1. Ler CSV (formato Rondônia): Sim, usa pd.read_csv com skiprows=1, header=0, sep=';', encoding='latin1'.
2. Ignorar primeira linha: Sim, skiprows=1.
3. Usar segunda linha como nomes das colunas: Sim, header=0 após skiprows=1.
4. Converter 'Preço' e 'Valor de avaliação' para float: Sim, função converter_para_float lida com 'R$', '.' e ','.
5. Converter 'Desconto' para float: Sim, função converter_desconto_para_float lida com ',' e '%'.
6. Manter colunas relevantes: Sim, lista colunas_relevantes_renomeadas e seleciona df[colunas_relevantes_renomeadas].
   - Nomes das colunas originais foram mapeados para nomes mais padronizados.
7. Script ser função/classe retornando DataFrame: Sim, função processar_imoveis_caixa retorna DataFrame.
8. Incluir comentários: Sim, comentários explicativos foram adicionados.
"""
