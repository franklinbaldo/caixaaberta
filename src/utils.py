import pandas as pd

def converter_valor_monetario_para_float(valor_str):
    """Converte uma string de valor monetário para float."""
    if isinstance(valor_str, (float, int)):
        return float(valor_str)
    if pd.isna(valor_str) or valor_str == '':
        return None
    try:
        valor_str_limpo = str(valor_str).replace('R$', '').replace('.', '').replace(',', '.').strip()
        return float(valor_str_limpo)
    except ValueError:
        return None

def converter_percentual_para_float(valor_str):
    """Converte uma string de percentual para float (ex: '0,50' ou '50%' para 0.5)."""
    if isinstance(valor_str, (float, int)):
        return float(valor_str)
    if pd.isna(valor_str) or valor_str == '':
        return None
    try:
        valor_str_limpo = str(valor_str).replace(',', '.').strip()
        if '%' in valor_str_limpo:
            valor_str_limpo = valor_str_limpo.replace('%', '')
            return float(valor_str_limpo) / 100.0
        return float(valor_str_limpo)
    except ValueError:
        return None
