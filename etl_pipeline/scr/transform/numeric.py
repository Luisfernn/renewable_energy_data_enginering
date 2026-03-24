import pandas as pd
from pathlib import Path
from config import DATA_RAW_DIR, DATA_PROCESSED_DIR

import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

INPUT_FILE = DATA_PROCESSED_DIR / 'renewable_energy_data_text.csv'
OUTPUT_PATH = DATA_PROCESSED_DIR / 'renewable_energy_data_clean.csv'

pd.set_option('display.max_columns', None)



metric_columns = [
    'electricity_generation_gwh',
    'electricity_installed_capacity_mw',
    'heat_generation_tj',
    'total_public_flows_usd_m',
    'international_public_flows_usd_m',
    'capacity_per_capita_w'
]



def clean_numeric_data(df):
    """
    Realiza a limpeza de integridade numérica dos dados.

    Aplica as seguintes regras:
    1. Remove registros onde o ano ('year') esteja ausente, pois o ano é fundamental para a análise temporal.
    2. Remove registros que não tenham pelo menos uma das colunas métricas preenchidas com um valor diferente de zero e nulo,
    garantindo que apenas os registros tenham de fato algum dado energético relevante persistam.

    Args:
        df (pd.DataFrame): O DataFrame original com dados de energia renovável.

    Returns:
        pd.DataFrame: DataFrame filtrado, contendo apenas registros com ano 
                     e pelo menos uma métrica válida.
    """
    
    before = len(df)

    df = df.dropna(subset=['year'])

    mask = df[metric_columns].replace(0, pd.NA).isna().all(axis=1)
    df = df[~mask]

    after = len(df)
    removed = before - after

    logger.info(f"Removidos {removed} registros")
    logger.info("✅ Limpeza de dados concluída!")
    logger.debug(f"{df.tail(5)}")

    return df



def fill_nan_numeric_data(df):
    """
    Preenche valores nulos em colunas numéricas com zero.

    Para uma linha ser válida é necessário somente que uma das colunas métricas esteja
    preenchida, com isso podem existir colunas vazias ou nulas, para evitar problemas em análises futuras,
    essas células serão preenchidas com zero.

    Args:
        df (pd.DataFrame): O DataFrame com colunas numéricas a serem tratadas.

    Returns:
        pd.DataFrame: O DataFrame com colunas métricas nulas preenchidas com 0.
    """

    nans_before = df[metric_columns].isna().sum().sum()


    df[metric_columns] = df[metric_columns].fillna(0)


    nans_after = df[metric_columns].isna().sum().sum()


    fiiled = nans_before - nans_after
    
    logger.debug(f"\n{fiiled} células preenchidas com 0\n")
    logger.debug(f"\nNaNs antes: {nans_before}, depois: {nans_after}\n")
    logger.info("✅ Preenchimento de céluas vazias concluido!")
    logger.debug(f"{df.tail(5)}")

    return df    



def round_metrics(df):
    """Arredonda as colunas métricas para duas casas decimais para padronização."""

    for col in metric_columns:
        if col in df.columns:
            df[col] = df[col].round(2)

    logger.info(f"✅ Dados numéricos arredondados!")
    logger.debug(f"\nApós arrendondamento:\n")
    logger.debug(f"{df.tail(5)}")     

    return df



if __name__ == "__main__":

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    # logger.setLevel(logging.INFO) 

    logger.info("="*60)
    logger.info("🚀 INICIANDO TRANSFORMAÇÕES NUMÉRICAS")
    logger.info("="*60 + "\n")

    df = pd.read_csv(INPUT_FILE)
    logger.info(f"📊 Carregados {len(df)} registros\n")

    df = clean_numeric_data(df)
    df = fill_nan_numeric_data(df)
    df = round_metrics(df)

    df.to_csv(OUTPUT_PATH, index=False)

    logger.debug("\n📊 Dados pós transformações:\n")
    logger.debug(f"{df.tail(5)}")