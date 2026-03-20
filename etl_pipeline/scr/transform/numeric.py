import pandas as pd
from pathlib import Path
from config import DATA_RAW_DIR, DATA_PROCESSED_DIR

import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logger.propagate = False

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

    
    before = len(df)

    df = df.dropna(subset=['year'])

    mask = df[metric_columns].replace(0, pd.NA).isna().all(axis=1)
    df = df[~mask]

    after = len(df)
    removed = before - after

    logger.info(f"Removidos {removed} registros")
    logger.info("\n✅ Limpeza de dados concluída!")
    logger.debug(f"{df.tail(5)}")

    return df



def fill_nan_numeric_data(df):

    nans_before = df[metric_columns].isna().sum().sum()


    df[metric_columns] = df[metric_columns].fillna(0)


    nans_after = df[metric_columns].isna().sum().sum()


    fiiled = nans_before - nans_after
    
    logger.debug(f"\n{fiiled} células preenchidas com 0\n")
    logger.debug(f"\nNaNs antes: {nans_before}, depois: {nans_after}\n")
    logger.info("\n✅ Preenchimento de céluas vazias concluido!")
    logger.debug(f"{df.tail(5)}")

    return df    



def round_metrics(df):

    for col in metric_columns:
        if col in df.columns:
            df[col] = df[col].round(2)

    logger.info(f"\n✅ Dados numéricos arredondados!")
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