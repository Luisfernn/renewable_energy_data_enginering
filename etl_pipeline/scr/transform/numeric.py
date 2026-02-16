import pandas as pd
from pathlib import Path

import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logger.propagate = False

BASE_DIR = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'
INPUT_FILE = PROCESSED_DIR / 'renewable_energy_data_text.csv'
OUTPUT_PATH = PROCESSED_DIR / 'renewable_energy_data_clean.csv'

pd.set_option('display.max_columns', None)



metric_columns = [
    'eletricity_generation_gwh',
    'eletricity_installed_capacity_mw',
    'heat_generation_tj',
    'total_public_flows_usd_m',
    'international_public_flows_usd_m',
    'capacity_per_capta_w'
]




def clean_numeric_data(df):

    
    before = len(df)

    df = df.dropna(subset=['year'])

    mask = df[metric_columns].replace(0, pd.NA).isna().all(axis=1)
    df = df[~mask]

    after = len(df)
    removed = before - after

    logger.info(f"Removidos {removed} registros\n")
    logger.debug(f"{df.tail(5)}")

    return df




def fill_nan_numeric_data(df):

    nans_before = df[metric_columns].isna().sum().sum()


    df[metric_columns] = df[metric_columns].fillna(0)


    nans_after = df[metric_columns].isna().sum().sum()


    fiiled = nans_before - nans_after

    logger.info(f"{fiiled} células preenchidas com 0\n")
    logger.debug(f"\nNaNs antes: {nans_before}, depois: {nans_after}")
    logger.debug(f"{df.tail(5)}")

    return df    




def round_metrics(df):

    for col in metric_columns:
        if col in df.columns:
            df[col] = df[col].round(2)

    logger.info(f"✅ Dados numéricos arredondados!")
    logger.debug(f"\nApós arrendondamento: \n{df[metric_columns].tail(5)}")     

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