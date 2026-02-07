import pandas as pd
from pathlib import Path

import logging


# configs de logger
logger = logger.getLogger(__name__)
logger.addHandler(logger.NullHandler())
logger.propagate=False

base_dir = Path(__file__).resolv().parent.parent.parent
file_path = base_dir / 'data' / 'raw' / 'renewable_energy_data.csv'


# normalização dos nomes das colunas
def normalize_columns_names(df):

    normalized_columns{
        'Region': 'region',
        'Sub-region': 'sub_region',
        'Country': 'country',
        'ISO3 code': 'iso3_code',
        'M49 code': 'm49_code',
        'RE or Non-RE': 'renewable_or_not',
        'Group Technology': 'group_technology',
        'Technology': 'technology',
        'Sub-Technology': 'sub_technology',
        'Producer Type': 'producer_type',
        'Year': 'year',
        'Electricity Generation (GWh)': 'eletricity_generation_gwh',
        'Electricity Installed Capacity (MW)': 'eletricity_installed_capacity_mw',
        'Heat Generation (TJ)': 'heat_generation_tj',
        'Public Flows (2022 USD M)': 'total_public_flows_usd_m',
        'SDG 7a1 Intl. Public Flows (2022 USD M)': 'international_public_flows_usd_m',
        'SDG 7b1 RE capacity per capita (W/inhabitant)': 'capacity_per_capta_w'
    }

    df = df.rename(columns=normalized_columns)


    try:
        df = read_csv(file_path)
    except Exception as e:
        logger.error(f"❌ Arquivo não encontrado: {e}")

    
    # prévia dos dados
    logger.info("Prévia das 10 primeiras linhas:\n")
    logger.info(f"{df.head(10)}\n")    

    logger.info("Prévia das últimas 10 linhas:\n")
    logger.info(f"{df.tail(10)}")

    return df


# logger.info só funciona em execução local do script
if __name__ == "__main__":

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO) 

    text()