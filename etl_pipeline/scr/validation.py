import pandas as pd
from pathlib import Path

import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logger.propagate = False

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'
INPUT_DIR = PROCESSED_DIR / 'renewable_energy_data_clean.csv'
OUTPUT_PATH = PROCESSED_DIR / 'renewable_energy_data_validated.csv'


def validate_columns(df):

    logger.info("Iniciando validação de colunas...")

    expected_columns = [
        'region',
        'sub_region',
        'country',
        'iso3_code',
        'm49_code',
        'renewable_or_not',
        'group_technology',
        'technology',
        'sub_technology',
        'producer_type',
        'year',
        'eletricity_generation_gwh',
        'eletricity_installed_capacity_mw',
        'heat_generation_tj',
        'total_public_flows_usd_m',
        'international_public_flows_usd_m',
        'capacity_per_capta_w'
    ]

    currently_columns = df.columns.tolist()

    if expected_columns == currently_columns:
        logger.info(f"✅ Todas as {len(expected_columns)} colunas presentes e na ordem correta\n")
    else:
        missing = set(expected_columns) - set(currently_columns)
        extra = set(currently_columns) - set(expected_columns)
        wrong_order = currently_columns != expected_columns 

        if missing:
            logger.error(f"❌ Colunas faltando: {missing}")
        if extra:
            logger.error(f"❌ Colunas extras: {extra}")
        if wrong_order:
            logger.error(f"❌ Ordem incorreta!\n Esperando: {expected_columns}\n Atual: {currently_columns} ")   

    return df                  



def nulls_year_column(df):

    null = df['year'].isna().sum()

    if null > 0:
        logger.warning(f"\n⚠️ {null} linhas com a coluna year vazia") 
    else:
        logger.info("\n✅ Sem null na coluna year!")

    return df        



def generation_without_instaled_capacity(df):

    gen_w_cap = df[(df['eletricity_generation_gwh'] > 0) & (df['eletricity_installed_capacity_mw'] <= 0)]

    per_w_cap = df[(df['capacity_per_capta_w'] > 0) & (df['eletricity_installed_capacity_mw'] <= 0)]
    

    if len(gen_w_cap) > 0:
        logger.warning("\n⚠️ Não tem capacidade de energia instalada onde tem geração energia!")
    else:
        logger.info("\n✅ Tem capacidade de energia instalada onde tem geração de energia!")
           

    if len(per_w_cap) > 0:
        logger.warning("\n⚠️ Não tem capacidade de energia instalada onde tem capacidade de energia per capta!")
    else:
        logger.info("\n✅ Tem capacidade de energia instalada onde tem capacidade de energia per capta!")


    return df



def validate_regions(df):

    valid_regions = [
        'Africa',
        'Americas',
        'Asia',
        'Europe',
        'Oceania'
    ]

    invalid_regions = df[~df['region'].isin(valid_regions)]

    if len(invalid_regions) > 0:
        found = invalid_regions['region'].unique().tolist()
        logger.error(f"\n❌ Regiões inválidas encontradas: {found}")
    else:
        logger.info("\n✅ Todas as linhas com region válido!")    

    return df           












if __name__ == '__main__':

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    #logger.setLevel(logging.DEBUG)

    logger.info("="*60)
    logger.info("INICIANDO VALIDAÇÃO")
    logger.info("="*60)

    df = pd.read_csv(INPUT_DIR)
    logger.info(f"📊 Carregados {len(df)} registros")


    df = validate_columns(df)


    df.to_csv(OUTPUT_PATH)

 