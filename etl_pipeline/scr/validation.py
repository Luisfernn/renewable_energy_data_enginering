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

    logger.info("\nIniciando validação de colunas...")

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
        'electricity_generation_gwh',
        'electricity_installed_capacity_mw',
        'heat_generation_tj',
        'total_public_flows_usd_m',
        'international_public_flows_usd_m',
        'capacity_per_capita_w'
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



def validate_registers_count(df):
    logger.info("Validando contagem de registros...")

    total = len(df)
    min_expected = 60000

    if total == 0:
        logger.error("❌ Dataset vazio! Todas as linhas foram removidas!")
    elif total < min_expected:
        logger.warning(f"⚠️ Poucos registros: {total:,} (esperado: >{min_expected:,})")
    else:
        logger.info(f"✅ Total de registros: {total:,}")


    return df



def nulls_year_column(df):

    logger.info("\nVerificando valores nulls na coluna year...")

    null = df['year'].isna().sum()

    if null > 0:
        logger.error(f"\n❌ {null} linhas com a coluna year vazia")
    else:
        logger.info("✅ Sem null na coluna year!")


    return df



def validate_regions(df):

    logger.info("\nVerificando se há registros com região inválida...")

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
        logger.warning(f"⚠️  Regiões inválidas encontradas: {found}")
    else:
        logger.info("✅ Todas as linhas com region válido!")


    return df



def validate_country_count(df):

    logger.info("\nValidando contagem de países...")

    total_country = df['country'].nunique()

    min_c_expected = 200

    if total_country < min_c_expected:
        logger.warning(f"⚠️ Poucos países: {total_country} (esperado: >{min_c_expected})")
    else:
        logger.info(f"✅ Total de países únicos: {total_country}\n")


    return df



def generation_without_instaled_capacity(df):

    logger.info("Verificando se há geração de energia sem capacidade instalada...")

    gen_w_cap = df[(df['electricity_generation_gwh'] > 0) & (df['electricity_installed_capacity_mw'] <= 0)]
    
    per_w_cap = df[(df['capacity_per_capita_w'] > 0) & (df['electricity_installed_capacity_mw'] <= 0)]


    if len(gen_w_cap) > 0:
        logger.warning(f"\n⚠️  {len(gen_w_cap)} Registros não tem capacidade de energia instalada onde tem geração energia!")
    else:
        logger.info("\n✅ Tem capacidade de energia instalada onde tem geração de energia!")


    if len(per_w_cap) > 0:
        logger.warning(f"⚠️  {len(per_w_cap)} Registros não tem capacidade de energia instalada onde tem capacidade de energia per capta!")
    else:
        logger.info("\n✅ Tem capacidade de energia instalada onde tem capacidade de energia per capta!")


    return df



def validate_composed_key(df):

    logger.info("\nVerificando se há duplicatas...")

    key = ['country', 'year', 'technology', 'sub_technology', 'producer_type']

    duplicates = df.duplicated(subset=key).sum()

    if duplicates > 0:
        logger.error(f"❌ {duplicates} linhas duplicadas encontradas!")
        logger.debug(f"\nExemplos:\n{df[df.duplicated(subset=key, keep=False)].head()}\n")
    else:
        logger.info("✅ Chave composta única. Sem duplicatas!\n")


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
    logger.info(f"\n📊 Carregados {len(df)} registros")


    df = validate_columns(df)
    validate_registers_count(df)
    nulls_year_column(df)
    validate_regions(df)
    validate_country_count(df)
    generation_without_instaled_capacity(df)
    validate_composed_key(df)

    df.to_csv(OUTPUT_PATH, index=False)

    logger.info("="*60)
    logger.info("✅ TRANSFORMAÇÕES TEXTUAIS CONCLUÍDAS")
    logger.info(f"📁 Salvo em: data/processed/renewable_energy_data_validated.csv")
    logger.info("="*60)