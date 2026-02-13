import pandas as pd
from pathlib import Path

import logging


# configs de logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logger.propagate = False

base_dir = Path(__file__).resolve().parent.parent.parent
file_path = base_dir / 'data' / 'raw' / 'renewable_energy_data.csv'

# obriga o pandas a mostrar todas as colunas
pd.set_option('display.max_columns', None) 



# normalização dos nomes das colunas
def normalize_columns_names(df = None):
    
    # se executada localmente (função) lê o arquivo csv para mostrar prév dos dados  
    if df is None:
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            logger.error(f"❌ Arquivo não encontrado: {e}")


    normalized_columns={
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



    logger.debug("\nnormalize_columns_names\n")
    logger.debug("Prévia das 10 primeiras linhas:")
    logger.debug(f"\n{df.head(10)}\n")    
    logger.debug("Prévia das últimas 10 linhas:")
    logger.debug(f"\n{df.tail(10)}")

    logger.info("\n✅ Normalização dos nomes de colunas concluida!")

    return df



# normaliza dados de colunas textuais
def normalize_textual_columns(df = None):

    if df is None:
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            logger.error(f"❌ Arquivo não encontrado: {e}")


    # nomes próprios
    locations_columns = ['region', 'sub_region', 'country'] 
    for col in locations_columns:
        if col in df.columns:
           df[col] = df[col].str.strip().str.title()

    # código
    df['iso3_code'] = df['iso3_code'].str.strip().str.upper()

    # categorias
    category_columns = ['renewable_or_not', 'group_technology', 'technology', 'sub_technology', 'producer_type']
    
    for col in category_columns:
        if col in df.columns:

            df[col] = (
                df[col]
                .str.replace('[*()-]', '', regex=True)
                .str.replace(' excl. ', ' excluding ')
                .str.strip()
                .str.lower()
            ) 

    logger.debug("\nnormalize_textual_columns\n")
    logger.debug("Prévia das 5 primeiras linhas:")
    logger.debug(f"\n{df.head(5)}\n")
    logger.debug("\Prévia das últimas 5 linhas:")
    logger.debug(f"\n{df.tail(5)}")

    logger.info("\n✅ Normalização de dados de colunas textuais concluída!")

               
    return df           



# remove linhas que os valores das colunas críticas não estão preenchidos
def clean_data(df= None):

    if df is None:
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            logger.error(f"❌ Arquivo não encontrado: {e}")


    critic_columuns = ['country', 'year', 'technology']

    before = len(df)

    for col in critic_columuns:
        null_before = df[col].isna().sum()
        if null_before > 0:
            print(f"⚠️ {null_before} registros sem '{col}' - removendo... ")
            df = df.dropna(subset=[col])

    invalid_region = ['Unspecified Countries']

    for col in invalid_region:
        null_before = df[col].isna().sum()
        if null_before > 0:
            print(f"⚠️ {null_before} registros em {col} - removendo...")
            df = df.dropna(subset=[col])        


    after = len(df)
    print(f"Total removido: {before - after} registros")
    print(f"Mantidos: {after} registros com identificação completa") 

    logger.debug("\nclean_critic_colmuns\n")
    logger.debug("Prévia das 5 primeiras linhas:")
    logger.debug(f"\n{df.head(5)}\n")
    logger.debug("\Prévia das últimas 5 linhas:")
    logger.debug(f"\n{df.tail(5)}")

    logger.info("\n✅ Limpeza de registros sem dados em colunas críticas concluída!")

    return df 



# logger.info só funciona em execução local do script
if __name__ == "__main__":

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    # logger.setLevel(logging.INFO) 

    df = normalize_columns_names()
    df = normalize_textual_columns(df)
    df = clean_data(df)