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
def normalize_text_columns(df = None):
    
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
    logger.debug(f"\n{df.head(10)}\n")    
    logger.debug(f"\n{df.tail(10)}")

    logger.info("\n✅ Normalização dos nomes de colunas concluida!")

    return df


def apply_text_rules(df):

    detail_col = ['technology', 'sub_technology']

    for col in detail_col:
        if col in df.columns:
            df[col] = (df[col]
                       .str.replace('n.e.s.', '', regex=False)
                       .str.replace('energy', '', regex=False)
                       .str.replace('renewable ', '', regex=False)
                       .str.strip())



    if 'group_technology' in df.columns:
        df['group_technology'] = (df['group_technology']
                                  .str.replace(' n.e.s.', '', regex=False)
                                  .str.strip())     



    tech_mapping = {
        'crops': 'energy crops',
        'other biogases from anaerobic fermentation': 'biogas anaerobic',
        'other primary solid biofuels': 'other primary biofuels'
    }

    for col in ['technology', 'sub_technology']:
        if col in df.columns:
            df[col] = df[col].replace(tech_mapping)                              

    return df                       


# normaliza dados de colunas textuais
def normalize_text_data(df = None):

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

    cols_to_fix = ['renewable_or_not', 'sub_technology']
    
    for col in cols_to_fix:
        if col in df.columns:
            df[col] = df[col].str.replace('total ', '', regex=False).str.strip()      


    df = apply_text_rules(df)        


    logger.debug("\nnormalize_textual_columns\n")
    logger.debug(f"\n{df.head(5)}\n")
    logger.debug(f"\n{df.tail(5)}")

    logger.info("\n✅ Normalização de dados concluída!")

               
    return df           



def clean_text_data(df= None):

    if df is None:
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            logger.error(f"❌ Arquivo não encontrado: {e}")


 #remove linhas em registros nas colunas críticas
    critic_columuns = ['country', 'year', 'technology']

    before = len(df)

    removed_any = False
    for col in critic_columuns:
        null_before = df[col].isna().sum()
        if null_before > 0:
            logger.info(f"⚠️ {null_before} registros sem '{col}' - removendo... ")
            df = df.dropna(subset=[col])
            removed_any = True

    if not removed_any:
        logger.info("✅ Nenhum registro com valor nulo nas colunas críticas!")           


 #remove linhas com valor inválido na coluna country
    invalid_region = ['Unspecified Countries']
    mask = df['country'].isin(invalid_region)
    count = mask.sum()
    if count > 0:
        logger.info(f"⚠️  {count} registros com country inválido - removendo...")
        df = df[~mask]
        logger.info("✅ Remoção concluída!")
    else:
        logger.info(f"✅ Nenhum registro com valor inválido na coluna country")


    after = len(df)
    logger.info(f"Total removido após limpeza de dados: {before - after} registros")
    logger.info(f"Mantidos: {after} registros com identificação completa") 


    logger.debug("\nclean_critic_colmuns\n")
    logger.debug(f"\n{df.head(5)}\n")
    logger.debug(f"\n{df.tail(5)}")

    logger.info("\n✅ Limpeza de dados concluída!")

    return df 



# logger.info só funciona em execução local do script
if __name__ == "__main__":

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    # logger.setLevel(logging.INFO) 

    df = normalize_text_columns()
    df = normalize_text_data(df)
    df = clean_text_data(df)