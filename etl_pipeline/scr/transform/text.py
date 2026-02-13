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
    logger.debug(f"\n{df.head(10)}\n")    
    logger.debug(f"\n{df.tail(10)}")

    logger.info("\n✅ Normalização dos nomes de colunas concluida!")

    return df


def apply_rules(df):

    tech_map = {
        'other primary solid biofuels n.e.s.': 'other primary',
        'other biogases from anaerobic fermentation': 'biogas anaerobic',
        'total nonrenewable': 'nonrenewable',
        'total renewable': 'renewable'
    }


    for col in df['sub_technology'], df['renewable_or_not']:
        if col in tech_map:
            df[col] = (df[col]
                       .replace(tech_map)
                       .str.replace('n.e.s.', '', regex=False)
                       .str.replace('energy', '', regex=False)
                       .str.replace('renewable ', '', regex=False)
                       .str.strip())

            df.loc[df[col] == 'crops', col] = 'energy crops'
            
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
    logger.debug(f"\n{df.head(5)}\n")
    logger.debug(f"\n{df.tail(5)}")

    logger.info("\n✅ Normalização de dados de colunas textuais concluída!")

               
    return df           



def clean_data(df= None):

    if df is None:
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            logger.error(f"❌ Arquivo não encontrado: {e}")


 #remome linhas em registros nas colunas críticas
    critic_columuns = ['country', 'year', 'technology']

    before = len(df)

    for col in critic_columuns:
        null_before = df[col].isna().sum()
        if null_before > 0:
            print(f"⚠️ {null_before} registros sem '{col}' - removendo... ")
            df = df.dropna(subset=[col])
        else:
            print("✅ Nenhum registro com valor nulo nas colunas críticas!")   


 #remove linhas com valor inválido na coluna country
    invalid_region = ['Unspecified Countries']
    mask = df['country'].isin(invalid_region)
    count = mask.sum()
    if count > 0:
        print(f"⚠️ {count} registros com região inválida - removendo...")
        df = df[~mask]


    after = len(df)
    print(f"Total removido: {before - after} registros")
    print(f"Mantidos: {after} registros com identificação completa") 


    logger.debug("\nclean_critic_colmuns\n")
    logger.debug(f"\n{df.head(5)}\n")
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