import pandas as pd
from pathlib import Path
from config import DATA_RAW_DIR, DATA_PROCESSED_DIR

import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

FILE_PATH = DATA_RAW_DIR / 'renewable_energy_data.csv'
OUTPUT_PATH = DATA_PROCESSED_DIR / 'renewable_energy_data_text.csv'

# obriga o pandas a mostrar todas as colunas
pd.set_option('display.max_columns', None) 


def normalize_text_columns(df):
    """
    Normaliza os nomes das colunas do DataFrame.

    A função renomeia os nomes das colunas para um formato mais consistente e padronizado, pensando em:
    - melhorar a legibilidade dos nomes das colunas, tornando-os mais intuitivos e fáceis de entender.
    - facilitar a manipulação dos dados na etapas seguintes do pipeline.
    - facilitar as análises futuras
    - não ter conflitos no banco de dados, evitando caracteres especiais e espaços que podem causar problemas.
    

    Args:
        df (pd.DataFrame): O DataFrame original direto da extração.

    Returns:
        pd.DataFrame: O DataFrame com os nomes das colunas normalizados.
    """

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
        'Electricity Generation (GWh)': 'electricity_generation_gwh',
        'Electricity Installed Capacity (MW)': 'electricity_installed_capacity_mw',
        'Heat Generation (TJ)': 'heat_generation_tj',
        'Public Flows (2022 USD M)': 'total_public_flows_usd_m',
        'SDG 7a1 Intl. Public Flows (2022 USD M)': 'international_public_flows_usd_m',
        'SDG 7b1 RE capacity per capita (W/inhabitant)': 'capacity_per_capita_w'
    }

    df = df.rename(columns=normalized_columns)


    logger.debug("\nnormalize_columns_names\n")
    logger.debug(f"\n{df.head(10)}\n")    
    logger.debug(f"\n{df.tail(10)}")

    logger.info("✅ Normalização dos nomes de colunas concluida!")

    return df



def apply_text_rules(df):
    """Aplica regras de texto como remoção de caracteres especiais e padronização de valores específicos em colunas textuais."""


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



def normalize_text_data(df):
    """
    Normaliza os dados textuais do DataFrame

    A função aplica as seguintes transformações:
    - Padroniza os nomes próprios (region, sub_region, country) para título (ex: "United States") e remove espaços em branco.
    - Padroniza o código ISO3 para maiúsculas e sem espaços vazios.
    - Padroniza as categorias (renewable_or_not, group_technology, technology, sub_technology, producer_type) para minúsculas, sem caracteres especiais e sem espaços vazios.
    - Aplica regras específicas de padronização de texto, como remoção de "total " e mais caracteres especiais.

    Args:
        df (pd.DataFrame): O DataFrame com os dados textuais a serem normalizados.
    Returns:
        pd.DataFrame: O DataFrame com os dados textuais normalizados.    
    """

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

    logger.info("✅ Normalização de dados concluída!")

               
    return df           



def clean_text_data(df):
    """
    Remove registros com valores nulos ou inválidos em colunas críticas.

    As colunas críticas são aquelas que trazem informações essenciais para a análise, como 'country', 'year' e 'technology'.
    Para uma linha ser considerada válida, é necessário que as 3 colunas estejam preenchidas.
    Limpezas realizadas nesta função:
    - Remove registros sem valores nas colunas críticas, possuí contagem de quantos registros foram removidos ao total.
    - Remove registros com valores inválidos antes já mapeados da coluna 'country', possuí contagem de quantos registros foram removidos ao total.

    Args:
        df (pd.DataFrame): O DataFrame com os dados textuais a serem limpos.
    Returns:
        pd.DataFrame: O DataFrame com os registros inválidos removidos, mantendo apenas aqueles com identificação completa e válida.    
    """
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
    invalid_country = [
        'Residual/Unallocated Oda: Sub-Saharan Africa',
        'Residual/Unallocated Oda: Latin America And The Caribbean',
        'Residual/Unallocated Oda: Central Asia And Southern Asia',
        'Residual/Unallocated Oda: Western Asia\xa0And Northern Africa',
        'Residual/Unallocated Oda: Eastern And South-Eastern Asia',
        'Residual/Unallocated Oda: Northern America And Europe',
        'Residual/Unallocated Oda: Oceania Excl. Aus. And N. Zealand',
        'European Union (27)',
        'Multilateral'
    ]
    mask = df['country'].isin(invalid_country)
    count = mask.sum()
    if count > 0:
        logger.warning(f"⚠️  {count} registros com country inválido - removendo...")
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

    logger.info("✅ Limpeza de dados concluída!")

    return df 



if __name__ == "__main__":

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    #logger.setLevel(logging.INFO) 

    logger.info("="*60)
    logger.info("🚀 INICIANDO TRANSFORMAÇÕES TEXTUAIS")
    logger.info("="*60 + "\n")

    df = pd.read_csv(FILE_PATH)
    logger.info(f"📊 Carregados {len(df)} registros\n")

    df = normalize_text_columns(df)
    df = normalize_text_data(df)
    df = clean_text_data(df)

    df.to_csv(OUTPUT_PATH, index=False)
    
    logger.info("="*60)
    logger.info("✅ TRANSFORMAÇÕES TEXTUAIS CONCLUÍDAS")
    logger.info(f"📁 Salvo em: {OUTPUT_PATH}")
    logger.info("="*60)