import pandas as pd
import logging
from sqlalchemy import text
from pathlib import Path
from config import DATA_PROCESSED_DIR
from config import get_engine, check_connection

logger = logging.getLogger(__name__)

INPUT_FILE = DATA_PROCESSED_DIR / 'renewable_energy_data_final.csv'

def load_dimensions(df, conn):

    logger.info("Carregando dimensões...")

    # dim_country
    logger.info("  → dim_country...")
    df_country = df[['country', 'iso3_code', 'm49_code', 'region', 'sub_region']].drop_duplicates()
    df_country.to_sql('dim_country', conn, if_exists='append', index=False, chunksize=500)
    logger.info(f"    ✅ {len(df_country)} países inseridos")

    # dim_technology
    logger.info("  → dim_technology...")
    df_tech = df[['technology', 'sub_technology', 'group_technology', 'renewable_or_not']].drop_duplicates()
    df_tech.to_sql('dim_technology', conn, if_exists='append', index=False, chunksize=500)
    logger.info(f"    ✅ {len(df_tech)} tecnologias inseridas")

    # dim_time
    logger.info("  → dim_time...")
    df_time = df[['year']].drop_duplicates()
    df_time['decade'] = (df_time['year'] // 10) * 10
    df_time.to_sql('dim_time', conn, if_exists='append', index=False, chunksize=500)
    logger.info(f"    ✅ {len(df_time)} anos inseridos")

    # dim_producer
    logger.info("  → dim_producer...")
    df_producer = df[['producer_type']].drop_duplicates()
    df_producer.to_sql('dim_producer', conn, if_exists='append', index=False, chunksize=500)
    logger.info(f"    ✅ {len(df_producer)} tipos de produtor inseridos")

    logger.info("✅ Dimensões carregadas!\n")


def get_ids_dimensions(df, conn):

    logger.info("Mapeando IDs das dimensões...")

    # Lê as dimensões do banco
    dim_country = pd.read_sql('SELECT country_id, country FROM dim_country', conn)
    dim_tech = pd.read_sql('SELECT technology_id, technology FROM dim_technology', conn)
    dim_time = pd.read_sql('SELECT time_id, year FROM dim_time', conn)
    dim_producer = pd.read_sql('SELECT producer_id, producer_type FROM dim_producer', conn)
    
    # Faz merge para obter IDs
    df = df.merge(dim_country, on='country', how='left')
    df = df.merge(dim_tech, on='technology', how='left')
    df = df.merge(dim_time, on='year', how='left')
    df = df.merge(dim_producer, on='producer_type', how='left')
    
    logger.info("✅ IDs mapeados!\n")
    
    return df


def load_fact(df, conn):

    logger.info("Carregando tabela fato...")

    # Seleciona apenas colunas necessárias
    df_fact = df[[
        'country_id', 'technology_id', 'time_id', 'producer_id',
        'electricity_generation_gwh', 'electricity_installed_capacity_mw', 'heat_generation_tj',
        'total_public_flows_usd_m', 'international_public_flows_usd_m',
        'capacity_per_capita_w'
    ]]

    # Insere
    df_fact.to_sql('fact_energy_generation', conn, if_exists='append', index=False, chunksize=500)
    
    logger.info(f"✅ {len(df_fact)} registros inseridos na tabela fato!\n")



def create_tables_from_sql(conn):
    """Garante que as tabelas existam antes de qualquer operação"""
    # Se o load.py está em /app/scr/ e o sql em /app/
    sql_path = Path(__file__).parent.parent / 'create_tables.sql'
    
    if sql_path.exists():
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_commands = f.read()
        conn.execute(text(sql_commands))
        logger.info("✅ Estrutura de tabelas verificada.")
    else:
        logger.error(f"❌ Arquivo SQL não encontrado em: {sql_path}")
        raise FileNotFoundError("create_tables.sql é obrigatório.")



def load_data(df):
    """Pipeline completo de carga"""
    logger.info("="*60)
    logger.info("📤 INICIANDO CARGA NO DATA WAREHOUSE")
    logger.info("="*60 + "\n")

    if not check_connection(): return

    engine = get_engine()

    try:
        with engine.begin() as conn:

            create_tables_from_sql(conn)

            # Limpa tabelas antes de recarregar (evita duplicatas)
            logger.info("🗑️  Limpando tabelas...")
            conn.execute(text("TRUNCATE TABLE fact_energy_generation, dim_country, dim_technology, dim_time, dim_producer RESTART IDENTITY CASCADE"))
            logger.info("✅ Tabelas limpas!\n")

            logger.info("🗑️  Limpando dados antigos...")
            conn.execute(text("""
                TRUNCATE TABLE 
                fact_energy_generation, dim_country, dim_technology, dim_time, dim_producer 
                RESTART IDENTITY CASCADE
            """))

            load_dimensions(df, conn)
            df_with_ids = get_ids_dimensions(df, conn)
            load_fact(df_with_ids, conn)
        
        logger.info("="*60)
        logger.info("✅ CARGA CONCLUÍDA COM SUCESSO!")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"❌ Erro na carga. O banco permanece como estava antes.")
        logger.error(f"⚠️ Detalhe: {e}")
        raise

if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    if INPUT_FILE.exists():
        df = pd.read_csv(INPUT_FILE)
        load_data(df)
    else:
        logger.error(f"❌ Arquivo não encontrado: {INPUT_FILE}")    