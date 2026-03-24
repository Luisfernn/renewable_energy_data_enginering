import logging
from dotenv import load_dotenv

load_dotenv()

from config import DATA_PROCESSED_DIR, DATA_LOGS_DIR, check_connection

logging.basicConfig(
    level = logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(DATA_LOGS_DIR / "etl.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

from load import load_data
from extract import extract_data

from transform.text import(
    normalize_text_columns,
    normalize_text_data,
    clean_text_data
)
from transform.numeric import(
    clean_numeric_data,
    fill_nan_numeric_data,
    round_metrics
)
from validation import(
    validate_columns,
    validate_registers_count,
    nulls_year_column,
    validate_regions,
    validate_country_count,
    generation_without_instaled_capacity,
    validate_composed_key
)


def run_pipeline():
    """
    Executa o fluxo completo de ETL.

    Coordena a extração de dados brutos, a aplicação de múltiplas camadas de 
    transformação (limpeza e padronização) e a carga final em um modelo Star Schema.
    
    O pipeline garante a integridade dos dados através de validações de chaves 
    compostas e garante a atomicidade da carga no banco de dados.

    Raises:
        Exception: Captura falhas em qualquer etapa do processo, 
                   interrompendo a execução e registrando o erro no log.
    """

    logger.info("="*60)
    logger.info("PIPELINE ETL - RENEWABLE ENERGY DATA")
    logger.info("="*60)

    try:

        logger.info("🔌 Verificando conexão com o Data Warehouse...")
        if not check_connection():
            raise ConnectionError("Não foi possível conectar ao banco de dados.")

        logger.info("📥 ETAPA 1/6: EXTRAÇÃO")
        df = extract_data()
        if df is None or df.empty:
            raise ValueError("A extração retornou um DataFrame vazio ou None.") 

        logger.info("📝 ETAPA 2/6: TRANSFORMAÇÕES TEXTUAIS")
        df = normalize_text_columns(df)
        df = normalize_text_data(df)
        df = clean_text_data(df)

        logger.info("🔢 ETAPA 3/6: TRANSFORMAÇÕES NUMÉRICAS")
        df = clean_numeric_data(df)
        df = fill_nan_numeric_data(df)
        df = round_metrics(df)

        logger.info("🔍 ETAPA 4/6: VALIDAÇÃO")
        df = validate_columns(df)
        df = validate_registers_count(df)
        df = nulls_year_column(df)
        df = validate_regions(df)
        df = validate_country_count(df)
        df = generation_without_instaled_capacity(df)
        df = validate_composed_key(df)
    
        logger.info("💾 ETAPA 5/6 SALVAMENTO DOS DADOS")
        df.to_csv(DATA_PROCESSED_DIR / 'renewable_energy_data_final.csv', index=False)
        logger.info(f"✅ Arquivo final salvo em {DATA_PROCESSED_DIR}")

        logger.info("💾 ETAPA 6/6 CARREGAMENTO DOS DADOS NO DATA WAREHOUSE")
        load_data(df)

    except Exception as e:
        logger.error(f"❌ Pipeline falhou: {e}")
        logger.error("="*60)
        logger.info("Encerrando pipeline devido ao erro.")
        raise    


if __name__ == "__main__":
    run_pipeline()    