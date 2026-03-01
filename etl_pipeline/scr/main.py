import logging
from pathlib import Path

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
from dotenv import load_dotenv
from config import check_connection
from load import load_data


load_dotenv()


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / 'data' / 'processed'

LOG_DIR = BASE_DIR / 'data' / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)


logging.basicConfig(
    level = logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "etl.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)


logger = logging.getLogger(__name__)


def main():

    logger.info("="*60)
    logger.info("PIPELINE ETL - RENEWABLE ENERGY DATA")
    logger.info("="*60)

    try:

        logger.info("\n📥 ETAPA 1/6: EXTRAÇÃO")
        df = extract_data()

        if df is None:
            raise ValueError("Extração falhou. Df vazio ou None.")

        logger.info("\n📝 ETAPA 2/6: TRANSFORMAÇÕES TEXTUAIS")
        df = normalize_text_columns(df)
        df = normalize_text_data(df)
        df = clean_text_data(df)

        logger.info("\n🔢 ETAPA 3/6: TRANSFORMAÇÕES NUMÉRICAS")
        df = clean_numeric_data(df)
        df = fill_nan_numeric_data(df)
        df = round_metrics(df)

        logger.info("\n🔍 ETAPA 4/6: VALIDAÇÃO")
        df = validate_columns(df)
        df = validate_registers_count(df)
        df = nulls_year_column(df)
        df = validate_regions(df)
        df = validate_country_count (df)
        df = generation_without_instaled_capacity(df)
        df = validate_composed_key(df)
    
        logger.info("\n💾 ETAPA 5/6 SALVAMENTO DOS DADOS")
        df.to_csv(OUTPUT_DIR / 'renewable_energy_data_final.csv', index=False)
        logger.info("✅ Salvo!")

        logger.info("\n💾 ETAPA 6/6 CARREGAMENTO DOS DADOS NO DATA WAREHOUSE")
        load_data(df)

    except Exception as e:
        logger.error(f"❌ Pipeline falhou: {e}")
        logger.error("="*60)
        logger.info("Encerrando pipeline devido ao erro.")
        raise    


if __name__ == "__main__":
    main()    