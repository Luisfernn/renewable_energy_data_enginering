import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logger.propagate = False

load_dotenv() 

def get_engine():
    url = os.getenv("DATABASE_URL")

    url = os.getenv("DATABASE_URL_DOCKER", url)
    
    # Se o Docker injetar a variável DATABASE_URL_DOCKER, usamos ela, caso contrário, usamos a do .env (localhost)

    return create_engine(url)

def check_connection():
    """Testa se o banco está online e acessível."""
    engine = get_engine()
    try:
        engine = get_engine()
        with engine.connect() as conn:
            print("✅ Conexão com o Data Warehouse estabelecida com sucesso!")
            return True
    except OperationalError as e:
        logger.error("❌ ERRO: Não foi possível conectar ao banco de dados.")
        logger.warning(f"👉 Detalhe técnico: {e}")
        return False


# Centraliza a lógica do diretório base
BASE_DIR = Path('/app') if os.path.exists('/app') else Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data'
RAW_DIR = DATA_DIR / 'raw'
PROCESSED_DIR = DATA_DIR / 'processed'        


if __name__ == "__main__":
    check_connection()