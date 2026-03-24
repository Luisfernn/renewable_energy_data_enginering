import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logger.propagate = False

load_dotenv() 


# Centraliza a lógica do diretório base
if os.path.exists('/app'):
    BASE_DIR = Path('/app')
else:
    BASE_DIR = Path(__file__).resolve().parent


# Atalhos globais para as pastas
DATA_RAW_DIR = BASE_DIR / 'data' / 'raw'
DATA_PROCESSED_DIR = BASE_DIR / 'data' / 'processed'
DATA_LOGS_DIR = BASE_DIR / 'data' / 'logs'

# Garante que TODAS as pastas existam
for folder in [DATA_RAW_DIR, DATA_PROCESSED_DIR, DATA_LOGS_DIR]:
    folder.mkdir(parents=True, exist_ok=True)


def get_engine():
    """
    Cria a engine de conexão com o banco de dados via SQLAlchemy.

    A função prioriza a URL de conexão do Docker (DATABASE_URL_DOCKER) se disponível,
    caindo para a URL local (DATABASE_URL) caso contrário. Isso permite que o 
    pipeline seja executado de forma transparente em diferentes ambientes.

    Returns:
        sqlalchemy.engine.Engine: Objeto de conexão configurado para o PostgreSQL.
    """
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
            logger.info("✅ Conexão com o Data Warehouse estabelecida com sucesso!")
            return True
    except OperationalError as e:
        logger.error("❌ ERRO: Não foi possível conectar ao banco de dados.")
        logger.warning(f"👉 Detalhe técnico: {e}")
        return False


if __name__ == "__main__":
    check_connection()