import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logger.propagate = False

load_dotenv() 
DATABASE_URL = os.getenv("DB_URL")

def get_engine():
    """Cria e retorna o engine do SQLAlchemy."""
    return create_engine(DATABASE_URL)

def check_connection():
    """Testa se o banco está online e acessível."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            print("✅ Conexão com o Data Warehouse estabelecida com sucesso!")
            return True
    except OperationalError:
        logger.error("❌ ERRO: Não foi possível conectar ao banco de dados.")
        logger.warning("👉 Verifique se o Docker está rodando e se a porta 5433 está aberta.")
        return False


if __name__ == "__main__":
    check_connection()