import os
from pathlib import Path
import pandas as pd

import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logger.propagate = False

def extract_data(file_path: Path = None):

    if os.path.exists('/app'):
        base_dir = Path('/app')
    else:
        # path da pasta do projeto 
        base_dir = Path(__file__).resolve().parent.parent
 

    # verifica se o arquivo raw existe
    if file_path is None:
        file_path = base_dir / 'data' / 'raw' / 'renewable_energy_data_raw.xlsx'

    if not file_path.exists():
        logger.error(f"❌ Arquivo não encontrado: {file_path}")
        return None
    

    # lê o arquivo xlsx
    try:
        df = pd.read_excel(file_path, sheet_name='Country')
    except Exception as e:
        logger.error(f"⚠️ Erro ao ler o arquivo: {e}")
        return None
    

    # transforma o arquivo xlsx em csv
    try:
        csv_path = base_dir / 'data' / 'raw' / 'renewable_energy_data.csv'
        df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"✅ CSV gerado em: {csv_path}")
    except Exception as e:
        logger.error(f"⚠️ Erro ao transformar em csv: {e}")
        # Mesmo que o CSV falhe, podemos retornar o DF para o pipeline seguir
        return df


    # prévia dos dados
    logger.info(f"\n✅ Arquivo carregado com sucesso: {file_path.name}")
    logger.info(f"📊 Linhas: {len(df)}, Colunas {len(df.columns)}\n")

    logger.info("Prévia das 10 primeiras linhas:\n")
    logger.info(f"{df.head(10)}\n")

    logger.info("Prévia das últimas 10 linhas:\n")
    logger.info(f"{df.tail(10)}\n")

    return df


# logger.info só funciona em execução local do script
if __name__ == "__main__":

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    extract_data()