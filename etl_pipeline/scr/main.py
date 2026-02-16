import logging
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / 'data' / 'processed'

LOG_DIR = BASE_DIR / 'data' / 'logs'
log_dir_mkdir(parents=True, exist_ok=True)


logging.basicConfig(
    level = logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "etl.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)