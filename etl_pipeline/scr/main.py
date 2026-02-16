import logging
from pathlib import Path

base_dir = Path(__file__).resolve().parent
output_dir = base_dir / 'data' / 'processed'

log_dir = base_dir / 'data' / 'logs'
log_dir_mkdir(parents=True, exist_ok=True)


logging.basicConfig(
    level = logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "etl.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)