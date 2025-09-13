from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True, parents=True)

print(BASE_DIR)
print(DATA_DIR)
print(RAW_DIR)
print(PROCESSED_DIR)
print(LOG_DIR)