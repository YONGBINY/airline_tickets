# main.py
from src.collect import run_collect
from src.preprocess import run_preprocess
from src.upload import run_upload

if __name__ == "__main__":
    run_collect()
    df = run_preprocess(save_csv=False)  # CSV 저장 안 함
    run_upload(df)
