# upload_tmp.py
import logging
from typing import Dict, Iterable, List

import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "airline_tickets",
    "user": "postgres",
    "password": "qkrdydqls12!"
}

# 테이블 컬럼 순서 (CSV/DF 모두 이 순서로 맞춰 삽입)
TARGET_COLUMNS: List[str] = [
    "agency_code", "code",
    "depDate", "depDay", "depTime", "depCity", "depDesc",
    "arrDate", "arrDay", "arrTime", "arrCity", "arrDesc",
    "carCode", "carDesc", "opCarCode", "opCarDesc",
    "mainFlt", "classCode", "classDesc", "seat",
    "total_price", "fare", "fareOrigin", "fuelChg", "airTax", "tasf",
    "scraped_date", "source_file"
]

DROP_COLUMNS = ["fareRecKey", "jejucomId", "itinInfo", "itinInfo2"]


def get_engine(db_config: Dict):
    url = (
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    return create_engine(url, pool_pre_ping=True)


def prepare_df_for_upload(df: pd.DataFrame) -> pd.DataFrame:
    """
    - 불필요 컬럼 삭제
    - 날짜/시간/정수 컬럼 타입 정리
    - 컬럼 순서 TARGET_COLUMNS로 정렬
    """
    df = df.copy()

    # 1) 드롭
    existing_drop = [c for c in DROP_COLUMNS if c in df.columns]
    if existing_drop:
        df = df.drop(columns=existing_drop)
        logger.info(f"✅ 불필요 컬럼 제거: {existing_drop}")

    # 2) 타입 정리 (preprocess에서 이미 처리했어도 안전망으로 한 번 더)
    # 날짜
    for dcol in ["depDate", "arrDate", "scraped_date"]:
        if dcol in df.columns:
            df[dcol] = pd.to_datetime(df[dcol], errors="coerce").dt.date

    # 시간
    for tcol in ["depTime", "arrTime"]:
        if tcol in df.columns:
            # 이미 datetime.time 이면 그대로, 문자열이면 변환
            df[tcol] = pd.to_datetime(df[tcol].astype(str), errors="coerce", format="%H:%M:%S").dt.time

    # 정수
    for icol in ["seat", "total_price", "fare", "fareOrigin", "fuelChg", "airTax", "tasf"]:
        if icol in df.columns:
            df[icol] = pd.to_numeric(df[icol], errors="coerce").fillna(0).astype(int)

    # 문자열 트림
    str_cols = [
        "agency_code", "code", "depDay", "depCity", "depDesc",
        "arrDay", "arrCity", "arrDesc",
        "carCode", "carDesc", "opCarCode", "opCarDesc",
        "mainFlt", "classCode", "classDesc", "source_file"
    ]
    for scol in str_cols:
        if scol in df.columns:
            df[scol] = df[scol].astype(str).str.strip()

    # 3) 컬럼 순서 강제
    missing = [c for c in TARGET_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"업로드에 필요한 컬럼이 없습니다: {missing}")

    df = df[TARGET_COLUMNS]
    logger.info(f"✅ 업로드 준비 완료: {len(df)}행, 컬럼 {len(df.columns)}개")
    return df


def to_tuples(df: pd.DataFrame) -> Iterable[tuple]:
    # 행 → 튜플 변환 (NaN → None)
    return (tuple(None if pd.isna(x) else x for x in row) for row in df.to_numpy())


def upload_to_db(df: pd.DataFrame, engine, batch_size: int = 5000):
    """
    ON CONFLICT로 안전 삽입, 배치 처리, 실패 시 롤백
    """
    if df.empty:
        logger.warning("⚠️ 업로드할 데이터가 없습니다.")
        return

    insert_sql = """
    INSERT INTO "flight_info" (
        "agency_code", "code", "depDate", "depDay", "depTime", "depCity", "depDesc",
        "arrDate", "arrDay", "arrTime", "arrCity", "arrDesc",
        "carCode", "carDesc", "opCarCode", "opCarDesc",
        "mainFlt", "classCode", "classDesc", "seat",
        "total_price", "fare", "fareOrigin", "fuelChg", "airTax", "tasf",
        "scraped_date", "source_file"
    ) VALUES %s
    ON CONFLICT ON CONSTRAINT "unique_flight" DO NOTHING;
    """

    total_rows = len(df)
    logger.info(f"📤 업로드 시작: {total_rows}건 (배치 {batch_size})")

    for start in range(0, total_rows, batch_size):
        batch = df.iloc[start:start + batch_size]
        tuples = list(to_tuples(batch))

        with engine.begin() as conn:
            raw_conn = conn.connection
            cursor = raw_conn.cursor()
            try:
                execute_values(cursor, insert_sql, tuples, page_size=batch_size)
                raw_conn.commit()
                logger.info(f"✅ {start + len(batch)}/{total_rows}행 업로드 완료")
            except Exception as e:
                raw_conn.rollback()
                logger.error(f"❌ 배치 업로드 실패 @ {start}: {e}")
                raise

    logger.info("🎉 전체 업로드 완료")


def run_upload(df: pd.DataFrame, db_config: Dict = DB_CONFIG, batch_size: int = 5000):
    """
    preprocess에서 반환된 DataFrame을 직접 받아 업로드
    """
    if df is None or df.empty:
        logger.warning("⚠️ 입력 DataFrame이 비어있습니다. 업로드 중단.")
        return

    # 업로드 전 정리
    df_prepared = prepare_df_for_upload(df)

    # 엔진 생성
    engine = get_engine(db_config)
    logger.info("✅ DB 연결 성공")

    # 업로드
    upload_to_db(df_prepared, engine, batch_size=batch_size)


# 유연성을 위해 CSV로부터 직접 업로드하는 유틸 (옵션)
def run_upload_from_csv(csv_path: str, db_config: Dict = DB_CONFIG, batch_size: int = 5000):
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    logger.info(f"✅ CSV 로드 완료: {len(df)}행 from {csv_path}")
    run_upload(df, db_config=db_config, batch_size=batch_size)


if __name__ == "__main__":
    # 필요시 수동 테스트용 (CSV 경로를 넣어 사용)
    run_upload_from_csv("../data/processed/preprocessing data.csv")
    pass