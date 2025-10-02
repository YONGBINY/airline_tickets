 # preprocess.py
import os
import json
import glob
import pandas as pd
from datetime import datetime, date

from src.common.logging_setup import setup_logging
from src.common.paths import RAW_DIR, PROCESSED_DIR

logger = setup_logging(__name__)


# --- 1. 도우미 함수들 정의 ---
def _create_dataframe_from_list(data_list: list) -> pd.DataFrame:
    """메모리에 있는 데이터 리스트를 하나의 데이터프레임으로 만듭니다."""
    df_list = []
    for item in data_list:
        filepath = item["filepath"]
        raw_data = item["raw_data"]

        flights = raw_data.get("data", {}).get("data", [])
        if flights:
            temp = pd.DataFrame(flights)
            # 파일 경로(filepath)에서 필요한 메타데이터 추출
            parts = filepath.split(os.sep)
            temp["source_file"] = filepath
            temp["agency_code"] = os.path.splitext(os.path.basename(filepath))[0].split("_")[-1]
            try:
                temp["scraped_date"] = parts[parts.index("raw") + 1]
            except (ValueError, IndexError):
                temp["scraped_date"] = None
            df_list.append(temp)

    if not df_list:
        return pd.DataFrame()
    return pd.concat(df_list, ignore_index=True)

def _load_data_from_files(file_paths: list) -> pd.DataFrame:
    df_list = []
    for file_path in file_paths:
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                raw = json.load(f)
                flights = raw.get("data", {}).get("data", [])
                if flights:
                    temp = pd.DataFrame(flights)
                    parts = file_path.split(os.sep)
                    temp["source_file"] = file_path
                    temp["agency_code"] = os.path.splitext(os.path.basename(file_path))[0].split("_")[-1]
                    temp["scraped_date"] = parts[parts.index("raw") + 1]
                    df_list.append(temp)
            except json.JSONDecodeError as e:
                logger.warning(f"JSON 파싱 실패: {file_path} ({e})")

    if not df_list:
        logger.warning("로드된 데이터가 없습니다.")
        return pd.DataFrame()

    return pd.concat(df_list, ignore_index=True)

def _clean_and_transform_data(df: pd.DataFrame) -> pd.DataFrame:
    desc_map = {
        "부산/김해": "부산", "부산(김해)": "부산", "서울/김포": "김포", "서울(김포)": "김포",
        "진주/사천": "사천", "진주(사천)": "사천", "진주": "사천", "HIN": "사천",
        "포항경주": "포항", "포항/경주": "포항", "KPO": "포항",
        "여수/순천": "여수", "RSU": "여수", "GMP": "김포", "PUS": "부산", "CJU": "제주",
        "KWJ": "광주", "CJJ": "청주", "TAE": "대구", "USN": "울산", "KUV": "군산",
        "WJU": "원주", "YNY": "양양"
    }
    df["depDesc"] = df["depDesc"].replace(desc_map)
    df["arrDesc"] = df["arrDesc"].replace(desc_map)
    depDesc_empty = df["depDesc"].str.strip() == ""
    arrDesc_empty = df["arrDesc"].str.strip() == ""
    df.loc[depDesc_empty, "depDesc"] = df.loc[depDesc_empty, "depCity"].map(desc_map)
    df.loc[arrDesc_empty, "arrDesc"] = df.loc[arrDesc_empty, "arrCity"].map(desc_map)

    for col in ["depDate", "arrDate"]:
        date_str = df[col].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        date_str = date_str.where(date_str.str.match(r"^\d{8}$"), pd.NA)
        df[col] = pd.to_datetime(date_str, format="%Y%m%d", errors="coerce")

    df["depDay"] = df["depDate"].dt.day_name().str[:3].str.upper()
    df["arrDay"] = df["arrDate"].dt.day_name().str[:3].str.upper()

    for col in ["depTime", "arrTime"]:
        time_str = df[col].astype(str).str.zfill(4)
        df[col] = pd.to_datetime(time_str, format="%H%M", errors="coerce").dt.time

    df["carDesc"] = df["carDesc"].replace({"아시아나항공": "아시아나"})
    airline_map = {
        "OZ": "아시아나", "KE": "대한항공", "BX": "에어부산", "LJ": "진에어",
        "TW": "티웨이항공", "7C": "제주항공", "ZE": "이스타항공", "RS": "에어서울", "WE": "파라타항공"
    }
    df["carDesc_official"] = df["carCode"].map(airline_map)

    for col in ["carDesc", "opCarDesc", "opCarCode"]:
        if col in df.columns:
            df[col] = df[col].astype(object)
            df[col] = df[col].where(df[col].notna(), "")
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"nan": "", "None": "", "NaT": ""})

    mask_we = (df["carCode"] == "WE")

    mask_we_fix_from_op = mask_we & (df["carDesc"] == "") & (df["opCarDesc"] == "파라타항공")
    df.loc[mask_we_fix_from_op, "carDesc"] = "파라타항공"

    mask_we_car_empty = mask_we & (df["carDesc"] == "")
    df.loc[mask_we_car_empty, "carDesc"] = df.loc[mask_we_car_empty, "carDesc_official"]
    df.loc[mask_we, ["opCarCode", "opCarDesc"]] = ""

    mismatch = (df["carDesc"] != df["carDesc_official"]) & (~mask_we)
    df.loc[mismatch, "opCarCode"] = df.loc[mismatch, "carCode"].map({"OZ": "BX", "KE": "LJ"})
    df.loc[mismatch, "opCarDesc"] = df.loc[mismatch, "carDesc"]
    df.loc[mismatch, "carDesc"] = df.loc[mismatch, "carDesc_official"]

    opCarDesc_map = {"BX": "에어부산", "LJ": "진에어"}
    mask_op = (df["opCarCode"].isin(opCarDesc_map.keys())) & (df["opCarDesc"].str.strip() == "")
    df.loc[mask_op, "opCarDesc"] = df["opCarCode"].map(opCarDesc_map)

    for col in ["seat", "fare", "fareOrigin", "airTax", "fuelChg", "tasf"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in ["classDesc", "mainFlt"]:
        df[col] = df[col].astype(str).str.strip()

    df["scraped_date"] = pd.to_datetime(df["scraped_date"], format="%Y-%m-%d", errors="coerce")

    df["total_price"] = df["fare"] + df["fuelChg"] + df["airTax"] + df["tasf"]

    return df

def _format_final_df(df: pd.DataFrame) -> pd.DataFrame:
    """최종 컬럼 순서를 정리하고 불필요한 컬럼을 제거합니다."""
    # `upload.py`와 동기화된 최종 컬럼 목록
    final_columns = [
        "agency_code", "code", "depDate", "depDay", "depTime", "depCity", "depDesc",
        "arrDate", "arrDay", "arrTime", "arrCity", "arrDesc", "carCode", "carDesc",
        "opCarCode", "opCarDesc", "mainFlt", "classCode", "classDesc", "seat",
        "total_price", "fare", "fareOrigin", "fuelChg", "airTax", "tasf",
        "scraped_date", "source_file"
    ]

    # 혹시 모를 누락 컬럼 추가 (결측값으로 채워짐)
    for col in final_columns:
        if col not in df.columns:
            df[col] = pd.NA

    return df[final_columns]


# --- 2. 메인 함수 (감독 역할) ---
def run_preprocess(collected_data: list = None, save_csv=True):
    logger.info("전처리 시작")

    if collected_data:
        # 1. 파이프라인 모드 (메모리에서 데이터 처리)
        logger.info(f"메모리로부터 {len(collected_data)}개 응답 데이터를 전처리합니다.")
        df_raw = _create_dataframe_from_list(collected_data)
    else:
        # 2. 독립 실행 모드 (파일 시스템)
        logger.info("파일 시스템으로부터 데이터를 로드하여 전처리합니다.")
        json_files = glob.glob(os.path.join(RAW_DIR, "**", "*.json"), recursive=True)
        if not json_files:
            logger.warning("처리할 파일이 없습니다.")
            return pd.DataFrame()
        df_raw = _load_data_from_files(json_files)

    if df_raw.empty:
        logger.warning("처리할 항공편 데이터가 없습니다.")
        return df_raw

    df_clean = _clean_and_transform_data(df_raw)
    df_final = _format_final_df(df_clean)

    if save_csv:
        output_path = PROCESSED_DIR / "preprocessing_data.csv"
        logger.info(f"CSV 저장/누적 작업을 시작합니다: {output_path}")

        unique_keys = [
            "agency_code", "code",
            "depDate", "depTime", "depCity",
            "arrDate", "arrTime", "arrCity",
            "carCode", "opCarCode",
            "mainFlt", "classCode", "classDesc"
        ]

        def _normalize_for_dedup(df_in: pd.DataFrame) -> pd.DataFrame:
            dfn = df_in.copy()

            # 1) 날짜 → YYYY-MM-DD
            for dcol in ["depDate", "arrDate", "scraped_date"]:
                if dcol in dfn.columns:
                    dfn[dcol] = pd.to_datetime(dfn[dcol], errors="coerce").dt.strftime("%Y-%m-%d")

            # 2) 시간 → HH:MM:SS (모든 입력 포맷 허용, zero-padding 강제)
            def _to_hms(s):
                s = s.astype(str).str.strip()
                # 먼저 HH:MM:SS 형태 시도
                t1 = pd.to_datetime(s, format="%H:%M:%S", errors="coerce")
                # HH:MM 형태 시도
                mask_na = t1.isna()
                if mask_na.any():
                    t2 = pd.to_datetime(s[mask_na], format="%H:%M", errors="coerce")
                    t1 = t1.where(~mask_na, t2)
                # HHMM 형태 시도
                mask_na = t1.isna()
                if mask_na.any():
                    t3 = pd.to_datetime(s[mask_na].str.zfill(4), format="%H%M", errors="coerce")
                    t1 = t1.where(~mask_na, t3)
                return t1.dt.strftime("%H:%M:%S")

            for tcol in ["depTime", "arrTime"]:
                if tcol in dfn.columns:
                    dfn[tcol] = _to_hms(dfn[tcol])

            # 3) 문자열 표준화: strip, NaN류 → ""
            str_cols = [
                "agency_code", "code", "depDay", "depCity", "depDesc",
                "arrDay", "arrCity", "arrDesc",
                "carCode", "carDesc", "opCarCode", "opCarDesc",
                "mainFlt", "classCode", "classDesc", "source_file"
            ]
            for col in str_cols:
                if col in dfn.columns:
                    dfn[col] = dfn[col].fillna("").astype(str).str.strip()
                    dfn[col] = dfn[col].replace({"nan": "", "None": "", "NaT": ""})

            for col in str_cols:
                if col in dfn.columns:
                    dfn[col] = dfn[col].fillna("").astype(str).str.strip()
                    dfn[col] = dfn[col].replace({"nan": "", "None": "", "NaT": ""})

            # 4) 코드류 대문자 통일
            for col in ["agency_code", "carCode", "opCarCode", "classCode", "mainFlt", "code"]:
                if col in dfn.columns:
                    dfn[col] = dfn[col].str.upper()

            # 5) 중복키 누락 컬럼 보강
            for col in unique_keys:
                if col not in dfn.columns:
                    dfn[col] = ""

            return dfn

        try:
            # 신규 데이터 정규화
            new_norm = _normalize_for_dedup(df_final)

            # 진단: 신규 데이터 내부 중복
            if len(new_norm) != len(new_norm.drop_duplicates(subset=unique_keys)):
                logger.warning("신규 데이터 내부에서 중복 키 발견(정규화 기준). 중복은 CSV 병합 시 제거됩니다.")

            if os.path.exists(output_path):
                logger.info("기존 CSV 파일을 불러와 데이터를 병합합니다.")
                # dtype=str로 읽어들여 포맷섞임 방지, 이후 정규화
                old_df = pd.read_csv(output_path, dtype=str, encoding="utf-8-sig")
                old_norm = _normalize_for_dedup(old_df)

                before = len(old_norm) + len(new_norm)
                combined_df = pd.concat([old_norm, new_norm], ignore_index=True)

                combined_df.drop_duplicates(subset=unique_keys, keep="last", inplace=True)
                after = len(combined_df)
                removed = before - after

                logger.info(f"중복 제거 결과: {removed}건 제거, 최종 {after}건")

                combined_df.to_csv(output_path, index=False, encoding="utf-8-sig")
                logger.info(f"💾 {len(new_norm)}건 신규 병합 완료. 총 {len(combined_df)}건 저장.")
            else:
                logger.info("기존 CSV 파일이 없어 새로 생성합니다.")
                new_norm.to_csv(output_path, index=False, encoding="utf-8-sig")
                logger.info(f"💾 {len(new_norm)}건 저장 완료.")

        except Exception as e:
            logger.error(f"❌ CSV 저장/누적 중 오류 발생: {e}", exc_info=True)


    logger.info("전처리 완료")
    return df_final

if __name__ == "__main__":
    run_preprocess(save_csv=True)