# preprocess.py
import os
import json
import glob
import pandas as pd

from src.common.logging_setup import setup_logging
from src.common.paths import RAW_DIR, PROCESSED_DIR
logger = setup_logging(__name__)

JSON_ROOT_DIR = RAW_DIR

def run_preprocess(save_csv=True):
    logger.info("전처리 시작")
    json_files = glob.glob(os.path.join(JSON_ROOT_DIR, "**", "*.json"), recursive=True)
    print(f"📁 처리할 파일 수: {len(json_files)}")

    ##-----------------------------------------def_scraped_date
    def extract_scraped_date(path):
        parts = path.split(os.sep)
        return parts[1]

    ##-----------------------------------------def_agency_code
    def extract_agency_code(path):
        filename = os.path.basename(path)
        name_no_ext = os.path.splitext(filename)[0]
        return name_no_ext.split("_")[-1]

    df_list = []
    for file_path in json_files:
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                raw = json.load(f)
                flights = raw.get("data", {}).get("data", [])
                if flights:
                    temp = pd.DataFrame(flights)
                    temp["source_file"] = file_path
                    temp["agency_code"] = extract_agency_code(file_path)
                    temp["scraped_date"] = extract_scraped_date(file_path)
                    df_list.append(temp)
            except json.JSONDecodeError as e:
                print(f"Fail Parsing: {file_path} ({e})")

    if not df_list:
        print("⚠️ 로드된 데이터가 없습니다.")
        return pd.DataFrame()

    df = pd.concat(df_list, ignore_index=True)

    ##-----------------------------------------depDesc, arrDesc
    desc_map = {
        "부산/김해": "부산", "부산(김해)": "부산",
        "여수/순천": "여수",
        "서울/김포": "김포", "서울(김포)": "김포",
        "진주": "사천", "진주/사천": "사천", "진주(사천)": "사천",
        "포항경주": "포항", "포항/경주": "포항",
        "GMP": "김포",
        "PUS": "부산",
        "CJU": "제주",
        "RSU": "여수",
        "KWJ": "광주",
        "CJJ": "청주",
        "TAE": "대구",
        "USN": "울산",
        "KUV": "군산",
        "WJU": "원주",
        "KPO": "포항",
        "HIN": "사천",
        "YNY": "양양"
    }
    df["depDesc"] = df["depDesc"].replace(desc_map)
    df["arrDesc"] = df["arrDesc"].replace(desc_map)

    depDesc_empty = df["depDesc"].str.strip() == ""
    arrDesc_empty = df["arrDesc"].str.strip() == ""
    df.loc[depDesc_empty, "depDesc"] = df.loc[depDesc_empty, "depCity"].map(desc_map)
    df.loc[arrDesc_empty, "arrDesc"] = df.loc[arrDesc_empty, "arrCity"].map(desc_map)

    ##-----------------------------------------depDate, arrDate
    depDate_str = df["depDate"].astype(str).str.strip()
    arrDate_str = df["arrDate"].astype(str).str.strip()
    depDate_str = depDate_str.str.replace(r"\.0$", "", regex=True)
    arrDate_str = arrDate_str.str.replace(r"\.0$", "", regex=True)
    depDate_str = depDate_str.where(depDate_str.str.match(r"^\d{8}$"), pd.NA)
    arrDate_str = arrDate_str.where(arrDate_str.str.match(r"^\d{8}$"), pd.NA)
    df["depDate"] = pd.to_datetime(depDate_str, format="%Y%m%d", errors="coerce")
    df["arrDate"] = pd.to_datetime(arrDate_str, format="%Y%m%d", errors="coerce")

    ##-----------------------------------------depDay, arrDay
    df["depDay"] = df["depDate"].dt.day_name().str[:3].str.upper()
    df["arrDay"] = df["arrDate"].dt.day_name().str[:3].str.upper()

    ##-----------------------------------------depTime, arrTime
    depTime_str = df["depTime"].astype(str).str.zfill(4)
    arrTime_str = df["arrTime"].astype(str).str.zfill(4)
    df["depTime"] = pd.to_datetime(depTime_str, format="%H%M", errors="coerce").dt.time
    df["arrTime"] = pd.to_datetime(arrTime_str, format="%H%M", errors="coerce").dt.time

    ##-----------------------------------------carCode, carDesc--------- 임시 중단, 매칭 안됨
    carDesc_map = {
        "아시아나항공": "아시아나"
    }
    df["carDesc"] = df["carDesc"].replace(carDesc_map)

    airline_map = {
        "OZ": "아시아나",
        "KE": "대한항공",
        "BX": "에어부산",
        "LJ": "진에어",
        "TW": "티웨이항공",
        "7C": "제주항공",
        "ZE": "이스타항공",
        "RS": "에어서울"
    }

    df["carDesc_official"] = df["carCode"].map(airline_map)
    mask_mismatch = df["carDesc"] != df["carDesc_official"]

    df.loc[mask_mismatch, "opCarCode"] = df.loc[mask_mismatch, "carCode"].map({
        "OZ": "BX",
        "KE": "LJ"
    })
    df.loc[mask_mismatch, "opCarDesc"] = df.loc[mask_mismatch, "carDesc"]
    df.loc[mask_mismatch, "carDesc"] = df.loc[mask_mismatch, "carDesc_official"]

    ##-----------------------------------------opCarCode, opCarDesc
    opCarDesc_map = {
        "BX": "에어부산",
        "LJ": "진에어"
    }
    df.loc[
        (df["opCarCode"].isin(opCarDesc_map.keys())) & (df["opCarDesc"].str.strip() == ""),
        "opCarDesc"
    ] = df["opCarCode"].map(opCarDesc_map)

    ##-----------------------------------------classCode, classDesc
    df["classDesc"] = df["classDesc"].astype(str).str.strip()

    ##-----------------------------------------seat
    seat_str = df["seat"].astype(str).str.strip()
    seat_str = seat_str.replace("", pd.NA)
    df['seat'] = pd.to_numeric(seat_str, errors="coerce")
    df['seat'] = df['seat'].fillna(0).astype(int)

    ##-----------------------------------------fare
    fare_str = df["fare"].astype(str).str.strip()
    fare_str = fare_str.replace("", pd.NA)
    df['fare'] = pd.to_numeric(fare_str, errors="coerce")
    df['fare'] = df['fare'].fillna(0).astype(int)

    ##-----------------------------------------fareOrigin
    fareOrigin_str = df["fareOrigin"].astype(str).str.strip()
    fareOrigin_str = fareOrigin_str.replace("", pd.NA)
    df['fareOrigin'] = pd.to_numeric(fareOrigin_str, errors="coerce")
    df['fareOrigin'] = df['fareOrigin'].fillna(0).astype(int)

    ##-----------------------------------------airTax
    airTax_str = df["airTax"].astype(str).str.strip()
    airTax_str = airTax_str.replace("", pd.NA)
    df['airTax'] = pd.to_numeric(airTax_str, errors="coerce")
    df['airTax'] = df['airTax'].fillna(0).astype(int)

    ##-----------------------------------------fuelChg
    fuelChg_str = df["fuelChg"].astype(str).str.strip()
    fuelChg_str = fuelChg_str.replace("", pd.NA)
    df['fuelChg'] = pd.to_numeric(fuelChg_str, errors="coerce")
    df['fuelChg'] = df['fuelChg'].fillna(0).astype(int)

    ##-----------------------------------------tasf
    tasf_str = df["tasf"].astype(str).str.strip()
    tasf_str = tasf_str.replace("", pd.NA)
    df['tasf'] = pd.to_numeric(tasf_str, errors="coerce")
    df['tasf'] = df['tasf'].fillna(0).astype(int)

    ##-----------------------------------------code (Maintain)
    ##-----------------------------------------mainFlt
    df["mainFlt"] = df["mainFlt"].astype(str).str.strip()

    ##-----------------------------------------agency_code(Maintain)
    ##-----------------------------------------scraped_date
    df["scraped_date"] = pd.to_datetime(df["scraped_date"], format="%Y-%m-%d")

    ##-----------------------------------------total_price(int)
    df["total_price"] = df["fare"] + df["fuelChg"] + df["airTax"] + df["tasf"]

    ##-----------------------------------------Formatting Options
    df.drop(columns=["carDesc_official"])
    column_order = [
        "agency_code","code",
        "depDate", "depDay", "depTime", "depCity","depDesc",
        "arrDate", "arrDay", "arrTime", "arrCity","arrDesc",
        "carCode", "carDesc", "opCarCode", "opCarDesc",
        "mainFlt", "classCode", "classDesc", "seat",
        "total_price", "fare", "fareOrigin", "fuelChg", "airTax", "tasf",
        "scraped_date", "source_file",
        "fareRecKey", "jejucomId", "itinInfo", "itinInfo2"
    ]
    df = df[column_order]

    if save_csv:
        output_path = PROCESSED_DIR/"preprocessing data.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"💾 CSV 저장 완료: {output_path}")
    logger.info("전처리 완료")
    return df

if __name__ == "__main__":
    run_preprocess(save_csv=True)
