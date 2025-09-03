# load_all_history.py
import os
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import glob

# ---------------------------------- 설정
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "airline_tickets",
    "user": "postgres",
    "password": "qkrdydqls12!"  # ⚠️ 본인 비밀번호로 변경
}

JSON_ROOT_DIR = "flight information"  # JSON이 저장된 루트 폴더
BATCH_SIZE = 500  # 일괄 삽입 단위 (성능 조정 가능)

# ---------------------------------- 데이터 변환 함수
def parse_flight_data(file_path: str, scraped_date: str):
    print(f"\n📄 처리 중: {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        print(f"  ✅ JSON 파싱 성공")

        # --- 1. 최상위 'data' 확인 ---
        if "data" not in raw:
            print(f"  ❌ 최상위 'data' 키 없음")
            return []

        data_block = raw["data"]

        # --- 2. header 확인 ---
        header = data_block.get("header", {})
        error_code = header.get("errorCode", "Unknown")
        error_desc = header.get("errorDesc", "N/A")
        print(f"  🔹 errorCode: {error_code}")
        print(f"  🔹 errorDesc: {error_desc}")

        success_codes = ["0", "0000", "9999"]
        if error_code not in success_codes:
            print(f"  ❌ 실패 응답({error_code})→ 건너뜀")
            return []

        # --- 3. 항공편 데이터 확인 ---
        if "data" not in data_block:
            print(f"  ❌ 내부 'data' 배열 없음")
            return []

        flights = data_block["data"]
        if not isinstance(flights, list):
            print(f"  ❌ 항공편 데이터가 리스트가 아님: {type(flights)}")
            return []

        print(f"  ✅ 성공! {len(flights)}개 항공편 발견")

        # --- 4. 파일명에서 travel_agent_code 추출 ---
        filename = os.path.basename(file_path)
        parts = filename.replace(".json", "").split("_")
        if len(parts) < 4:
            print(f"  ❌ 파일명 형식 오류: {filename}")
            return []
        travel_agent_code = parts[-1]
        print(f"  🔹 여행사 코드: {travel_agent_code}")

        # --- 5. 항공편 레코드 생성 ---
        records = []
        for item in flights:
            try:
                dep_date = datetime.strptime(str(item["depDate"]), "%Y%m%d").date()
                arr_date = datetime.strptime(str(item["arrDate"]), "%Y%m%d").date() if item.get("arrDate") else None

                dep_time_raw = str(item['depTime'])
                arr_time_raw = str(item['arrTime']) if item.get('arrTime') else None

                dep_time = f"{dep_time_raw[:2]}:{dep_time_raw[2:]}" if len(dep_time_raw) == 4 else None
                arr_time = f"{arr_time_raw[:2]}:{arr_time_raw[2:]}" if arr_time_raw and len(arr_time_raw) == 4 else None

                service_fee = int(item["tasf"]) if item["tasf"] else 0

                record = (
                    item["depCity"],
                    item["depDesc"],
                    dep_date,
                    item.get("depDay"),
                    dep_time,
                    item["arrCity"],
                    item["arrDesc"],
                    arr_date,
                    item.get("arrDay"),
                    arr_time,
                    item.get("carCode"),
                    item.get("carDesc"),
                    item.get("opCarCode") or item.get("carCode"),
                    item.get("opCarDesc") or item.get("carDesc"),
                    item.get("classCode"),
                    item.get("classDesc"),
                    item.get("seat"),
                    int(item.get("fare")),
                    int(item.get("fareOrigin")) if item.get("fareOrigin") else 0,
                    int(item.get("airTax")),
                    int(item.get("fuelChg")),
                    service_fee,
                    item.get("code"),
                    str(item.get("mainFlt")),
                    travel_agent_code,
                    scraped_date,
                    json.dumps(raw, ensure_ascii=False)
                )
                records.append(record)
            except Exception as e:
                print(f"  ⚠️ 항공편 처리 실패: {e}")
                print(f"     [Error] {file_path}")
                print(f"     [Data] {item}")
                continue

        return records

    except json.JSONDecodeError as e:
        print(f"  ❌ JSON 형식 오류: {e}")
        return []
    except Exception as e:
        print(f"  ❌ 파일 처리 중 오류: {e}")
        return []

# ---------------------------------- 메인 실행
if __name__ == "__main__":
    print(f"[{datetime.now()}] 🚀 전체 과거 데이터 일괄 적재 시작")

    # DB 연결
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("✅ PostgreSQL 연결 성공")
    except Exception as e:
        print(f"❌ DB 연결 실패: {e}")
        exit()

    total_files = 0
    total_records = 0
    all_records = []

    # 1. 모든 JSON 파일 탐색
    json_files = glob.glob(os.path.join(JSON_ROOT_DIR, "**", "*.json"), recursive=True)
    print(f"📁 처리할 파일 수: {len(json_files)}")

    for file_path in json_files:
        # scraped_date: 중간 폴더명 (예: 2025-04-05)
        parts = file_path.split(os.sep)
        if len(parts) < 3:
            continue
        scraped_date = parts[-3]  # flight information/2025-04-05/20250901/file.json

        records = parse_flight_data(file_path, scraped_date)
        all_records.extend(records)
        total_records += len(records)
        total_files += 1

        if total_files % 100 == 0:
            print(f"📄 {total_files}개 파일 처리 완료... (총 {total_records}건 수집)")

    print(f"✅ 모든 파일 처리 완료: {total_files}개 파일, {total_records}개 항공편")

    # 2. 일괄 삽입
    insert_success = False
    if all_records:
        insert_sql = """
        INSERT INTO flights (
            dep_code, dep_name, dep_date, dep_day, dep_time,
            arr_code, arr_name, arr_date, arr_day, arr_time,
            marketing_airline_code, marketing_airline_name,
            operating_airline_code, operating_airline_name,
            class_code, class_description, available_seats,
            base_fare, fare_origin, air_tax, fuel_surcharge, service_fee,
            flight_code, flight_number,
            travel_agent_code, scraped_date, raw_json
        ) VALUES %s
        ON CONFLICT DO NOTHING;  -- 중복 방지 (기본키 기반)
        """

        try:
            execute_values(cur, insert_sql, all_records, page_size=BATCH_SIZE)
            conn.commit()
            print(f"🎉 성공! {total_records}개 레코드를 DB에 일괄 삽입 완료")
            insert_success = True
        except Exception as e:
            print(f"❌ 일괄 삽입 실패: {e}")
            conn.rollback()
            insert_success = False
    else:
        print("📭 삽입할 데이터가 없습니다.")
        insert_success = False

    cur.close()
    conn.close()

    print(f"\n📊 최종 결과")
    print(f"📁 처리한 파일 수: {total_files}")
    print(f"✅ 성공적으로 추출된 항공편 수: {total_records}")
    print(f"💾 DB 적재 성공 여부: {'성공' if insert_success else '실패'}")

    if not insert_success and total_records > 0:
        print("⚠️ 경고: 데이터는 추출되었으나, DB 적재에 실패했습니다.")
        print("💡 원인: NULL 값, 데이터 타입 불일치, 인덱스 문제 등")