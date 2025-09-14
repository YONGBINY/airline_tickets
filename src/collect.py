# collect.py

import os
import time
import json
import requests
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options

from requirements.config import DEPARTURES, ARRIVALS, AGENT_CODES, PASSENGERS, CABIN_CLASS, AGENTS
from src.common.logging_setup import setup_logging
from src.common.paths import RAW_DIR

logger = setup_logging(__name__)

#-------------------------------------- 1. 설정
DRIVER_PATH = r"D:\Users\bin\PycharmProjects\airline_tickets\requirements\edgedriver_win64\msedgedriver.exe"
BASE_URL = "https://www.airport.co.kr"
API_URL = f"{BASE_URL}/booking/ajaxf/frAirticketSvc/getData.do"
TARGET_URL = f"{BASE_URL}/booking/cms/frCon/index.do?MENU_ID=80"

ROOT_OUTPUT_DIR = RAW_DIR
os.makedirs(ROOT_OUTPUT_DIR, exist_ok=True)

#-------------------------------------- 2. Edge 옵션
def create_edge_options():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    )
    return options

#-------------------------------------- 3. 쿠키 획득
def get_cookies():
    service = Service(DRIVER_PATH)
    driver = None
    try:
        driver = webdriver.Edge(service=service, options=create_edge_options())
        driver.get(TARGET_URL)
        time.sleep(3)
        cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
        print(f"✅ 쿠키 {len(cookies)}개 획득 완료")
        return cookies
    except Exception as e:
        print(f"❌ 쿠키 획득 실패: {e}")
        return {}
    finally:
        if driver:
            driver.quit()

#-------------------------------------- 날짜 리스트 생성
def generate_dates(start_date, end_date):
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates

#-------------------------------------- 4. 항공권 조회 및 저장
def search_flights(cookies, pDep, pArr, pDepDate, pAdt, pChd, pInf, pSeat, comp, base_output_dir):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": TARGET_URL,
        "Origin": BASE_URL,
        "X-Requested-With": "XMLHttpRequest"
    }

    payload = {
        "pDep": pDep,
        "pArr": pArr,
        "pDepDate": pDepDate,
        "pArrDate": "",
        "pAdt": pAdt,
        "pChd": pChd,
        "pInf": pInf,
        "pSeat": pSeat,
        "comp": comp,
        "carCode": "ALL"  # ✅ YB2 등 여행사 조회에 필수
    }

    session = requests.Session()
    session.cookies.update(cookies)
    session.headers.update(headers)

    try:
        logger.info(f"요청: {pDep} → {pArr}, {pDepDate}, {AGENTS.get(comp, comp)}")
        response = session.post(API_URL, data=payload, timeout=15)

        if response.status_code == 200:
            try:
                result = response.json()

                # ✅ 저장 구조: flight information/[수집일자]/[운항일자]/GMP_PUS_20250901_JD.json
                acquisition_date = datetime.now().strftime("%Y-%m-%d")
                collection_dir = os.path.join(base_output_dir, acquisition_date)
                flight_date_dir = os.path.join(collection_dir, pDepDate)
                os.makedirs(flight_date_dir, exist_ok=True)

                # 파일명 생성 (특수문자 제거)
                safe_dep = pDep.replace("/", "_").replace("\\", "_")
                safe_arr = pArr.replace("/", "_").replace("\\", "_")
                safe_comp = comp.replace("/", "_").replace("\\", "_")
                filename = f"{safe_dep}_{safe_arr}_{pDepDate}_{safe_comp}.json"
                filepath = os.path.join(flight_date_dir, filename)

                # 저장
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False, indent=4)

                header = result.get("data", {}).get("header", {})
                cnt = header.get("cnt", 0)
                error = header.get("errorDesc", "") if header.get("errorCode") != "0" else "정상"
                logger.info(f"저장 완료: {filepath} | 응답: {error} | 편수: {cnt}")

            except json.JSONDecodeError:
                logger.error("응답이 JSON 형식이 아님")
            except Exception as e:
                logger.error(f"파일 저장 실패: {e}")
        else:
            logger.error(f"요청 실패: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logger.error(f"요청 중 오류: {e}")
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}")

#-------------------------------------- 5. 메인 실행
def run_collect(start_date, end_date):
    logger.info("데이터 수집 시작")

    cookies = get_cookies()
    if not cookies:
        logger.error("쿠키 획득 실패, 프로그램 종료")
        return

    # ✅ 수집 루트 폴더
    base_output_path = os.path.join(ROOT_OUTPUT_DIR)
    os.makedirs(base_output_path, exist_ok=True)

    dep_dates = generate_dates(start_date, end_date)
    total_combinations = len(DEPARTURES) * len(ARRIVALS) * len(dep_dates) * len(AGENT_CODES)

    logger.info(f"총 요청 수 예상: {total_combinations}건")

    # ✅ 반복 조회
    processed = 0
    start_time = time.time()

    for dep in DEPARTURES:
        for arr in ARRIVALS:
            if dep == arr:
                continue
            for date in dep_dates:
                for agent in AGENT_CODES:
                    processed += 1
                    search_flights(
                        cookies=cookies,
                        pDep=dep,
                        pArr=arr,
                        pDepDate=date,
                        pAdt=str(PASSENGERS["adult"]),
                        pChd=str(PASSENGERS["child"]),
                        pInf=str(PASSENGERS["infant"]),
                        pSeat=CABIN_CLASS,
                        comp=agent,
                        base_output_dir=base_output_path
                    )
                    time.sleep(1)

        elapsed = time.time() - start_time
        logger.info(f"전체 수집 완료: {processed}/{total_combinations} 요청 | 소요 시간: {elapsed:.1f}초")

    if __name__ == "__main__":
        run_collect(datetime(2025, 9, 1).date(), datetime(2025, 9, 5).date())
