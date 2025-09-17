# collect.py
import os
import time
import json
import asyncio
import aiohttp
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options

# requirements.config와 로깅 설정은 기존과 동일하다고 가정합니다.
from requirements.config import DEPARTURES, ARRIVALS, AGENT_CODES, PASSENGERS, CABIN_CLASS, AGENTS
from src.common.logging_setup import setup_logging
from src.common.paths import RAW_DIR

logger = setup_logging(__name__)

# ---------------------------------- 1. 설정 (기존과 동일)
DRIVER_PATH = r"D:\Users\bin\PycharmProjects\airline_tickets\requirements\edgedriver_win64\msedgedriver.exe"
BASE_URL = "https://www.airport.co.kr"
API_URL = f"{BASE_URL}/booking/ajaxf/frAirticketSvc/getData.do"
TARGET_URL = f"{BASE_URL}/booking/cms/frCon/index.do?MENU_ID=80"
ROOT_OUTPUT_DIR = RAW_DIR
os.makedirs(ROOT_OUTPUT_DIR, exist_ok=True)


# ---------------------------------- 2. Selenium 관련 함수 (기존과 동일)
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

def get_cookies():
    service = Service(DRIVER_PATH)
    driver = None
    try:
        driver = webdriver.Edge(service=service, options=create_edge_options())
        driver.get(TARGET_URL)
        time.sleep(3)
        cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
        logger.info(f"✅ 쿠키 {len(cookies)}개 획득 완료")
        return cookies
    except Exception as e:
        logger.error(f"❌ 쿠키 획득 실패: {e}")
        return {}
    finally:
        if driver:
            driver.quit()

# ---------------------------------- 3. 날짜 리스트 생성 (기존과 동일)
def generate_dates(start_date, end_date):
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates

# ---------------------------------- 4. ✨ 비동기 항공권 조회 및 저장
async def search_flight_async(session, semaphore, params):
    MAX_RETRIES = 3
    BASE_DELAY = 2
    pDep, pArr, pDepDate, comp = params["pDep"], params["pArr"], params["pDepDate"], params["comp"]

    for attempt in range(MAX_RETRIES):
        async with semaphore:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Referer": TARGET_URL
            }
            try:
                if attempt > 0:
                    logger.warning(f"재시도 ({attempt + 1}/{MAX_RETRIES}): {pDep}→{pArr}, {pDepDate}, {comp}")
                else:
                    logger.info(f"요청 시작: {pDep}→{pArr}, {pDepDate}, {AGENTS.get(comp, comp)}")

                async with session.post(API_URL, data=params, headers=headers, timeout=20) as response:
                    if response.status == 200:
                        result = await response.json(content_type=None)

                        if attempt > 0:
                            logger.info(f"✅ 재시도 성공: {pDep}→{pArr}, {pDepDate}, {comp}")

                        acquisition_date = datetime.now().strftime("%Y-%m-%d")
                        collection_dir = os.path.join(params["base_output_dir"], acquisition_date)
                        flight_date_dir = os.path.join(collection_dir, pDepDate)
                        os.makedirs(flight_date_dir, exist_ok=True)

                        filename = f"{pDep}_{pArr}_{pDepDate}_{comp}.json"
                        filepath = os.path.join(flight_date_dir, filename)

                        with open(filepath, "w", encoding="utf-8") as f:
                            json.dump(result, f, ensure_ascii=False, indent=4)

                        header = result.get("data", {}).get("header", {})
                        cnt = header.get("cnt", 0)
                        error = header.get("errorDesc", "") if header.get("errorCode") != "0" else "정상"

                        logger.info(f"저장 완료: {pDep}→{pArr}, {pDepDate}, {AGENTS.get(comp, comp)} | 편수: {cnt}")
                        return {"filepath": filepath, "raw_data": result}
                    else:
                        logger.error(f"요청 실패 ({response.status}): {pDep}→{pArr}, {pDepDate}, {comp}")
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                logger.error(f"오류 발생 ({pDep}→{pArr}, {pDepDate}, {comp}): {e}")
            if attempt < MAX_RETRIES - 1:
                delay = BASE_DELAY * (2 ** attempt)
                logger.warning(f"{delay}초 후 재시도합니다...")
                await asyncio.sleep(delay)

    logger.critical(f"최종 실패: {pDep}→{pArr}, {pDepDate}, {comp}")
    return None

# ---------------------------------- 5. ✨ 메인 실행 함수 (수정됨)
async def run_collect_async(start_date, end_date):
    logger.info("비동기 데이터 수집 시작")

    cookies = get_cookies()
    if not cookies:
        logger.error("쿠키 획득 실패, 프로그램 종료")
        return

    base_output_path = os.path.join(ROOT_OUTPUT_DIR)
    os.makedirs(base_output_path, exist_ok=True)
    dep_dates = generate_dates(start_date, end_date)

    # --- 1. 모든 요청 조합을 미리 생성 ---
    tasks_params = []
    for dep in DEPARTURES:
        for arr in ARRIVALS:
            if dep == arr: continue
            for date in dep_dates:
                for agent in AGENT_CODES:
                    payload = {
                        "pDep": dep, "pArr": arr, "pDepDate": date,
                        "pAdt": str(PASSENGERS["adult"]), "pChd": str(PASSENGERS["child"]),
                        "pInf": str(PASSENGERS["infant"]), "pSeat": CABIN_CLASS, "comp": agent,
                        "carCode": "ALL", "base_output_dir": base_output_path, "pArrDate": ""
                    }
                    tasks_params.append(payload)

    logger.info(f"총 요청 수: {len(tasks_params)}건")

    # --- 2. 비동기 작업 실행 ---
    start_time = time.time()

    semaphore = asyncio.Semaphore(50)
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=50, ttl_dns_cache=300)

    async with aiohttp.ClientSession(cookies=cookies, connector=connector) as session:
        tasks = [search_flight_async(session, semaphore, params) for params in tasks_params]
        results = await asyncio.gather(*tasks)

    elapsed = time.time() - start_time

    # --- 👇 [추가] 최종 결과 요약 로그 ---
    collected_data = [r for r in results if r is not None]

    success_count = len(collected_data)
    failure_count = len(tasks_params) - success_count

    saved_count = len(collected_data)

    logger.info("=" * 50)
    logger.info("📊 전체 수집 결과 요약")
    logger.info(f"  - 총 요청 수: {len(tasks_params)} 건")
    logger.info(f"  - ✅ 성공: {success_count} 건")
    logger.info(f"  - ❌ 실패: {failure_count} 건")
    logger.info(f"  - 💾 저장된 JSON 파일 수: {saved_count} 건")
    logger.info(f"  - ⏱️ 총 소요 시간: {elapsed:.2f} 초")
    logger.info("=" * 50)

    return collected_data

# ---------------------------------- 실행
if __name__ == "__main__":
    start_dt = datetime(2025, 10, 20).date()
    end_dt = datetime(2025, 10, 24).date()
    asyncio.run(run_collect_async(start_dt, end_dt))