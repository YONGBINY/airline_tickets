import requests
from requests import session
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
import time
import json

#-------------------------------------- 1-1. Edge option definition
driver_path = r"D:\Users\bin\PycharmProjects\airline_tickets\requirements\edgedriver_win64\msedgedriver.exe"

edge_options = Options()
edge_options.add_argument("--headless")
edge_options.add_argument("--disable-gpu")
edge_options.add_argument("--no-sandbox")
edge_options.add_argument("--disable-dev-shm-usage")
edge_options.add_argument(
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)

#-------------------------------------- 1-2. URL constant
BASE_URL = "https://www.airport.co.kr"
API_URL  = f"{BASE_URL}/booking/ajaxf/frAirticketSvc/getData.do"
TARGET_URL = f"{BASE_URL}/booking/cms/frCon/index.do?MENU_ID=80"

#-------------------------------------- 1-3. Get Cookie
def get_cookies_from_selenium(target_page_url: str) -> dict:
    print(f"Get cookies by connecting to '{target_page_url}' by Selenium...\n")

    service = Service(driver_path)
    driver = None
    try:
        driver = webdriver.Edge(service=service, options=edge_options)
        driver.get(target_page_url)
        time.sleep(3)

        cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
        #print("--- 가져온 쿠키 ---")
        #print(cookies)
        #print("-------------------\n")
        return cookies
    except Exception as e:
        print(f"Error: {e}")
        return {}
    finally:
        if driver:
            driver.quit()

#-------------------------------------- 2-1. Payload
payload = {
    "pDep":      "GMP",
    "pArr":      "PUS",
    "pDepDate":  "20250901",
    "pArrDate":  "",
    "pAdt":      "1",
    "pChd":      "0",
    "pInf":      "0",
    "pSeat":     "A",
    "comp":      "JD",
    "carCode":   "ALL"
}

#-------------------------------------- 2-2. fetch flights
def fetch_flights(cookies: dict, payload: dict) -> dict:
    session = requests.Session()
    session.headers.update({
        "User-Agent": edge_options.arguments[-1].split("=")[1],
        "Referer": TARGET_URL,
        "X-Requested-With": "XMLHttpRequest"
    })
    session.cookies.update(cookies)

    resp = session.post(API_URL, data=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()

#-------------------------------------- 5. Immediately run
if __name__ == "__main__":
    cookies = get_cookies_from_selenium(TARGET_URL)
    if not cookies:
        print("쿠키 확보 실패")
        exit()

    flights_json = fetch_flights(cookies, payload)
    print(json.dumps(flights_json, ensure_ascii=False, indent=2))
