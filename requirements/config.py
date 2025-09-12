# config.py
from datetime import datetime, timedelta

AIRPORTS = {
    "GMP": "서울/김포",
    "PUS": "부산/김해",
    "CJU": "제주",
    "CJJ": "청주",
    "TAE": "대구",
    "MWX": "무안",
    "YNY": "양양",
    "KWJ": "광주",
    "USN": "울산",
    "RSU": "여수",
    "KPO": "포항경주",
    "HIN": "사천",
    "KUV": "군산",
    "WJU": "횡성/원주"
}

AGENTS = {
    "LT": "롯데투어",
    "IP": "인터파크",
    "JD": "하나투어",
    "SM": "선민투어",
    "WT": "웹투어",
    "YB2": "노랑풍선",
    "OT": "온라인투어",
    "JC": "제주도닷컴"
}

SEAT_CLASSES = {
    "A": "전체",
    "Y": "일반석",
    "C": "비즈니스석",
    "S": "할인석",
    "T": "특가석"
}

PASSENGERS = {
    "adult": 1,
    "child": 0,
    "infant": 0
}

def generate_dates(start: str, end: str) -> list:
    start_dt = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")
    dates = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates

DEP_DATES = generate_dates("20251005", "20251005")
# print(f"생성된 날짜 개수: {len(DEP_DATES)}")
# print(f"첫 5개 날짜: {DEP_DATES[:5]}")
# print(f"마지막 5개 날짜: {DEP_DATES[-5:]}")

DEPARTURES = list(AIRPORTS.keys())
ARRIVALS = list(AIRPORTS.keys())
AGENT_CODES = list(AGENTS.keys())
CABIN_CLASS = "A"

print(DEPARTURES)
print(ARRIVALS)