# requirements/fields.py
FIELD_DESCRIPTIONS = {
    # --- 항공편 정보 ---
    "code": "항공편 코드",
    "mainFlt": "항공편 식별 번호",

    "depDesc": "출발지 이름",
    "depCity": "출발 공항 이름(IATA)",
    "depDate": "출발 날짜",
    "depDay": "출발 요일",
    "depTime": "출발 시간",

    "arrDesc": "도착지 이름",
    "arrCity": "도착 공항 이름(IATA)",
    "arrDate": "도착 날짜",
    "arrDay": "도착 요일",
    "arrTime": "도착 시간",

    "carCode": "항공사 코드",
    "carDesc": "항공사 이름",
    "opCarCode": "운항 항공사 코드",
    "opCarDesc": "운항 항공사 이름",

    "classDesc": "좌석 설명",
    "classCode": "좌석 클래스 코드",

    "fareOrigin": "원본 운임 요금",
    "fare": "운임/기본요금",
    "fuelChg": "유류 할증료",
    "airTax": "공항세",
    "tasf": "발권 수수료",

    "fareRecKey": "운임 키",
    "jejucomId": "운임 키",
    "itinInfo2": "운임 키",

    "seat": "남은 좌석 수",
}

HEADER_DESCRIPTIONS = {
    "dep": "출발 공항 코드",
    "depDesc": "출발지 이름",
    "depCity": "출발 공항 이름(IATA)",
    "depDate": "출발 날짜",
    "arr": "도착 공항 코드",
    "arrDesc": "도착지 이름",
    "arrCity": "도착 공항 이름(IATA)",
    "adt": "성인 수",
    "chd": "소아 수",
    "inf": "유아 수",
    "agentCode": "여행사 코드",
    "cnt": "조회 된 수",
    "errorCode": "오류 코드",
    "errorDesc": "결과 호출 메세지"
}

def calculate_total(fare: int, airTax: int, fuelChg: int, tasf: int) -> int:
    return fare + airTax + fuelChg + tasf