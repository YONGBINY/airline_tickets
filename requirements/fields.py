# fields.py
FIELD_DESCRIPTIONS = {
    # --- 항공편 정보 ---
    "code": "항공편 코드",
    "mainFlt": "항공편 식별 번호",
    "depCity": "출발 도시",
    "arrCity": "도착 도시",
    "depDate": "출발 날짜",
    "arrDate": "도착 날짜",
    "depTime": "출발 시간",
    "arrTime": "도착 시간",
    "depDay": "출발 요일",
    "arrDay": "도착 요일",
    "carCode": "판매 여행사 코드",
    "opCarCode": "운항 항공사 코드",
    "carDesc": "판매 여행사 이름",
    "opCarDesc": "운항 항공사 이름",
    "classCode": "좌석 클래스 코드",
    "classDesc": "좌석 설명",
    "seat": "남은 좌석 수",
    "fare": "운임/기본요금",
    "fareOrigin": "원본 운임 요금",
    "airTax": "공항세",
    "fuelChg": "유류 할증료",
    "tasf": "발권 수수료",
    "fareRecKey": "운임 키"
}

HEADER_DESCRIPTIONS = {
    "errorCode": "오류 코드",
    "errorDesc": "호출 결과 메시지",
    "agentCode": "여행사 코드",
    "dep": "출발지",
    "arr": "도착지",
    "depDate": "출발 날짜",
    "adt": "성인 수",
    "chd": "소아 수",
    "inf": "유아 수"
}

def calculate_total(fare: int, airTax: int, fuelChg: int, tasf: int) -> int:
    return fare + airTax + fuelChg + tasf