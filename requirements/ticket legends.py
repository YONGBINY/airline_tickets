data = {
    "fare": "운임/기본요금",
    "airTax": "공항세",
    "code": "항공편 코드",
    "arrDate": "도착 날짜",
    "arrCity": "도착 도시",
    "depCity": "출발 도시",
    "fareRecKey": "운임 키",
    "classDesc": "01",
    "depDate": "출발 날짜",
    "depDesc": "출발 도시",
    "carCode": "항공사 코드",
    "opCarCode": "운항 항공사 코드",
    "classCode": "좌석 클래스 코드",
    "depTime": "출발 시간",
    "fareOrigin": "원본 운임 요금",
    "fuelChg": "유류 할증료",
    "depDay": "출발 요일",
    "carDesc": "항공사 이름",
    "seat": "남은 좌석 수",
    "tasf": "발권 수수료",
    "arrDesc": "도착 도시",
    "arrDay": "도착 날짜",
    "opCarDesc": "운항 항공사 이름",
    "mainFlt": "항공편 식별 번호",
    "arrTime": "도착 시간"
}

# fare(≒fareOrigin) + airTax + fuelChg + tasf = Payment Amount

# USE DATE

## data
### code
### (CREATE) fare + airTax + fuelChg + tasf = Payment Amount
### fareOrigin
### airTax
### fuelChg
### tasf
### depDate
### depCity
### depTime
### depDay
### arrDate
### arrCity
### arrTime
### arrDay
### classDesc
### carCode
### opcarCode
### classCode
### carDesc
### opcarDesc
### mainFlt
### fareRecKey

## header
### agentCode
### adt
### chd
### inf

header = {
    "arr": "도착지",
    "inf": "유아",
    "errorDesc": "호출 결과 메세지",
    "agentCode": "여행사 이름",
    "arrCity": "도착 공항",
    "cnt": "카운트",
    "depCity": "출발 공항",
    "errorCode": "오류 코드",
    "dep": "출발지",
    "depDate": "출발 날짜",
    "adt": "성인",
    "depDesc": "출발 도시",
    "arrDesc": "도착 도시",
    "chd": "소아"
}