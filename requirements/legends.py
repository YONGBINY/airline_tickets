# legends.py
import datetime

payload_pDep = {
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
pDep = ["GMP", "PUS", "CJU", "CJJ", "TAE", "MWX", "YNY", "KWJ", "USN", "RSU", "KPO", "HIN", "KUV", "WJU"]
pDep1 = ["GMP"]

payload_pArr = {
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
pArr = ["GMP", "PUS", "CJU", "CJJ", "TAE", "MWX", "YNY", "KWJ", "USN", "RSU", "KPO", "HIN", "KUV", "WJU"]
pArr1 = ["RSU", "PUS"]

payload_pAdt = "성인"
pAdt = str(1)
payload_pChd = "소아"
pChd = str(0)
payload_pInf = "유아"
pInf = str(0)

payload_pSeat = {
    "A": "전체",
    "Y": "일반석",
    "C": "비즈니스석",
    "S": "할인석",
    "T": "특가석",
}
pSeat = "A"

payload_comp = {
    "LT": "롯데투어",
    "IP": "인터파크",
    "JD": "하나투어",
    "SM": "선민투어",
    "WT": "웹투어",
    "YB2": "노랑풍선",
    "OT": "온라인투어",
    "JC": "제주도닷컴"
}
comp = ["LT", "IP", "JD", "SM", "WT", "YB2", "OT", "JC"]

start_date_str = "20250901"
end_date_str = "20250901"

start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d").date()
end_date = datetime.datetime.strptime(end_date_str, "%Y%m%d").date()
pDepDate = []

current_date = start_date
while current_date <= end_date:
    pDepDate.append(current_date.strftime("%Y%m%d"))
    current_date += datetime.timedelta(days=1)

# print(f"생성된 날짜 개수: {len(pDepDate)}")
# print(f"첫 5개 날짜: {pDepDate[:5]}")
# print(f"마지막 5개 날짜: {pDepDate[-5:]}")
