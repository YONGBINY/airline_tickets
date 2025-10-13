# Airline Tickets Data Pipeline

## 프로젝트 개요
국내 항공권 데이터를 airport.co.kr에서 수집/전처리/DB 업로드하는 자동화 파이프라인입니다.
1. Selenium과 aiohttp로 스크래핑
2. Pandas로 데이터 정제
3. PostgreSQL로 저장

## 주요 기능
- **수집 (collect.py)**: 비동기 요청으로 출발/도착지별 항공편 정보 JSON 저장.
- **전처리 (preprocess.py)**: 데이터 정규화(공항 코드, 날짜/시간), 중복 제거 후 CSV 저장.
- **업로드 (upload.py)**: CSV를 DB에 배치 업로드 (중복 방지).
- **실행 (main.py)**: CLI로 전체 파이프라인 실행.

## 설치 및 실행
1. 리포 클론: `git clone https://github.com/YONGBINY/airline_tickets.git`
2. 의존성 설치: `pip install -r requirements.txt` (requirements.txt 파일 만들어주세요 – selenium, pandas, sqlalchemy 등 나열.)
3. 설정: src/common/config.py에 DB_CONFIG 입력 (PostgreSQL 연결 정보).
4. 실행: `python main.py --start-date 20251020 --end-date 20251024 --save-csv`

## 파일 구조
```
airline_tickets/
├── README.md           # Overview
├── main.py             # 메인 실행기 (CLI 파이프라인)
├── run_pipeline .bat    # bat 실행기
├── .env                # (비공개) DB 사용자 정보
├── src/
│   ├── common/             # 공통 모듈
│   │   ├── logging_setup.py        # 로깅 설정
│   │   ├── paths.py                # 파일 경로 관리 (RAW_DIR, PROCESSED_DIR)
│   │   └── config.py               # DB 설정 (DB_CONFIG)
│   ├── collect.py          # 데이터 수집 (Selenium + aiohttp 비동기 스크래핑)
│   ├── preprocess.py       # 데이터 전처리 (Pandas로 정제, CSV 저장)
│   └── upload.py           # DB 업로드 (PostgreSQL 배치 삽입)
│
├── data/               # (비공개) Data
│   ├── raw/                # 원본 json 파일
│   └── processed/          # 전처리 된 csv 파일
├── logs/               # (비공개) pipeline logs
├── notebooks/          # (비공개) 분석 작업용 Jupyternotebook
├── reports/            # (비공개) 분석 내용 정리
└── requirements/       # (미정)
    ├──edgedriver_win64/    # 웹 드라이버 정보
    ├──config.py            # Define scraping parameters    
    └──fields.py            # Defining header values
```

## 주의사항
- Selenium 드라이버 경로(DRIVER_PATH) 확인.
- src/common/config.py 에서 .env DB 사용자 정보 확인.
- 법적: 스크래핑 시 사이트 이용약관 준수.

## 기여
Issue & PR 환영