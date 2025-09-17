@echo off
chcp 65001 >nul

echo.
echo == 1. Anaconda 가상환경 'airline_tickets'를 활성화합니다... ==
call conda activate airline_tickets
if %errorlevel% neq 0 (
    echo.
    echo [오류] 'airline_tickets' Conda 환경을 활성화할 수 없습니다.
    pause
    exit /b
)
echo.

echo == 2. DB에서 가장 최근 '출발 날짜(depDate)'를 조회합니다... ==
REM 파이썬 코드를 이용해 DB에서 가장 마지막 depDate를 조회합니다.
python -c "import sys, psycopg2, os; from dotenv import load_dotenv; sys.path.append('src'); load_dotenv(); db_config={'host': os.getenv('DB_HOST'), 'port': int(os.getenv('DB_PORT', 5432)), 'database': os.getenv('DB_NAME'), 'user': os.getenv('DB_USER'), 'password': os.getenv('DB_PASSWORD')}; conn = psycopg2.connect(**db_config); cur = conn.cursor(); cur.execute(\"SELECT TO_CHAR(MAX(\\\"depDate\\\"), 'YYYY-MM-DD') FROM flight_info;\"); latest_date = cur.fetchone()[0]; print(latest_date if latest_date else '데이터 없음'); cur.close(); conn.close();" > latest_date.tmp

set /p LATEST_DEP_DATE=<latest_date.tmp
del latest_date.tmp

echo ================================================================
echo  📊 DB에 저장된 가장 마지막 출발 날짜: %LATEST_DEP_DATE%
echo ================================================================
echo.

set /p START_DATE="시작 날짜(YYYYMMDD)를 입력하세요: "
set /p END_DATE="종료 날짜(YYYYMMDD)를 입력하세요: "

echo.
echo 🚀 파이프라인을 실행합니다 (시작: %START_DATE%, 종료: %END_DATE%)
echo.
python main.py --start-date %START_DATE% --end-date %END_DATE% --save-csv

echo.
echo ✅ 모든 작업이 완료되었습니다.
pause