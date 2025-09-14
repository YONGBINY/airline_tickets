# main.py
import sys
from datetime import datetime
import click

from src.collect import run_collect
from src.preprocess import run_preprocess
from src.upload import run_upload
from src.common.logging_setup import setup_logging

logger = setup_logging(__name__)

def parse_yyyymmdd(value: str):
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        raise click.BadParameter("날짜 형식은 YYYYMMDD 이어야 합니다. 예: 20250901")

@click.command(help="항공권 데이터 파이프라인 실행 (수집 → 전처리 → 업로드)")
@click.option("--start-date", "start_date_str", required=True, help="검색 시작 날짜 (YYYYMMDD)")
@click.option("--end-date", "end_date_str", required=True, help="검색 끝 날짜 (YYYYMMDD)")
@click.option("--save-csv", is_flag=True, help="전처리 결과를 CSV로 저장")

def cli_main(start_date_str, end_date_str, save_csv):
    start_date = parse_yyyymmdd(start_date_str)
    end_date = parse_yyyymmdd(end_date_str)

    if start_date > end_date:
        raise click.BadParameter("시작 날짜가 끝 날짜보다 늦을 수 없습니다.")

    click.secho(f"검색 범위: {start_date} ~ {end_date}", fg="cyan")

    run_collect(start_date, end_date)
    df = run_preprocess(save_csv=save_csv)
    run_upload(df)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # CLI 모드 (터미널에서 인자 전달)
        cli_main()
    else:
        # PyCharm 수동 실행 모드 (값 직접 지정)
        start_date = datetime(2025, 9, 1).date()
        end_date = datetime(2025, 9, 5).date()
        save_csv = True

        logger.info(f"[수동 실행] 검색 범위: {start_date} ~ {end_date}, CSV 저장: {save_csv}")
        run_collect(start_date, end_date)
        df = run_preprocess(save_csv=save_csv)
        run_upload(df)