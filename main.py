# main.py
import sys
import asyncio
from datetime import datetime
import click

from src.collect import run_collect_async as run_collect
from src.preprocess import run_preprocess
from src.upload import run_upload
from src.common.logging_setup import setup_logging

logger = setup_logging(__name__)

def parse_yyyymmdd(value: str):
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        raise click.BadParameter("날짜 형식은 YYYYMMDD 이어야 합니다. 예: 20250901")

async def run_pipeline(start_date, end_date, save_csv):
    click.secho(f"\n🚀 '{start_date}'부터 '{end_date}'까지의 데이터 파이프라인을 시작합니다.", fg="green")
    await run_collect(start_date, end_date)
    df = run_preprocess(save_csv=save_csv)
    if df is not None and not df.empty:
        logger.info(f"전처리 완료. {len(df)}건의 데이터를 업로드합니다.")
        run_upload(df)
    else:
        logger.warning("전처리된 데이터가 없어 업로드 단계를 건너뜁니다.")

@click.command(help="항공권 데이터 파이프라인 실행 (수집 → 전처리 → 업로드)")
@click.option("--start-date", "start_date_str", required=True, help="검색 시작 날짜 (YYYYMMDD)")
@click.option("--end-date", "end_date_str", required=True, help="검색 끝 날짜 (YYYYMMDD)")
@click.option("--save-csv", is_flag=True, help="전처리 결과를 CSV로 저장")
def cli_main(start_date_str, end_date_str, save_csv):
    start_date = parse_yyyymmdd(start_date_str)
    end_date = parse_yyyymmdd(end_date_str)
    if start_date > end_date:
        raise click.BadParameter("시작 날짜가 끝 날짜보다 늦을 수 없습니다.")
    asyncio.run(run_pipeline(start_date, end_date, save_csv))

def manual_run():
    start_date = datetime(2025, 10, 20).date()
    end_date = datetime(2025, 10, 20).date()
    save_csv = True
    logger.info(f"[수동 실행] 검색 범위: {start_date} ~ {end_date}, CSV 저장: {save_csv}")
    asyncio.run(run_pipeline(start_date, end_date, save_csv))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cli_main()
    else:
        manual_run()

## Run Terminal commands
## python main.py --start-date [시작날짜] --end-date [끝날짜] --save-csv