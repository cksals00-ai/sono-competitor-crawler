"""
소노 경쟁사 크롤러 스케줄러
- 매일 지정 시간에 크롤링 + Excel/CSV 저장을 자동 실행
- 실행: python scheduler.py
- 백그라운드 실행: nohup python scheduler.py &
"""

import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

import schedule
import time
import yaml

ICLOUD_DIR  = Path.home() / "Library/Mobile Documents/com~apple~CloudDocs"
ICLOUD_FILE = ICLOUD_DIR / "소노_경쟁사_대시보드.html"

from crawler import run_crawl
from export_powerbi import export_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def daily_job():
    logger.info("=" * 60)
    logger.info(f"일일 크롤링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    try:
        df = run_crawl()
        export_all(df)
        logger.info("일일 크롤링 및 내보내기 완료")
    except Exception as e:
        logger.error(f"일일 크롤링 실패: {e}", exc_info=True)
        return

    # HTML 대시보드 생성
    try:
        from dashboard_generator import generate_dashboard, load_previous_df
        with open("config.yaml", encoding="utf-8") as _f:
            _cfg = yaml.safe_load(_f)
        export_dir = _cfg.get("output", {}).get("export_dir", "./exports")
        prev_df = load_previous_df(export_dir)
        out = generate_dashboard(df, "dashboard/index.html", prev_df=prev_df)
        logger.info(f"HTML 대시보드 생성 완료: {out}")

        # iCloud Drive 동기화
        try:
            if ICLOUD_DIR.exists():
                shutil.copy2(out, ICLOUD_FILE)
                logger.info(f"iCloud 복사 완료: {ICLOUD_FILE}")
            else:
                logger.warning("iCloud Drive 경로를 찾을 수 없음 — iCloud 동기화 건너뜀")
        except Exception as ie:
            logger.error(f"iCloud 복사 실패: {ie}")

    except Exception as e:
        logger.error(f"HTML 대시보드 생성 실패: {e}", exc_info=True)


def main():
    # 매일 오전 7시 실행 (필요시 시간 변경)
    run_time = "07:00"
    schedule.every().day.at(run_time).do(daily_job)
    logger.info(f"스케줄러 시작. 매일 {run_time}에 크롤링 실행")

    # 시작 즉시 1회 실행 옵션 (--now 인자)
    if "--now" in sys.argv:
        logger.info("--now 옵션: 즉시 1회 실행")
        daily_job()

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
