"""
3채널 순차 크롤링: 야놀자 → Trip.com → 네이버호텔
결과는 /tmp/crawl_*.log 에 저장 + 최종 대시보드 생성
"""
import logging
import os
import pickle
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.resolve()
os.chdir(PROJECT_DIR)

LOG_FILE = "/tmp/sono_3ch_crawl.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

import pandas as pd
import yaml

from crawler import run_crawl
from export_powerbi import export_all


def run_channel(ota_name: str, pkl_path: str) -> pd.DataFrame:
    logger.info("=" * 60)
    logger.info(f"[{ota_name}] 크롤링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    t0 = time.time()
    df = run_crawl(ota_filter=[ota_name])
    elapsed = time.time() - t0
    logger.info(f"[{ota_name}] 완료 — {len(df)}행, 소요: {elapsed/60:.1f}분")
    logger.info(f"RESULT:{ota_name}:{len(df)}")
    with open(pkl_path, "wb") as f:
        pickle.dump(df, f)
    return df


if __name__ == "__main__":
    dfs = []

    # 1. 야놀자
    df_ya = run_channel("야놀자", "/tmp/df_yanolja.pkl")
    dfs.append(df_ya)

    # 2. Trip.com (hotel_id=0 자동 스킵)
    df_trip = run_channel("Trip.com", "/tmp/df_tripcom.pkl")
    dfs.append(df_trip)

    # 3. 네이버호텔
    df_naver = run_channel("네이버호텔", "/tmp/df_naver.pkl")
    dfs.append(df_naver)

    # 4. 브랜드몰 (자사몰 — homepage_store_cd 있는 사업장만 수집)
    df_brand = run_channel("브랜드몰", "/tmp/df_brandmall.pkl")
    dfs.append(df_brand)

    # 합치기 + 내보내기
    df_all = pd.concat(dfs, ignore_index=True)
    logger.info(f"전체 합계: {len(df_all)}행")
    export_all(df_all)
    logger.info("Excel/CSV 내보내기 완료")

    # 대시보드 생성
    try:
        from dashboard_generator import generate_dashboard, load_previous_df
        with open("config.yaml", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        export_dir = cfg.get("output", {}).get("export_dir", "./exports")
        prev_df = load_previous_df(export_dir)
        out = generate_dashboard(df_all, "dashboard/index.html", prev_df=prev_df)
        logger.info(f"대시보드 생성 완료: {out}")
        logger.info("DASHBOARD:OK")
    except Exception as e:
        logger.error(f"대시보드 생성 실패: {e}", exc_info=True)
        logger.info("DASHBOARD:FAIL")

    # docs/ 동기화
    import shutil
    src = PROJECT_DIR / "dashboard" / "index.html"
    dst = PROJECT_DIR / "docs" / "index.html"
    if src.exists():
        shutil.copy2(src, dst)
        logger.info(f"docs/index.html 업데이트")

    # git add/commit/push
    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    for cmd in [
        ["git", "add", "exports/", "dashboard/", "docs/"],
        ["git", "commit", "-m", f"crawl: 4채널 자동수집 {today} (브랜드몰+야놀자+Trip.com+네이버)"],
        ["git", "push"],
    ]:
        result = subprocess.run(cmd, cwd=PROJECT_DIR, capture_output=True, text=True)
        if result.returncode != 0:
            if "nothing to commit" in result.stdout + result.stderr:
                logger.info("git: 변경사항 없음")
                break
            logger.error(f"git 실패: {' '.join(cmd)}\n{result.stderr.strip()}")
        else:
            logger.info(f"git: {' '.join(cmd)} 완료")

    logger.info("ALL_DONE")
