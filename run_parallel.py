#!/usr/bin/env python3
"""
3개 OTA를 병렬 크롤링 후 CSV 병합 → 대시보드 생성 → git push.

각 phase를 별도 subprocess로 실행해 exports/temp_phase{N}_{date}.csv 에 저장.
모두 완료되면 3개 temp CSV를 병합해 최종 CSV/Excel을 생성한다.

Usage:
    python run_parallel.py
"""

import logging
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_DIR = Path(__file__).parent.resolve()
os.chdir(PROJECT_DIR)

# ── 로거 설정 ─────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/parallel.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

ICLOUD_DIR  = Path.home() / "Library/Mobile Documents/com~apple~CloudDocs"
ICLOUD_FILE = ICLOUD_DIR / "소노_경쟁사_대시보드.html"


def _run_phase_subprocess(phase_num: int, today: str) -> bool:
    """run_phased.py --phase N --temp-output 을 새 세션(os.setsid)으로 실행.

    preexec_fn=os.setsid 으로 자식을 독립 프로세스 그룹에 배치하므로
    부모(run_parallel.py)가 SIGTERM을 받아도 자식은 계속 실행된다.
    """
    log_path = PROJECT_DIR / f"logs/phase{phase_num}_{today}_{datetime.now().strftime('%H%M%S')}.log"
    logger.info(f"[Phase {phase_num}] 시작 → 로그: {log_path.name}")

    with open(log_path, "w", encoding="utf-8") as log_f:
        result = subprocess.run(
            [sys.executable, "run_phased.py", "--phase", str(phase_num), "--temp-output"],
            cwd=PROJECT_DIR,
            stdout=log_f,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,  # 새 세션 → 독립 프로세스 그룹
        )

    if result.returncode == 0:
        logger.info(f"[Phase {phase_num}] subprocess 종료: returncode=0")
    else:
        logger.error(f"[Phase {phase_num}] subprocess 종료: returncode={result.returncode}")

    return result.returncode == 0


def _merge_temp_csvs(today: str) -> pd.DataFrame:
    """temp_phase*.csv 를 병합해 단일 DataFrame 반환."""
    dfs = []
    for phase_num in [1, 2, 3]:
        temp_path = PROJECT_DIR / f"exports/temp_phase{phase_num}_{today}.csv"
        if temp_path.exists():
            try:
                df = pd.read_csv(temp_path, encoding="utf-8-sig")
                logger.info(f"  temp CSV 로드: {temp_path.name} ({len(df)} 행)")
                dfs.append(df)
            except Exception as e:
                logger.error(f"  temp CSV 로드 실패: {temp_path.name}: {e}")
        else:
            logger.warning(f"  temp CSV 없음: {temp_path.name} — 해당 phase 데이터 누락")

    if not dfs:
        raise RuntimeError("병합할 temp CSV가 하나도 없습니다. 크롤링이 모두 실패했습니다.")

    combined = pd.concat(dfs, ignore_index=True)
    logger.info(f"병합 완료: 총 {len(combined)} 행 ({len(dfs)}개 phase)")
    return combined


def _save_final(combined: pd.DataFrame, today: str):
    """최종 CSV/Excel 저장."""
    import yaml
    from export_powerbi import _save_csv, _save_excel, load_output_config

    out_cfg = load_output_config()
    export_dir = PROJECT_DIR / out_cfg["export_dir"]
    export_dir.mkdir(parents=True, exist_ok=True)

    csv_name   = out_cfg["csv_filename"].format(date=today)
    excel_name = out_cfg["excel_filename"].format(date=today)
    latest_name = out_cfg.get("powerbi_filename", "sono_competitor_prices_latest.xlsx")

    _save_csv(combined, export_dir / csv_name)
    _save_excel(combined, export_dir / excel_name)
    _save_excel(combined, export_dir / latest_name, sheet_name="최신데이터")
    logger.info(f"최종 CSV 저장: {export_dir / csv_name} ({len(combined)} 행)")


def _generate_dashboard(combined: pd.DataFrame) -> str:
    from dashboard_generator import generate_dashboard, load_previous_df
    import yaml
    with open("config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    export_dir = cfg.get("output", {}).get("export_dir", "./exports")
    prev_df = load_previous_df(export_dir)
    out = generate_dashboard(combined, "dashboard/index.html", prev_df=prev_df)
    logger.info(f"대시보드 생성: {out}")
    return out


def _copy_to_docs(dashboard_path: str):
    src = Path(dashboard_path)
    dst = PROJECT_DIR / "docs" / "index.html"
    dst.parent.mkdir(exist_ok=True)
    shutil.copy2(src, dst)
    logger.info(f"docs/ 복사 완료: {dst}")


def _copy_to_icloud(dashboard_path: str):
    if ICLOUD_DIR.exists():
        try:
            shutil.copy2(dashboard_path, ICLOUD_FILE)
            logger.info(f"iCloud 복사 완료: {ICLOUD_FILE}")
        except Exception as e:
            logger.error(f"iCloud 복사 실패: {e}")


def _git_push(today: str):
    files_to_add = [
        "docs/index.html",
        f"exports/sono_competitor_prices_{today}.csv",
        f"exports/sono_competitor_prices_{today}.xlsx",
        "exports/sono_competitor_prices_latest.xlsx",
    ]
    for f in files_to_add:
        if Path(f).exists():
            subprocess.run(["git", "add", f], cwd=PROJECT_DIR, capture_output=True)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    commit_msg = f"dashboard: 3채널 병렬 크롤링 완료 {now}"
    result = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        cwd=PROJECT_DIR, capture_output=True, text=True,
    )
    if result.returncode != 0:
        if "nothing to commit" in result.stdout + result.stderr:
            logger.info("git: 변경사항 없음 — push 건너뜀")
            return
        logger.error(f"git commit 실패:\n{result.stderr.strip()}")
        return

    result = subprocess.run(
        ["git", "push"], cwd=PROJECT_DIR, capture_output=True, text=True,
    )
    if result.returncode != 0:
        logger.error(f"git push 실패:\n{result.stderr.strip()}")
    else:
        logger.info(f"git push 완료: {commit_msg}")


def main():
    today = datetime.today().strftime("%Y%m%d")
    logger.info("=" * 60)
    logger.info(f"병렬 크롤링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"오늘 날짜: {today}")
    logger.info("=" * 60)

    # 이전 temp 파일 정리
    for phase_num in [1, 2, 3]:
        temp_path = PROJECT_DIR / f"exports/temp_phase{phase_num}_{today}.csv"
        if temp_path.exists():
            temp_path.unlink()
            logger.info(f"이전 temp CSV 삭제: {temp_path.name}")

    # 3개 phase 병렬 실행
    results = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(_run_phase_subprocess, phase_num, today): phase_num
            for phase_num in [1, 2, 3]
        }
        for future in as_completed(futures):
            phase_num = futures[future]
            try:
                ok = future.result()
                results[phase_num] = ok
            except Exception as e:
                logger.error(f"[Phase {phase_num}] 예외: {e}")
                results[phase_num] = False

    logger.info(f"병렬 실행 결과: {results}")
    success_count = sum(1 for ok in results.values() if ok)
    if success_count == 0:
        logger.error("모든 phase가 실패했습니다. 중단합니다.")
        sys.exit(1)

    # temp CSV 병합 → 최종 저장
    try:
        combined = _merge_temp_csvs(today)
        _save_final(combined, today)
    except Exception as e:
        logger.error(f"CSV 병합/저장 실패: {e}", exc_info=True)
        sys.exit(1)

    # 대시보드 생성 → docs/ → iCloud → git push
    try:
        dashboard_path = _generate_dashboard(combined)
        _copy_to_docs(dashboard_path)
        _copy_to_icloud(dashboard_path)
        _git_push(today)
    except Exception as e:
        logger.error(f"대시보드/push 실패: {e}", exc_info=True)

    logger.info("=" * 60)
    logger.info(f"전체 완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"결과: exports/sono_competitor_prices_{today}.csv")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
