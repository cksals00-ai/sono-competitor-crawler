"""
단계별 크롤링 실행 스크립트

크롤링 → 대시보드 반영 → docs/ 복사 → iCloud 복사 → git push 를
OTA별로 나눠 단계적으로 실행한다.

각 단계가 완료될 때마다 중간 결과를 대시보드에 반영하고 push하므로
전체 완료 전에도 부분 데이터를 확인할 수 있다.

단계:
  --phase 1  : 야놀자 전체 크롤링 (약 10-15분)
  --phase 2  : Agoda 전체 크롤링 → 완료 후 오류건 재시도
  --phase 3  : 여기어때 전체 크롤링
  (인자 없음): 1 → 2 → 3 순서로 전체 실행

각 단계는 오늘 날짜 CSV에 결과를 누적(append)한 뒤 대시보드를 재생성한다.

Usage:
    python run_phased.py              # 전체 단계 순서 실행
    python run_phased.py --phase 1   # 야놀자만
    python run_phased.py --phase 2   # Agoda만
    python run_phased.py --phase 3   # 여기어때만
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

# ── 환경 설정 ─────────────────────────────────────────────────────────────────
PROJECT_DIR = Path(__file__).parent.resolve()
os.chdir(PROJECT_DIR)

ICLOUD_DIR  = Path.home() / "Library/Mobile Documents/com~apple~CloudDocs"
ICLOUD_FILE = ICLOUD_DIR / "소노_경쟁사_대시보드.html"

# ── 로거 설정 ─────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/phased.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── OTA 단계 정의 ─────────────────────────────────────────────────────────────
PHASES = {
    0: {
        "label": "0단계: 자사홈",
        "otas":  ["자사홈"],
    },
    1: {
        "label": "1단계: 야놀자",
        "otas":  ["야놀자"],
    },
    2: {
        "label": "2단계: Agoda",
        "otas":  ["Agoda"],
    },
    3: {
        "label": "3단계: 여기어때",
        "otas":  ["여기어때"],
    },
    4: {
        "label": "4단계: 네이버호텔",
        "otas":  ["네이버호텔"],
    },
    5: {
        "label": "5단계: Trip.com",
        "otas":  ["Trip.com"],
    },
}


# ── 헬퍼 함수 ─────────────────────────────────────────────────────────────────

def _today_csv_path() -> Path:
    """오늘 날짜 CSV 경로 반환."""
    with open("config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    export_dir = Path(cfg["output"]["export_dir"])
    today = datetime.today().strftime("%Y%m%d")
    csv_name = cfg["output"]["csv_filename"].format(date=today)
    return export_dir / csv_name


def _load_existing_df() -> pd.DataFrame:
    """오늘 기존 CSV가 있으면 로드, 없으면 어제 CSV에서 fallback.

    단일 phase만 실행할 때 다른 OTA 데이터가 누락되지 않도록
    어제 CSV를 기본 데이터로 사용한다.
    """
    path = _today_csv_path()
    if path.exists():
        try:
            df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
            logger.info(f"기존 CSV 로드: {path} ({len(df)} 행)")
            return df
        except Exception as e:
            logger.warning(f"기존 CSV 로드 실패 (무시): {e}")

    # 오늘 CSV가 없으면 어제 CSV에서 fallback
    with open("config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    export_dir = Path(cfg["output"]["export_dir"])
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    yesterday_csv = export_dir / cfg["output"]["csv_filename"].format(date=yesterday)
    if yesterday_csv.exists():
        try:
            df = pd.read_csv(yesterday_csv, encoding="utf-8-sig", low_memory=False)
            logger.info(f"어제 CSV fallback 로드: {yesterday_csv} ({len(df)} 행)")
            return df
        except Exception as e:
            logger.warning(f"어제 CSV 로드 실패 (무시): {e}")

    return pd.DataFrame()


def _merge_and_save(new_df: pd.DataFrame, ota_filter: list) -> pd.DataFrame:
    """
    기존 CSV에서 해당 OTA 데이터를 제거하고 새 데이터를 병합한 뒤 저장.
    같은 OTA를 재실행할 때 중복 없이 덮어쓴다.
    """
    from dashboard_generator import _normalize_columns

    existing = _load_existing_df()

    # 기존 CSV는 한글 컬럼명(_prepare_df 결과)이므로 영어로 정규화
    # → new_df(영어 컬럼)와 컬럼셋을 통일해야 수직 concat이 올바르게 동작한다.
    if not existing.empty:
        existing = _normalize_columns(existing)

    ota_col = "ota" if "ota" in existing.columns else ("OTA" if "OTA" in existing.columns else None)

    if not existing.empty and ota_col and ota_filter:
        # 이번 단계 OTA 데이터는 새 결과로 덮어씀
        existing = existing[~existing[ota_col].isin(ota_filter)]
        logger.info(f"기존 CSV에서 {ota_filter} 행 제거 후 새 데이터 병합")

    # 양쪽 모두 영어 컬럼으로 통일된 상태에서 수직 concat
    combined = pd.concat([existing, new_df], ignore_index=True)

    # 오늘 CSV에 저장 (export_powerbi.export_all 형식과 동일하게)
    from export_powerbi import _prepare_df, _save_csv, _save_excel, load_output_config
    out_cfg = load_output_config()
    export_dir = Path(out_cfg["export_dir"])
    export_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.today().strftime("%Y%m%d")

    csv_name   = out_cfg["csv_filename"].format(date=today)
    excel_name = out_cfg["excel_filename"].format(date=today)
    latest_name = out_cfg.get("powerbi_filename", "sono_competitor_prices_latest.xlsx")

    _save_csv(combined, export_dir / csv_name)
    _save_excel(combined, export_dir / excel_name)
    _save_excel(combined, export_dir / latest_name, sheet_name="최신데이터")
    logger.info(f"병합 저장 완료: {export_dir / csv_name} ({len(combined)} 행)")

    return combined


def _generate_dashboard(combined_df: pd.DataFrame):
    """대시보드 HTML 생성 → dashboard/index.html"""
    from dashboard_generator import generate_dashboard, load_previous_df, load_golf_df
    with open("config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    export_dir = cfg.get("output", {}).get("export_dir", "./exports")
    prev_df = load_previous_df(export_dir)
    golf_df = load_golf_df(export_dir)
    out = generate_dashboard(combined_df, "dashboard/index.html", prev_df=prev_df, golf_df=golf_df)
    logger.info(f"대시보드 생성: {out}")
    return out


def _copy_to_docs(dashboard_path: str):
    """dashboard/index.html → docs/index.html 복사"""
    src = Path(dashboard_path)
    dst = PROJECT_DIR / "docs" / "index.html"
    dst.parent.mkdir(exist_ok=True)
    shutil.copy2(src, dst)
    logger.info(f"docs/ 복사 완료: {dst}")


def _copy_to_icloud(dashboard_path: str):
    """iCloud Drive 동기화"""
    if ICLOUD_DIR.exists():
        try:
            shutil.copy2(dashboard_path, ICLOUD_FILE)
            logger.info(f"iCloud 복사 완료: {ICLOUD_FILE}")
        except Exception as e:
            logger.error(f"iCloud 복사 실패: {e}")
    else:
        logger.warning("iCloud Drive 경로를 찾을 수 없음 — iCloud 동기화 건너뜀")


def _git_push(phase_label: str):
    """docs/index.html + exports/ CSV/XLSX를 git add/commit/push"""
    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    today_date = datetime.today().strftime("%Y%m%d")

    # 스테이징할 파일 목록
    files_to_add = [
        "docs/index.html",
        f"exports/sono_competitor_prices_{today_date}.csv",
        f"exports/sono_competitor_prices_{today_date}.xlsx",
        "exports/sono_competitor_prices_latest.xlsx",
    ]

    for f in files_to_add:
        if Path(f).exists():
            subprocess.run(["git", "add", f], cwd=PROJECT_DIR, capture_output=True)

    commit_msg = f"dashboard: {phase_label} 완료 {today}"
    result = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        if "nothing to commit" in result.stdout or "nothing to commit" in result.stderr:
            logger.info("git: 변경사항 없음 — push 건너뜀")
            return
        logger.error(f"git commit 실패:\n{result.stderr.strip()}")
        return

    result = subprocess.run(
        ["git", "push"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error(f"git push 실패:\n{result.stderr.strip()}")
    else:
        logger.info(f"git push 완료: {commit_msg}")


def _retry_agoda_errors(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Agoda no_room_data 오류건을 재시도한다.
    오류가 있는 (사업장, 경쟁사명, 날짜) 조합만 재크롤링 후 성공하면 교체.
    """
    from crawler import crawl_agoda, load_config, close_driver

    # 컬럼명 탐지 (영어/한글 혼용 대응)
    ota_col   = "ota"   if "ota"   in combined_df.columns else "OTA"
    err_col   = "error" if "error" in combined_df.columns else "오류"
    prop_col  = "property_name" if "property_name" in combined_df.columns else "소노사업장"
    comp_col  = "competitor_name" if "competitor_name" in combined_df.columns else "경쟁사명"
    ci_col    = "checkin_date" if "checkin_date" in combined_df.columns else "체크인"
    co_col    = "checkout_date" if "checkout_date" in combined_df.columns else "체크아웃"
    url_col   = "url" if "url" in combined_df.columns else "URL"

    mask = (combined_df[ota_col] == "Agoda") & (combined_df[err_col] == "no_room_data")
    error_rows = combined_df[mask]

    if error_rows.empty:
        logger.info("Agoda 재시도: 오류 없음")
        return combined_df

    logger.info(f"Agoda 재시도 대상: {len(error_rows)} 건")
    cfg = load_config()

    retry_success = 0
    drop_indices = []

    # 경쟁사명+날짜 단위로 그룹핑해서 재시도
    for (comp_name, checkin), group in error_rows.groupby([comp_col, ci_col]):
        checkout = group[co_col].iloc[0]
        url = group[url_col].iloc[0]
        # base_url에서 쿼리 제거
        base_url = url.split("?")[0] if "?" in str(url) else str(url)
        competitor = {"name": comp_name, "agoda_url": base_url}

        try:
            new_records = crawl_agoda(competitor, checkin, checkout, cfg)
            valid = [r for r in new_records if not r.error]
            if valid:
                logger.info(f"  재시도 성공: {comp_name} {checkin} ({len(valid)}건)")
                drop_indices.extend(group.index.tolist())
                retry_df = pd.DataFrame([r.__dict__ for r in new_records])
                # property_name / property_id 복원
                retry_df["property_name"] = group["property_name"].iloc[0] if "property_name" in group.columns else ""
                retry_df["property_id"]   = group["property_id"].iloc[0]   if "property_id"   in group.columns else ""
                combined_df = pd.concat([combined_df, retry_df], ignore_index=True)
                retry_success += 1
            else:
                logger.warning(f"  재시도도 실패: {comp_name} {checkin}")
        except Exception as e:
            logger.error(f"  재시도 오류: {comp_name} {checkin}: {e}")

    close_driver()

    if drop_indices:
        combined_df = combined_df.drop(index=drop_indices).reset_index(drop=True)

    logger.info(f"Agoda 재시도 완료: {retry_success}/{len(error_rows.groupby([comp_col, ci_col]))} 성공")
    return combined_df


# ── 단계 실행 ─────────────────────────────────────────────────────────────────

def run_phase(phase_num: int, temp_output: bool = False):
    """
    phase_num : 1=야놀자, 2=Agoda, 3=여기어때
    temp_output: True면 exports/temp_phase{N}_{date}.csv 로만 저장하고
                 대시보드·git push 건너뜀. 병렬 실행 시 경합 방지용.
    """
    phase = PHASES[phase_num]
    label = phase["label"]
    otas  = phase["otas"]

    logger.info("=" * 60)
    logger.info(f"{label} 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    from crawler import run_crawl

    try:
        new_df = run_crawl(ota_filter=otas)
    except Exception as e:
        logger.error(f"{label} 크롤링 실패: {e}", exc_info=True)
        return

    if temp_output:
        # 병렬 실행용: 경합 없이 phase별 임시 CSV에 저장
        today = datetime.today().strftime("%Y%m%d")
        temp_path = Path(f"exports/temp_phase{phase_num}_{today}.csv")
        temp_path.parent.mkdir(parents=True, exist_ok=True)

        # 2단계(Agoda) 오류건 재시도
        if phase_num == 2 and not new_df.empty:
            logger.info("--- Agoda 오류 재시도 시작 ---")
            new_df = _retry_agoda_errors(new_df)

        from export_powerbi import _save_csv
        _save_csv(new_df, temp_path)
        logger.info(f"{label} 임시 CSV 저장 완료: {temp_path} ({len(new_df)} 행)")
        logger.info(f"{label} 완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return

    # 순차 실행 경로 (기존 동작 유지)
    combined_df = _merge_and_save(new_df, otas)

    if phase_num == 2:
        logger.info("--- Agoda 오류 재시도 시작 ---")
        combined_df = _retry_agoda_errors(combined_df)
        _merge_and_save(combined_df, otas)

    try:
        dashboard_path = _generate_dashboard(combined_df)
        _copy_to_docs(dashboard_path)
        _copy_to_icloud(dashboard_path)
        _git_push(label)
    except Exception as e:
        logger.error(f"{label} 대시보드/push 실패: {e}", exc_info=True)

    logger.info(f"{label} 완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ── 메인 ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="단계별 OTA 크롤링")
    parser.add_argument(
        "--phase", type=int, choices=[0, 1, 2, 3, 4, 5], default=None,
        help="실행할 단계 (0=자사홈, 1=야놀자, 2=Agoda, 3=여기어때, 4=네이버호텔, 5=Trip.com). 생략 시 전체 실행",
    )
    parser.add_argument(
        "--temp-output", action="store_true",
        help="phase별 임시 CSV만 저장 (병렬 실행 시 경합 방지용, 대시보드·push 건너뜀)",
    )
    args = parser.parse_args()

    if args.phase is not None:
        run_phase(args.phase, temp_output=args.temp_output)
    else:
        for p in [1, 4, 5]:
            run_phase(p)

        # 골프 그린피 크롤링 (호텔 크롤링 완료 후)
        try:
            from golf_crawler import run_golf_crawl, export_golf_df
            logger.info("=== 골프 그린피 크롤링 시작 ===")
            golf_df = run_golf_crawl()
            if not golf_df.empty:
                export_golf_df(golf_df)
            logger.info("=== 골프 그린피 크롤링 완료 ===")

            # 골프 크롤링 완료 후 대시보드 재생성 (골프 섹션 포함)
            try:
                from export_powerbi import load_output_config
                out_cfg = load_output_config()
                export_dir = Path(out_cfg["export_dir"])
                today = datetime.today().strftime("%Y%m%d")
                csv_name = out_cfg["csv_filename"].format(date=today)
                csv_path = export_dir / csv_name
                if csv_path.exists():
                    from dashboard_generator import _normalize_columns
                    combined_df = _normalize_columns(pd.read_csv(csv_path, encoding="utf-8-sig"))
                    dashboard_path = _generate_dashboard(combined_df)
                    _copy_to_docs(dashboard_path)
                    _copy_to_icloud(dashboard_path)
                    _git_push("골프 크롤링")
                    logger.info("=== 골프 포함 대시보드 재생성 완료 ===")
            except Exception as e:
                logger.error(f"골프 대시보드 재생성 실패: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"골프 크롤링 실패 (Power BI 수집에는 영향 없음): {e}", exc_info=True)

        # 전체 phase 완료 후 Power BI RNS 수집 (당월 + 다음달 + 그 다음달 3개월)
        try:
            from powerbi_collector import collect_multi_months
            logger.info("=== Power BI RNS 수집 시작 (3개월 투숙기준) ===")
            collect_multi_months()
            logger.info("=== Power BI RNS 수집 완료 ===")
        except Exception as e:
            logger.error(f"Power BI 수집 실패 (크롤링 결과에는 영향 없음): {e}", exc_info=True)

        # 전체 완료 후 추이 데이터 전처리
        try:
            from preprocess_trends import run as preprocess_run
            from export_powerbi import load_output_config
            out_cfg = load_output_config()
            export_dir = Path(out_cfg["export_dir"])
            analytics_dir = PROJECT_DIR / "analytics"
            logger.info("=== 추이 전처리 시작 ===")
            preprocess_run(export_dir, analytics_dir, parquet=True)
            logger.info(f"=== 추이 전처리 완료 → {analytics_dir} ===")
        except Exception as e:
            logger.error(f"추이 전처리 실패 (크롤링 결과에는 영향 없음): {e}", exc_info=True)


if __name__ == "__main__":
    main()
