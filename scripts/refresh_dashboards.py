#!/usr/bin/env python3
"""
refresh_dashboards.py — 매일 06:00 대시보드 전체 자동 갱신
=============================================================
파이프라인:
  [1] 팔라티움 리포트  : parse_palatium.py → build_palatium.py
  [2] 트렌드 리포트    : parse_raw_db.py → db_to_notes.py → build.py → git push

LaunchAgent: ~/Library/LaunchAgents/com.sono.dashboard-refresh.plist
실행 환경: venv python (/Projects/sono-competitor-crawler/venv/bin/python)

뉴스 수집(collect_news.py)은 별도 스케줄 또는 수동 실행.
"""
import logging
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

# ─── 경로 ───
PALATIUM_DIR  = Path(__file__).resolve().parent.parent          # sono-competitor-crawler/
PALATIUM_SCR  = PALATIUM_DIR / "scripts"
PALATIUM_DB   = Path("/Users/chanminpark/Desktop/gs_daily_trend_news_public_temp/data/palatium_db")

TREND_DIR     = Path("/Users/chanminpark/Desktop/gs_daily_trend_news_public_temp")
TREND_SCR     = TREND_DIR / "scripts"


def run(cmd: list, cwd: Path = None, check: bool = True) -> bool:
    cwd_str = str(cwd or Path.cwd())
    logger.info(f"$ {' '.join(str(c) for c in cmd)}  (cwd={cwd_str})")
    try:
        r = subprocess.run(cmd, cwd=cwd_str, check=check, text=True,
                           capture_output=False)
        return r.returncode == 0
    except subprocess.CalledProcessError as e:
        logger.error(f"실패 (code {e.returncode})")
        return False


def step(title: str) -> None:
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"  {title}")
    logger.info("=" * 60)


# ─────────────────────────────────────────
# 팔라티움 파이프라인
# ─────────────────────────────────────────
def run_palatium() -> bool:
    step("[1/2] 팔라티움 리포트 갱신")
    ok = True

    parse_pl = PALATIUM_SCR / "parse_palatium.py"
    build_pl = PALATIUM_SCR / "build_palatium.py"

    if not PALATIUM_DB.exists():
        logger.error(f"팔라티움 DB 디렉터리 없음: {PALATIUM_DB}")
        return False

    if parse_pl.exists():
        ok &= run([sys.executable, str(parse_pl), str(PALATIUM_DB)], cwd=PALATIUM_DIR)
    else:
        logger.warning(f"parse_palatium.py 없음: {parse_pl}")

    if build_pl.exists():
        ok &= run([sys.executable, str(build_pl)], cwd=PALATIUM_DIR)
    else:
        logger.warning(f"build_palatium.py 없음: {build_pl}")

    if ok:
        ok &= _git_push(PALATIUM_DIR)

    return ok


# ─────────────────────────────────────────
# 트렌드 리포트 파이프라인
# ─────────────────────────────────────────
def run_trend() -> bool:
    step("[2/2] 트렌드 리포트 갱신")

    if not TREND_DIR.exists():
        logger.error(f"트렌드 리포트 레포 없음: {TREND_DIR}")
        return False

    ok = True

    # parse_raw_db.py — stdlib만 사용, venv python 또는 system python3 모두 가능
    ok &= run([sys.executable, str(TREND_SCR / "parse_raw_db.py")], cwd=TREND_DIR)
    if not ok:
        logger.error("parse_raw_db.py 실패 — 이후 단계 중단")
        return False

    # db_to_notes.py
    ok &= run([sys.executable, str(TREND_SCR / "db_to_notes.py")], cwd=TREND_DIR)
    if not ok:
        logger.error("db_to_notes.py 실패 — 빌드 중단")
        return False

    # build.py
    ok &= run([sys.executable, str(TREND_SCR / "build.py")], cwd=TREND_DIR)

    # git push
    if ok:
        ok &= _git_push(TREND_DIR)

    return ok


def _git_push(repo: Path) -> bool:
    now = datetime.now(KST)
    msg = f"chore(auto): daily update {now.strftime('%Y-%m-%d %H:%M')} KST [skip ci]"

    # git config (처음 실행 환경 대비)
    run(["git", "config", "user.email", "action@github.com"], cwd=repo, check=False)
    run(["git", "config", "user.name",  "GS Auto-Bot"],       cwd=repo, check=False)

    # 변경 확인
    r = subprocess.run(["git", "status", "--porcelain"], cwd=str(repo),
                       capture_output=True, text=True)
    if not r.stdout.strip():
        logger.info("변경사항 없음 — 커밋 스킵")
        return True

    run(["git", "add", "data/", "docs/", "scripts/"], cwd=repo, check=False)
    commit_ok = run(["git", "commit", "-m", msg], cwd=repo, check=False)
    if not commit_ok:
        logger.info("커밋할 변경 없음 (이미 처리됨)")
        return True

    push_ok = run(["git", "push"], cwd=repo, check=False)
    if push_ok:
        logger.info("✓ GitHub 푸시 완료")
    else:
        logger.warning("⚠ 푸시 실패 — 로컬 커밋은 유지됨 (수동 push 필요)")
    return push_ok


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def main() -> None:
    start = datetime.now(KST)
    logger.info("=" * 60)
    logger.info(f"대시보드 자동 갱신 시작  {start.strftime('%Y-%m-%d %H:%M KST')}")
    logger.info("=" * 60)

    pal_ok   = run_palatium()
    trend_ok = run_trend()

    elapsed = (datetime.now(KST) - start).seconds
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"완료  소요: {elapsed}초")
    logger.info(f"  팔라티움: {'✓' if pal_ok   else '✗'}")
    logger.info(f"  트렌드:   {'✓' if trend_ok else '✗'}")
    logger.info("=" * 60)

    if not (pal_ok and trend_ok):
        sys.exit(1)


if __name__ == "__main__":
    main()
