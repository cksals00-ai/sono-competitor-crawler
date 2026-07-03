#!/usr/bin/env python3
"""
refresh_dashboards.py — 매일 06:00 대시보드 전체 자동 갱신
=============================================================
파이프라인:
  [1] 팔라티움 리포트  : parse_palatium.py → build_palatium.py → git push
  (트렌드 리포트 갱신은 gs repo host_daily_crawl.sh(05:00)+GitHub Actions가 전담 — 이 잡에서 제거)

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

    # 경쟁사 프로모션 수집 (B: 공식홈 크롤 + A: 가격 CSV 특가신호) → data/competitors.json
    # best-effort: 실패해도 기존 competitors.json 유지하고 파이프라인 계속.
    promo_scr = PALATIUM_DIR / "crawl_competitor_promos.py"
    if promo_scr.exists():
        run([sys.executable, str(promo_scr)], cwd=PALATIUM_DIR, check=False)

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


# 트렌드 리포트 파이프라인(run_trend)은 제거됨 — gs repo의 host_daily_crawl.sh(05:00)+
# GitHub Actions가 온북·트렌드 갱신을 전담한다. 과거 이 단계는 db_to_notes.py(→generate_insights.py로
# 리네임됨) 부재로 매일 실패했고, gs repo로의 이중 push만 유발하는 중복 파이프라인이었다.


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

    pal_ok = run_palatium()
    # 트렌드 단계는 gs repo의 host_daily_crawl.sh(05:00)+GitHub Actions가 전담하므로 제거.
    # (과거 db_to_notes.py→generate_insights.py 리네임 이후 이 단계가 매일 실패했고,
    #  중복 파이프라인이라 gs repo로의 이중 push만 유발했음.)

    elapsed = (datetime.now(KST) - start).seconds
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"완료  소요: {elapsed}초")
    logger.info(f"  팔라티움: {'✓' if pal_ok else '✗'}")
    logger.info("  트렌드:   ⏭ skip (gs host_daily_crawl 전담)")
    logger.info("=" * 60)

    if not pal_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
