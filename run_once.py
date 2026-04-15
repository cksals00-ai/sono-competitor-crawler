"""
launchd 전용: 크롤링 + 내보내기 + HTML 대시보드 생성을 1회 실행 후 종료.
scheduler.py는 무한루프(while True)가 있어 launchd와 함께 쓰면 중복 실행되므로
launchd plist에서는 이 파일을 직접 호출한다.

Usage:
    python run_once.py
"""

import logging
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

# plist의 WorkingDirectory 설정이 있어도, 직접 실행 시 이 파일 위치 기준으로 chdir
PROJECT_DIR = Path(__file__).parent.resolve()
os.chdir(PROJECT_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/crawler.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

from scheduler import daily_job  # noqa: E402 (import after chdir/logging setup)


def _git_push_dashboard():
    """dashboard/index.html → docs/index.html 복사 후 git add/commit/push."""
    src = PROJECT_DIR / "dashboard" / "index.html"
    dst = PROJECT_DIR / "docs" / "index.html"

    if not src.exists():
        logger.warning(f"GitHub Pages 동기화 건너뜀: {src} 없음")
        return

    try:
        shutil.copy2(src, dst)
        logger.info(f"docs/index.html 업데이트: {dst}")
    except Exception as e:
        logger.error(f"docs/ 복사 실패: {e}")
        return

    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    cmds = [
        ["git", "add", "docs/index.html"],
        ["git", "commit", "-m", f"dashboard: auto-update {today}"],
        ["git", "push"],
    ]
    for cmd in cmds:
        result = subprocess.run(
            cmd,
            cwd=PROJECT_DIR,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            # "nothing to commit" is not a real error
            if "nothing to commit" in result.stdout or "nothing to commit" in result.stderr:
                logger.info("git: 변경사항 없음 — push 건너뜀")
                return
            logger.error(f"git 명령 실패: {' '.join(cmd)}\n{result.stderr.strip()}")
            return
        logger.info(f"git: {' '.join(cmd)} 완료")

    logger.info("GitHub Pages 자동 배포 완료")


if __name__ == "__main__":
    daily_job()
    _git_push_dashboard()
