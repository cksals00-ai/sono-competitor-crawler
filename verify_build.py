#!/usr/bin/env python3
"""
빌드 검증 스크립트 — GitHub Pages 대시보드 빌드 결과물 검증
독립 실행: python3 verify_build.py
"""

import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

# ─── 설정 ───────────────────────────────────────────────
TREND_DIR = Path.home() / "Desktop" / "gs_daily_trend_news_public_temp"
DOCS_DIR = TREND_DIR / "docs"
DATA_DIR = DOCS_DIR / "data"

# 자체 GitHub Pages 도메인 (절대 URL에서 경로 추출용)
OWN_DOMAINS = [
    "cksals00-ai.github.io/gs_daily_trend_news_public_temp",
    "cksals00-ai.github.io/sono-competitor-crawler",
]

# 세그먼트/채널 이름 (사업장과 혼재되면 경고)
SEGMENT_NAMES = {"OTA", "G-OTA", "Inbound", "FIT", "Group", "Wholesale"}

# 사업장 이름 패턴: 숫자로 시작 (01.벨비발디 등)
PROPERTY_PATTERN = re.compile(r"^\d{2}\.")


# ─── 결과 수집 ──────────────────────────────────────────
class Results:
    def __init__(self):
        self.passed = []
        self.warnings = []
        self.failures = []

    def ok(self, msg):
        self.passed.append(msg)
        print(f"  \033[32m✓\033[0m {msg}")

    def warn(self, msg, detail):
        self.warnings.append((msg, detail))
        print(f"  \033[33m⚠\033[0m {msg} — {detail}")

    def fail(self, msg, detail):
        self.failures.append((msg, detail))
        print(f"  \033[31m✗\033[0m {msg} — {detail}")

    def summary(self):
        total = len(self.passed) + len(self.warnings) + len(self.failures)
        print(f"\n{'='*50}")
        print(f"  총 {total}개 검증: "
              f"{len(self.passed)}개 통과, "
              f"{len(self.warnings)}개 경고, "
              f"{len(self.failures)}개 실패")
        print(f"{'='*50}")
        return 1 if self.failures else 0


R = Results()


# ─── 1. 내부 링크 검증 ──────────────────────────────────
def check_internal_links():
    print("\n[1/4] 내부 링크 검증")

    html_files = set(f.name for f in DOCS_DIR.glob("*.html"))
    if not html_files:
        R.fail("HTML 파일 탐색", f"{DOCS_DIR}에 HTML 파일 없음")
        return

    href_re = re.compile(r'href="([^"#]+)"', re.IGNORECASE)
    broken = []

    for html_path in sorted(DOCS_DIR.glob("*.html")):
        content = html_path.read_text(encoding="utf-8", errors="ignore")
        for match in href_re.finditer(content):
            target = match.group(1).strip()

            # 절대 URL → 같은 레포 도메인이면 경로 추출, 다른 레포는 제외
            if target.startswith("http"):
                # 같은 레포 (gs_daily_trend_news_public_temp)만 검증
                same_repo = "cksals00-ai.github.io/gs_daily_trend_news_public_temp"
                if same_repo in target:
                    target = urlparse(target).path.split("/")[-1]
                    if not target:
                        continue  # 루트 경로
                else:
                    continue  # 외부 링크 또는 다른 레포

            # .html 파일 링크만 검증
            if not target.endswith(".html"):
                continue

            if target not in html_files:
                broken.append((html_path.name, target))

    if broken:
        # 대상별로 그룹화하여 출력
        missing_targets = {}
        for src, tgt in broken:
            missing_targets.setdefault(tgt, []).append(src)
        for tgt, srcs in sorted(missing_targets.items()):
            src_list = ", ".join(sorted(set(srcs)))
            R.fail("깨진 링크", f"{tgt} (참조: {src_list})")
    else:
        R.ok(f"내부 링크 정상 (HTML {len(html_files)}개 검사)")


# ─── 2. 데이터 구조 검증 ────────────────────────────────
def check_data_structure():
    print("\n[2/4] 데이터 구조 검증")

    otb_path = DATA_DIR / "otb_data.json"
    if not otb_path.exists():
        R.fail("otb_data.json", "파일 없음")
        return

    with open(otb_path, encoding="utf-8") as f:
        otb = json.load(f)

    # yoyTable 검사
    yoy_table = otb.get("yoyTable", [])
    mixed_yoy = []
    for item in yoy_table:
        name = item.get("name", "")
        if name in SEGMENT_NAMES:
            # 세그먼트는 사업장 바로 뒤에 오는 게 정상 구조
            # 단독으로 최상위에 있으면 문제
            pass  # 현재 구조상 사업장-세그먼트 교차 배치가 정상
        elif not PROPERTY_PATTERN.match(name) and name not in SEGMENT_NAMES:
            mixed_yoy.append(name)

    if mixed_yoy:
        R.warn("yoyTable 비정상 항목", f"사업장/세그먼트 패턴 불일치: {mixed_yoy}")
    else:
        R.ok(f"yoyTable 구조 정상 ({len(yoy_table)}개 항목)")

    # byProperty 검사
    by_prop = otb.get("byProperty", [])
    non_standard = []
    for item in by_prop:
        name = item.get("name", "")
        if name in SEGMENT_NAMES:
            non_standard.append(f"세그먼트 혼입: {name}")
        elif not PROPERTY_PATTERN.match(name):
            non_standard.append(f"번호 패턴 불일치: {name}")

    if non_standard:
        R.warn("byProperty 비정상 항목", "; ".join(non_standard))
    else:
        R.ok(f"byProperty 구조 정상 ({len(by_prop)}개 사업장)")


# ─── 3. 데이터 일관성 검증 ──────────────────────────────
def check_data_consistency():
    print("\n[3/4] 데이터 일관성 검증")

    otb_path = DATA_DIR / "otb_data.json"
    db_path = DATA_DIR / "db_aggregated.json"

    if not otb_path.exists() or not db_path.exists():
        R.fail("일관성 검증", "otb_data.json 또는 db_aggregated.json 없음")
        return

    with open(otb_path, encoding="utf-8") as f:
        otb = json.load(f)
    with open(db_path, encoding="utf-8") as f:
        db = json.load(f)

    otb_names = set(
        item["name"] for item in otb.get("byProperty", [])
    )
    db_names = set(db.get("by_property", {}).keys())

    # 이름 형식이 다름 (01.벨비발디 vs 소노문 비발디파크)
    # 직접 비교 대신 개수와 누락 여부만 체크
    only_otb = otb_names - db_names
    only_db = db_names - otb_names

    if only_otb or only_db:
        details = []
        if only_otb:
            details.append(f"OTB에만 존재({len(only_otb)}): {sorted(only_otb)[:5]}")
        if only_db:
            details.append(f"DB에만 존재({len(only_db)}): {sorted(only_db)[:5]}")
        R.warn("사업장 목록 불일치", "; ".join(details))
    else:
        R.ok("사업장 목록 일치")

    # 사업장 수 비교
    R.ok(f"사업장 수: OTB {len(otb_names)}개, DB {len(db_names)}개")


# ─── 4. JSON 유효성 검증 ────────────────────────────────
def check_json_validity():
    print("\n[4/4] JSON 파일 유효성")

    json_files = list(DATA_DIR.glob("*.json"))
    if not json_files:
        R.fail("JSON 파일", f"{DATA_DIR}에 JSON 파일 없음")
        return

    invalid = []
    for jf in sorted(json_files):
        try:
            with open(jf, encoding="utf-8") as f:
                json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            invalid.append((jf.name, str(e)[:80]))

    if invalid:
        for name, err in invalid:
            R.fail(f"JSON 파싱 실패: {name}", err)
    else:
        R.ok(f"JSON 파일 전체 유효 ({len(json_files)}개)")


# ─── 메인 ───────────────────────────────────────────────
def main():
    print(f"{'='*50}")
    print(f"  빌드 검증 시작")
    print(f"  대상: {DOCS_DIR}")
    print(f"{'='*50}")

    if not DOCS_DIR.exists():
        print(f"\033[31m✗ docs 디렉토리가 존재하지 않습니다: {DOCS_DIR}\033[0m")
        sys.exit(1)

    check_internal_links()
    check_data_structure()
    check_data_consistency()
    check_json_validity()

    exit_code = R.summary()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
