#!/usr/bin/env python3
"""
팔라티움 대시보드 빌드 스크립트
parse_palatium.py → JSON 생성 → HTML 템플릿에 인라인 삽입 → docs/palatium.html 출력
"""

import json
import sys
import os
import re

# 현재 스크립트 디렉토리 기준으로 프로젝트 루트 탐색
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

sys.path.insert(0, SCRIPT_DIR)
from parse_palatium import parse

TEMPLATE = os.path.join(PROJECT_DIR, "docs", "palatium.html")
OUTPUT   = os.path.join(PROJECT_DIR, "docs", "palatium.html")
DATA_DIR = os.path.join(PROJECT_DIR, "data")
JSON_OUT = os.path.join(DATA_DIR, "palatium_data.json")


def build():
    # 1. Excel 파싱
    print("→ Excel 파싱 중...")
    data = parse(DATA_DIR)

    # JSON 캐시 저장
    with open(JSON_OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  ✓ JSON 저장: {JSON_OUT}")

    kpi = data["kpi"]
    tgt = data["targets"]
    print(f"  매출: {kpi['revenue']:,}원  ({kpi['revenue']/tgt['revenue']*100:.1f}%)")
    print(f"  RN:   {kpi['rn']:,}박      ({kpi['rn']/tgt['rn']*100:.1f}%)")
    print(f"  ADR:  {kpi['adr']:,}원  OCC: {kpi['occ']}%  RevPAR: {kpi['revpar']:,}원")

    # 2. HTML 템플릿 로드
    with open(TEMPLATE, "r", encoding="utf-8") as f:
        html = f.read()

    # 3. 플레이스홀더 교체
    json_str = json.dumps(data, ensure_ascii=False)
    if "__PALATIUM_DATA__" not in html:
        print("  ⚠ 플레이스홀더 __PALATIUM_DATA__ 없음 — 이미 빌드된 파일에 재삽입")
        # 기존 DATA = {...} 라인을 교체
        html = re.sub(
            r'const DATA = \{.*?\};',
            f'const DATA = {json_str};',
            html, count=1, flags=re.DOTALL
        )
    else:
        html = html.replace("__PALATIUM_DATA__", json_str)

    # 4. 출력
    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  ✓ HTML 빌드 완료: {OUTPUT}")


if __name__ == "__main__":
    build()
