#!/usr/bin/env python3
"""
팔라티움 BI 대시보드(개요) 빌드 — pbix(26_Palatium_dashboard_0408) 이식판.

parse_palatium.parse()로 예약 raw → 확장 팩트(rows: 요금타입/거래처/객실타입/국적/경로/
투숙일·취소일 풀날짜/패키지 등) 생성 → docs/palatium-bi.html 의 `const DATA = __PALATIUM_DATA__`
플레이스홀더(또는 기존 const DATA = {...})에 인라인 주입.

사용법:
  python3 scripts/build_palatium_bi.py [예약raw_dir] [사업계획_xlsx]
  - 예약raw_dir 생략 시 data/ 에서 탐색
  - 사업계획 생략 시 raw_dir → data/ 순으로 탐색 (macOS NFD/NFC 회피 위해 명시 권장)
"""
import json, os, re, sys, glob
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, SCRIPT_DIR)
from parse_palatium import parse, _load_business_plan, _build_targets, _find_business_plan

TEMPLATE = os.path.join(PROJECT_DIR, "docs", "palatium-bi.html")
CLIENT   = os.path.join(PROJECT_DIR, "docs", "palatium-bi-client.html")
JSON_OUT = os.path.join(PROJECT_DIR, "data", "palatium_fact.json")
DOCS_JSON = os.path.join(PROJECT_DIR, "docs", "data", "palatium_fact.json")


def _find_biz_plan(*dirs):
    """사업계획 xlsx 탐색 — glob NFD/NFC 불일치 대비 os.listdir 폴백 포함."""
    for d in dirs:
        if not d or not os.path.isdir(d):
            continue
        hit = _find_business_plan(d)
        if hit:
            return hit
        for root, _, files in os.walk(d):
            for fn in files:
                if "사업계획" in fn and fn.lower().endswith(".xlsx"):
                    return os.path.join(root, fn)
    return None


def build():
    raw_dir  = sys.argv[1] if len(sys.argv) > 1 else os.path.join(PROJECT_DIR, "data")
    plan_arg = sys.argv[2] if len(sys.argv) > 2 else None

    print(f"→ 예약 raw 파싱: {raw_dir}")
    data = parse(raw_dir)

    # 사업계획 목표 명시 적용 (매출 ×1.01)
    plan_path = plan_arg or _find_biz_plan(raw_dir, os.path.join(PROJECT_DIR, "data"),
                                           os.path.join(PROJECT_DIR, "data", "palatium_db"))
    if plan_path and os.path.exists(plan_path):
        try:
            plan = _load_business_plan(plan_path)
            tgt, mtgt, src = _build_targets(plan)
            data["targets"] = tgt; data["monthly_targets"] = mtgt
            data["monthly_rev_target"] = round(tgt["revenue"] / 12)
            data["business_plan_source"] = src
            print(f"  ✓ 사업계획 목표 적용: {src}")
        except Exception as e:
            print(f"  ⚠ 사업계획 로드 실패({e}) — parse 기본 targets 유지")
    else:
        print("  ⚠ 사업계획 미발견 — parse 기본 targets 유지")

    # 팩트 JSON 저장 (참고/디버그용)
    os.makedirs(os.path.dirname(DOCS_JSON), exist_ok=True)
    for p in (JSON_OUT, DOCS_JSON):
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    print(f"  ✓ 팩트 JSON: {JSON_OUT}")

    rows = data["rows"]
    base = [r for r in rows if r["v"] and r["seg"] != "기타"]
    rev = sum(r["r"] for r in base); rn = sum(r["n"] for r in base)
    print(f"  KPI base(유효·비기타): {len(base)}행  매출 {rev:,}  RN {rn:,}  ADR {rev//rn if rn else 0:,}")

    # 템플릿 주입 (플레이스홀더 → 데이터, 재빌드 시 const DATA = {...} regex 교체)
    with open(TEMPLATE, "r", encoding="utf-8") as f:
        html = f.read()
    json_str = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    if "__PALATIUM_DATA__" in html:
        html = html.replace("__PALATIUM_DATA__", json_str)
    else:
        html = re.sub(r"const DATA = .*?;\n", f"const DATA = {json_str};\n", html, count=1, flags=re.DOTALL)
    with open(TEMPLATE, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  ✓ HTML 빌드(내부·auth): {TEMPLATE}")

    # 무로그인(공개) 버전 — 마커 영역(GSN 네비/CSS/auth 스크립트) 제거
    client = html
    client = re.sub(r"<!--__GSN__-->.*?<!--__GSN_END__-->", "", client, flags=re.DOTALL)
    client = re.sub(r"<!--__AUTH__-->.*?<!--__AUTH_END__-->", "", client, flags=re.DOTALL)
    client = re.sub(r"/\*__GSN_CSS__\*/.*?/\*__GSN_CSS_END__\*/", "", client, flags=re.DOTALL)
    with open(CLIENT, "w", encoding="utf-8") as f:
        f.write(client)
    print(f"  ✓ HTML 빌드(무로그인·공개): {CLIENT}")


if __name__ == "__main__":
    build()
