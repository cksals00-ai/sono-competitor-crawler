#!/usr/bin/env python3
"""
팔라티움 해운대 | 매출 성과 대시보드 빌드 (pbix 26_Palatium_dashboard 이식판).

parse_palatium 의 확장 팩트(rows: 요금타입/거래처/객실타입/국적/경로/투숙일·취소일 풀날짜/
패키지 등) → docs/palatium.html 템플릿의 `const DATA = ...` 에 인라인 주입 →
  · docs/palatium.html         : 내부용(auth + GSN 네비 포함)
  · docs/palatium-client.html  : 무로그인 공개판(마커 영역 GSN/auth 제거)

데몬(refresh_dashboards.py)은 인자 없이 호출 → 직전에 parse_palatium 이 써 둔
data/palatium_data.json(확장 rows) 캐시를 로드해 재빌드한다.

사용법:
  python3 scripts/build_palatium.py [예약raw_dir] [사업계획_xlsx]
    - raw_dir 생략 시 data/palatium_data.json 캐시 → 없으면 data/ 파싱
    - 사업계획 생략 시 raw_dir → data/ → data/palatium_db 순 탐색(NFD/NFC 폴백)
"""
import json, os, re, sys
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, SCRIPT_DIR)
from parse_palatium import parse, _load_business_plan, _build_targets, _find_business_plan, _load_room_plan

TEMPLATE  = os.path.join(PROJECT_DIR, "docs", "palatium.html")
CLIENT    = os.path.join(PROJECT_DIR, "docs", "palatium-client.html")
JSON_OUT  = os.path.join(PROJECT_DIR, "data", "palatium_data.json")


def _find_biz_plan(*dirs):
    """사업계획 xlsx 탐색 — glob NFD/NFC 불일치 대비 os.walk 폴백 포함."""
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


def _find_room_plan(*dirs):
    """(심사) 객실계획 초안 xlsx 탐색 (재귀). 임시(~$) 제외."""
    for d in dirs:
        if not d or not os.path.isdir(d):
            continue
        for root, _, files in os.walk(d):
            for fn in files:
                if "객실계획" in fn and fn.lower().endswith(".xlsx") and not fn.startswith("~$"):
                    return os.path.join(root, fn)
    return None


def _apply_plan(data, raw_dir, plan_arg):
    # 1순위: (심사) 객실계획 초안 — 월별 Grand Total(Budget) 기준 목표
    avail = data.get("avail_by_month", {}) or {}
    ann_avail = sum(float(v) for v in avail.values()) if avail else 0
    rp = _find_room_plan(raw_dir, os.path.join(PROJECT_DIR, "data"),
                         os.path.join(PROJECT_DIR, "data", "palatium_db"))
    if rp:
        try:
            plan = _load_room_plan(rp)
            a, mm = plan["annual"], plan["monthly"]
            ann_rn = int(round(float(a["rn"])))
            ann_rev = round(float(a["rev"]) * 1000)          # 천원 → 원
            data["targets"] = {
                "revenue": ann_rev,
                "rn": ann_rn,
                "adr": int(round(float(a["adr"]) * 1000)),
                "occ": round(ann_rn / ann_avail, 4) if ann_avail else 0,
                "revpar": round(ann_rev / ann_avail) if ann_avail else 0,
            }
            data["monthly_targets"] = {
                str(m): {"rev": round(float(mm[m]["rev"]) * 1000),
                          "rn": int(round(float(mm[m]["rn"])))}
                for m in range(1, 13)
            }
            data["monthly_rev_target"] = round(ann_rev / 12)
            data["business_plan_source"] = plan["source"]
            print(f"  ✓ 객실계획(초안) 목표 적용: {plan['source']} (연 {ann_rev/1e8:.1f}억/RN {ann_rn:,})")
            return
        except Exception as e:
            print(f"  ⚠ 객실계획 로드 실패({e}) — 사업계획 폴백")

    # 2순위(폴백): 기존 사업계획
    plan_path = plan_arg or _find_biz_plan(
        raw_dir, os.path.join(PROJECT_DIR, "data"),
        os.path.join(PROJECT_DIR, "data", "palatium_db"))
    if plan_path and os.path.exists(plan_path):
        try:
            tgt, mtgt, src = _build_targets(_load_business_plan(plan_path))
            data["targets"] = tgt
            data["monthly_targets"] = mtgt
            data["monthly_rev_target"] = round(tgt["revenue"] / 12)
            data["business_plan_source"] = src
            print(f"  ✓ 사업계획 목표 적용: {src}")
            return
        except Exception as e:
            print(f"  ⚠ 사업계획 로드 실패({e}) — 기존 targets 유지")
    else:
        print("  ⚠ 사업계획 미발견 — 기존 targets 유지")


def _strip_markers(html):
    """무로그인 공개판 — 마커 영역(GSN 네비/CSS/auth 스크립트) 제거."""
    html = re.sub(r"<!--__GSN__-->.*?<!--__GSN_END__-->", "", html, flags=re.DOTALL)
    html = re.sub(r"<!--__AUTH__-->.*?<!--__AUTH_END__-->", "", html, flags=re.DOTALL)
    html = re.sub(r"/\*__GSN_CSS__\*/.*?/\*__GSN_CSS_END__\*/", "", html, flags=re.DOTALL)
    return html


def build():
    raw_dir  = sys.argv[1] if len(sys.argv) > 1 else None
    plan_arg = sys.argv[2] if len(sys.argv) > 2 else None

    if raw_dir:
        print(f"→ 예약 raw 파싱: {raw_dir}")
        data = parse(raw_dir)
    elif os.path.exists(JSON_OUT):
        print(f"→ 캐시 로드: {JSON_OUT}")
        with open(JSON_OUT, encoding="utf-8") as f:
            data = json.load(f)
    else:
        print(f"→ data/ 파싱(폴백)")
        data = parse(os.path.join(PROJECT_DIR, "data"))

    _apply_plan(data, raw_dir, plan_arg)

    # 캐시 갱신 (다음 무인자 빌드가 이어받음). 페이지는 데이터 인라인이라 docs/data 사본 불필요.
    with open(JSON_OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

    rows = data["rows"]
    base = [r for r in rows if r.get("v")]
    rev = sum(r["r"] for r in base); rn = sum(r["n"] for r in base)
    print(f"  KPI base(유효·비기타): {len(base)}행  매출 {rev:,}  RN {rn:,}  ADR {rev//rn if rn else 0:,}")
    _nr = data.get("new_rates") or []
    if _nr:
        print(f"  ⚠ 신규 미검토 요금타입 {len(_nr)}건 — 스킬 palatium-promo-review 로 표준/프로모 확정 필요:")
        for _x in _nr:
            print(f"      · {_x}")

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

    with open(CLIENT, "w", encoding="utf-8") as f:
        f.write(_strip_markers(html))
    print(f"  ✓ HTML 빌드(무로그인·공개): {CLIENT}")


if __name__ == "__main__":
    build()
