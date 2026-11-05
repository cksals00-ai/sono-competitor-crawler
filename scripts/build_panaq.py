#!/usr/bin/env python3
"""
파나크 영덕 | 매출 성과 대시보드 빌드 (pbix 26_Palatium_dashboard 이식판).

parse_palatium 의 확장 팩트(rows: 요금타입/거래처/객실타입/국적/경로/투숙일·취소일 풀날짜/
패키지 등) → docs/panaq.html 템플릿의 `const DATA = ...` 에 인라인 주입 →
  · docs/panaq.html         : 내부용(auth + GSN 네비 포함)
  · docs/panaq-client.html  : 무로그인 공개판(마커 영역 GSN/auth 제거)

데몬(refresh_dashboards.py)은 인자 없이 호출 → 직전에 parse_palatium 이 써 둔
data/panaq_data.json(확장 rows) 캐시를 로드해 재빌드한다.

사용법:
  python3 scripts/build_palatium.py [예약raw_dir] [사업계획_xlsx]
    - raw_dir 생략 시 data/panaq_data.json 캐시 → 없으면 data/ 파싱
    - 사업계획 생략 시 raw_dir → data/ → data/palatium_db 순 탐색(NFD/NFC 폴백)
"""
import json, os, re, sys
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, SCRIPT_DIR)
from parse_panaq import parse, _load_business_plan, _build_targets, _find_business_plan, _load_room_plan

TEMPLATE  = os.path.join(PROJECT_DIR, "docs", "panaq.html")
CLIENT    = os.path.join(PROJECT_DIR, "docs", "panaq-client.html")
JSON_OUT  = os.path.join(PROJECT_DIR, "data", "panaq_data.json")      # 로컬 캐시(비커밋)
DOCS_DATA = os.path.join(PROJECT_DIR, "docs", "data")
GZ_OUT    = os.path.join(DOCS_DATA, "panaq_data.json.gz")             # 배포(페이지 fetch 대상)


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


# GS 사업계획 워크북의 '파나크 영덕' 시트에서 목표 로드 (기존 대시보드가 쓰던 그 사업계획).
#   포맷: Grand Total 월행[26,50,…,290], RN=col6(idx5)·ADR=col8(idx7,천원)·REV=col10(idx9,천원).
PANAQ_BUDGET_CANDIDATES = [
    os.path.join(PROJECT_DIR, "data", "panaq_db"),  # 사용자가 panaq_db에 복사 시 우선
    "/Users/chanminpark/Projects/gs_daily_trend_news_public_temp/data/raw_db/budget",
]
_GT_ROWS = [26, 50, 74, 98, 122, 146, 170, 194, 218, 242, 266, 290]


def _find_gs_budget():
    import glob
    for d in PANAQ_BUDGET_CANDIDATES:
        if not os.path.isdir(d):
            continue
        for f in sorted(glob.glob(os.path.join(d, "**", "*사업계획*.xlsx"), recursive=True) +
                        glob.glob(os.path.join(d, "*사업계획*.xlsx")), reverse=True):
            if os.path.basename(f).startswith("~$"):
                continue
            try:
                import openpyxl
                wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
                if "파나크 영덕" in wb.sheetnames:
                    wb.close(); return f
                wb.close()
            except Exception:
                continue
    return None


def load_panaq_targets(data):
    """GS 사업계획 '파나크 영덕' 시트 → targets/monthly_targets. 못 찾으면 None."""
    import openpyxl
    path = _find_gs_budget()
    if not path:
        return None
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    if "파나크 영덕" not in wb.sheetnames:
        wb.close(); return None
    rows = list(wb["파나크 영덕"].iter_rows(values_only=True))
    wb.close()
    def _f(v):
        try: return float(v or 0)
        except (TypeError, ValueError): return 0.0
    m_rn, m_rev = [], []
    for ri in _GT_ROWS:
        r = rows[ri - 1] if ri - 1 < len(rows) else ()
        m_rn.append(_f(r[5]) if len(r) > 5 else 0.0)
        m_rev.append(_f(r[9]) if len(r) > 9 else 0.0)   # 千원
    ann_rn = int(round(sum(m_rn)))
    ann_rev = round(sum(m_rev) * 1000)                   # 원
    if ann_rn <= 0 or ann_rev <= 0:
        return None
    avail = data.get("avail_by_month", {}) or {}
    ann_avail = sum(float(v) for v in avail.values()) if avail else 0
    tgt = {
        "revenue": ann_rev,
        "rn":      ann_rn,
        "adr":     int(round(ann_rev / ann_rn)) if ann_rn else 0,
        "occ":     round(ann_rn / ann_avail, 4) if ann_avail else 0,
        "revpar":  round(ann_rev / ann_avail) if ann_avail else 0,
    }
    mtgt = {str(i + 1): {"rev": round(m_rev[i] * 1000), "rn": int(round(m_rn[i]))}
            for i in range(12)}
    return tgt, mtgt, f"{os.path.basename(path)} · 파나크 영덕"


def _apply_plan(data, raw_dir, plan_arg):
    # 0순위: GS 사업계획 '파나크 영덕' 시트 (기존 대시보드가 쓰던 사업계획)
    try:
        res = load_panaq_targets(data)
        if res:
            tgt, mtgt, src = res
            data["targets"] = tgt
            data["monthly_targets"] = mtgt
            data["monthly_rev_target"] = round(tgt["revenue"] / 12)
            data["business_plan_source"] = src
            print(f"  ✓ 파나크 목표 적용: {src} (연 {tgt['revenue']/1e8:.1f}억/RN {tgt['rn']:,}/OCC {tgt['occ']*100:.0f}%)")
            return
    except Exception as e:
        print(f"  ⚠ 파나크 사업계획 로드 실패({e}) — 팔라티움 폴백 경로 시도")
    # 1순위: (심사) 객실계획 초안 — 월별 Grand Total(Budget) 기준 목표
    avail = data.get("avail_by_month", {}) or {}
    ann_avail = sum(float(v) for v in avail.values()) if avail else 0
    # ⚠ 파나크 목표는 파나크 raw_dir(panaq_db)에서만 탐색 — competitor repo/data의
    #   팔라티움 객실계획을 잘못 집어오지 않도록 제한.
    rp = _find_room_plan(raw_dir)
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
    plan_path = plan_arg or _find_biz_plan(raw_dir)
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

    rows = data["rows"]
    base = [r for r in rows if r.get("v")]
    rev = sum(r["r"] for r in base); rn = sum(r["n"] for r in base)
    print(f"  KPI base(유효·비기타): {len(base)}행  매출 {rev:,}  RN {rn:,}  ADR {rev//rn if rn else 0:,}")
    _nr = data.get("new_rates") or []
    if _nr:
        print(f"  ⚠ 신규 미검토 요금타입 {len(_nr)}건 — 표준/프로모 확정 필요(parse_panaq STANDARD_RATES/KNOWN_PROMOS):")
        for _x in _nr:
            print(f"      · {_x}")

    # 데이터 외부화: docs/data/panaq_data.json.gz(gzip) — 페이지가 fetch+DecompressionStream으로 로드.
    #   (69MB 인라인 HTML을 repo에 매일 커밋하면 비대 → 2.5MB gz만 배포)
    import gzip
    json_bytes = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    os.makedirs(DOCS_DATA, exist_ok=True)
    with gzip.open(GZ_OUT, "wb", compresslevel=9) as f:
        f.write(json_bytes)
    print(f"  ✓ 데이터(gz): {GZ_OUT}  ({len(json_bytes)/1e6:.0f}MB → {os.path.getsize(GZ_OUT)/1e6:.1f}MB)")

    # 로컬 캐시(비커밋 — .gitignore). 무인자 재빌드가 이어받음.
    with open(JSON_OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

    # 공개판(무로그인) — 마커(GSN/auth) 제거만, 데이터는 동일 .gz fetch.
    with open(TEMPLATE, "r", encoding="utf-8") as f:
        html = f.read()
    with open(CLIENT, "w", encoding="utf-8") as f:
        f.write(_strip_markers(html))
    print(f"  ✓ HTML(내부·auth): {TEMPLATE}")
    print(f"  ✓ HTML(무로그인·공개): {CLIENT}")


if __name__ == "__main__":
    build()
