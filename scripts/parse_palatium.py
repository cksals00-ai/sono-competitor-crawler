#!/usr/bin/env python3
"""
팔라티움 해운대 Excel 파서 — BI Python 스크립트(01_PowerBI_데이터가져오기.py)와 동일 로직
- classify_segment / get_channel_name / classify_fit_channel 함수 동일
- rows 배열 출력 → HTML 클라이언트 사이드 필터링
- VAT 1.1 이미 제외 완료 상태 그대로 사용
"""
import json, sys, glob, os, calendar
from datetime import datetime
import openpyxl
import pandas as pd

TOTAL_ROOMS = 57
YEAR = 2026
TARGETS = {
    "revenue": 4_000_000_000,
    "rn":      13_200,
    "adr":     300_000,
    "occ":     0.80,
    "revpar":  222_000,
}
VALID_STATUSES = {"Checked Out","Reservation","In House","Assigned Room","Holding Check Out"}
OVERSEAS_OTA = {"아고다","익스피디아","트립닷컴","부킹닷컴"}
DOMESTIC_OTA = {"놀유니버스","여기어때","타이드스퀘어투어비스","웹투어"}


def find_excel(data_dir):
    """Excel 파일 탐색 — 여러 파일이면 전부 반환 (리스트)"""
    for pat in [f"{data_dir}/*p_data*.xlsx", f"{data_dir}/*palatium*.xlsx",
                f"{data_dir}/*예약정보조회*.xlsx", f"{data_dir}/*.xlsx"]:
        hits = sorted(glob.glob(pat), key=os.path.getmtime, reverse=True)
        if hits:
            return hits  # 리스트 반환
    raise FileNotFoundError(f"{data_dir}/ 에서 팔라티움 Excel 없음")


def _load_single(path):
    """단일 xlsx → DataFrame"""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    for s in wb.worksheets:
        if s.max_row and s.max_row > 1:
            ws = s
            break
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    # 타이틀/날짜 행이 앞에 붙은 경우 실제 헤더 행 탐지
    header_idx = 0
    for i, row in enumerate(rows[:5]):
        if "도착일자" in row:
            header_idx = i
            break
    headers = [str(h).strip() if h else f"col_{i}" for i, h in enumerate(rows[header_idx])]
    return pd.DataFrame(rows[header_idx + 1:], columns=headers)


def load_df(path):
    """단일 파일 또는 여러 파일 합산 로드"""
    if isinstance(path, list):
        dfs = []
        for p in path:
            df = _load_single(p)
            dfs.append(df)
            print(f"  로드: {os.path.basename(p)} ({len(df)}행)")
        combined = pd.concat(dfs, ignore_index=True)
        # 예약번호 기준 중복 제거
        before = len(combined)
        if "예약번호" in combined.columns:
            combined = combined.drop_duplicates(subset=["예약번호"], keep="last")
        print(f"  합산: {before}행 → {len(combined)}행 (중복 {before - len(combined)}건 제거)")
        return combined
    return _load_single(path)


# ── BI 로직 (01_PowerBI_데이터가져오기.py와 동일) ─────────────────────────
def classify_segment(rt: str) -> str:
    if "소노회원" in rt:
        return "소노회원"
    if "D-멤버스" in rt:
        return "D-멤버스"
    if rt == "FIT":
        return "FIT(OTA)"
    if any(k in rt for k in ["팔라티움", "Direct Call", "Walk-In", "Rack Rate"]):
        return "홈페이지(다이렉트)"
    return "기타"


def classify_fit_channel(seg: str, vendor: str) -> str | None:
    if seg != "FIT(OTA)":
        return None
    if vendor in OVERSEAS_OTA:
        return "FIT-해외OTA"
    if vendor in DOMESTIC_OTA:
        return "FIT-국내OTA"
    return "FIT-기타"


def get_channel_name(seg: str, rt: str, vendor: str) -> str:
    if seg in ("소노회원", "D-멤버스"):
        return seg
    if seg == "FIT(OTA)":
        return vendor if vendor else "기타"
    if seg == "홈페이지(다이렉트)":
        if "Direct Call" in rt:
            return "전화예약"
        if "Walk-In" in rt:
            return "워크인"
        return "팔라티움자체"
    return "기타"


def classify_room(rt: str) -> str:
    for kw, cat in [("Superior","슈페리어"),("Deluxe","디럭스"),("Premier","프리미어"),
                    ("Prestige","프레스티지"),("Presidential","프레지덴셜")]:
        if kw in rt:
            return cat
    return "기타"


def classify_view(rt: str) -> str:
    if "Ocean" in rt:
        return "오션뷰"
    if "VF" in rt:
        return "밸리/포레스트뷰"
    return "기타"


# ─────────────────────────────────────────────────────────────────────────────
def parse(data_dir: str = "data") -> dict:
    paths = find_excel(data_dir)
    df = load_df(paths)
    path = paths[0] if isinstance(paths, list) else paths

    # 타입 변환
    for col in ["도착일자","출발일자","등록일시","취소일자"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df["박수"]   = pd.to_numeric(df["박수"],   errors="coerce").fillna(0).astype(int)
    df["객실수"] = pd.to_numeric(df["객실수"], errors="coerce").fillna(1).clip(lower=1).astype(int)
    df["총합계"] = pd.to_numeric(df["총합계"], errors="coerce").fillna(0)
    for col in ["요금타입","거래처","상태","투숙객명","객실타입"]:
        df[col] = df[col].fillna("").astype(str).str.strip()

    # 파생 컬럼
    df["세그먼트"]     = df["요금타입"].apply(classify_segment)
    df["FIT채널구분"]  = df.apply(lambda r: classify_fit_channel(r["세그먼트"], r["거래처"]), axis=1)
    df["채널명"]       = df.apply(lambda r: get_channel_name(r["세그먼트"], r["요금타입"], r["거래처"]), axis=1)
    df["세그먼트상세"] = df.apply(lambda r: r["FIT채널구분"] if r["FIT채널구분"] else r["세그먼트"], axis=1)
    df["객실대분류"]   = df["객실타입"].apply(classify_room)
    df["뷰타입"]       = df["객실타입"].apply(classify_view)
    df["RN"]           = df["박수"] * df["객실수"].clip(lower=1)
    df["is_valid"]     = df["상태"].isin(VALID_STATUSES)
    df["is_cancel"]    = df["상태"] == "Cancelled Reservation"
    df["도착월"]       = df["도착일자"].dt.month
    df["도착일"]       = df["도착일자"].dt.day
    df["예약월"]       = df["등록일시"].dt.month
    df["예약일자"]     = df["등록일시"].dt.date.apply(lambda x: x.isoformat() if pd.notna(x) else None)
    df["리드타임"]     = (df["도착일자"] - df["등록일시"].dt.normalize()).dt.days
    df.loc[df["리드타임"] < 0, "리드타임"] = None

    # 재방문 판별 (유효예약 + 비기타 게스트 기준)
    guest_cnts = (df[df["is_valid"] & (df["세그먼트"] != "기타")]
                  .groupby("투숙객명")["예약번호"].count())
    repeat_set = set(guest_cnts[guest_cnts > 1].index)
    df["재방문"] = df["투숙객명"].apply(lambda g: 1 if g in repeat_set else 0)

    # 월별 가용 객실수 (57실 × 해당 월 일수)
    avail_by_month = {str(m): calendar.monthrange(YEAR, m)[1] * TOTAL_ROOMS
                      for m in range(1, 13)}

    # Slim rows 배열 (클라이언트 필터링용)
    rows_out = []
    for _, r in df.iterrows():
        rows_out.append({
            "m":   int(r["도착월"])  if pd.notna(r["도착월"])  else None,
            "d":   int(r["도착일"])  if pd.notna(r["도착일"])  else None,
            "bm":  int(r["예약월"])  if pd.notna(r["예약월"])  else None,
            "r":   int(r["총합계"]),
            "n":   int(r["RN"]),
            "seg": r["세그먼트"],
            "fit": r["FIT채널구분"],
            "ch":  r["채널명"],
            "v":   int(r["is_valid"]),
            "k":   int(r["is_cancel"]),
            "bd":  r["예약일자"] if pd.notna(r["예약일자"]) else None,
            "lead":int(r["리드타임"]) if pd.notna(r["리드타임"]) else None,
            "rt":  r["객실대분류"],
            "vw":  r["뷰타입"],
            "rep": int(r["재방문"]),
        })

    bd_series = df["등록일시"].dropna()
    return {
        "generated_at":    datetime.now().strftime("%Y-%m-%d %H:%M"),
        "source_file":     os.path.basename(path),
        "date_range": {
            "min": df["도착일자"].min().strftime("%Y-%m-%d") if pd.notna(df["도착일자"].min()) else "",
            "max": df["도착일자"].max().strftime("%Y-%m-%d") if pd.notna(df["도착일자"].max()) else "",
        },
        "book_date_range": {
            "min": bd_series.min().strftime("%Y-%m-%d") if len(bd_series) else "",
            "max": bd_series.max().strftime("%Y-%m-%d") if len(bd_series) else "",
        },
        "targets":        TARGETS,
        "monthly_rev_target": round(TARGETS["revenue"] / 12),
        "avail_by_month": avail_by_month,
        "rows":           rows_out,
    }


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "data"
    out_path  = sys.argv[2] if len(sys.argv) > 2 else "data/palatium_data.json"
    result = parse(data_dir)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, separators=(",", ":"))
    rows  = result["rows"]
    valid = [r for r in rows if r["v"]]
    rev   = sum(r["r"] for r in valid)
    rn    = sum(r["n"] for r in valid)
    print(f"✓ {out_path}  ({len(rows)}행)")
    print(f"  매출: {rev:,}원  RN: {rn:,}  ADR: {rev//rn if rn else 0:,}")
    print(f"  세그먼트 분포: {{}}")
    from collections import Counter
    for seg, cnt in Counter(r['seg'] for r in valid).most_common():
        print(f"    {seg}: {cnt}건")
