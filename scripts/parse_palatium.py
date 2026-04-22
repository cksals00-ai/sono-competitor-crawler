#!/usr/bin/env python3
"""
팔라티움 해운대 Excel 파서
- 다올 RAW Data 기반 (1.1 VAT 이미 제외 완료)
- 시트명 무관하게 첫 번째 유효 시트 자동 감지
- 세그먼트/채널 분류 후 JSON 출력
"""

import json
import sys
import glob
import os
from datetime import datetime

import openpyxl
import pandas as pd

# ── 상수 ──────────────────────────────────────────────────────
TOTAL_ROOMS = 57
TARGETS = {
    "revenue":  4_000_000_000,
    "rn":       13_200,
    "adr":      300_000,
    "occ":      0.80,
    "revpar":   222_000,
}

VALID_STATUSES = {"Checked Out", "Reservation", "In House", "Assigned Room", "Holding Check Out"}
OVERSEAS_OTA   = {"아고다", "익스피디아", "트립닷컴", "부킹닷컴"}
DOMESTIC_OTA   = {"놀유니버스", "여기어때", "타이드스퀘어투어비스", "웹투어", "트립비토즈", "트립토파즈", "호텔패스"}
EXCLUDED_RATES = {"회원COMP", "Complimentary", "House Use", "팔라티움(임직원)", "소노(임직원)"}


def find_excel(data_dir: str = "data") -> str:
    patterns = [
        os.path.join(data_dir, "*p_data*.xlsx"),
        os.path.join(data_dir, "*palatium*.xlsx"),
        os.path.join(data_dir, "*.xlsx"),
    ]
    for p in patterns:
        files = sorted(glob.glob(p), key=os.path.getmtime, reverse=True)
        if files:
            return files[0]
    raise FileNotFoundError(f"{data_dir}/ 에서 팔라티움 Excel 파일을 찾을 수 없습니다.")


def load_dataframe(path: str) -> pd.DataFrame:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    # 첫 번째 시트(또는 데이터가 있는 시트) 자동 선택
    ws = wb.active
    for sheet in wb.worksheets:
        if sheet.max_row and sheet.max_row > 1:
            ws = sheet
            break

    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if not rows:
        raise ValueError("Excel에 데이터가 없습니다.")

    headers = [str(h).strip() if h is not None else f"col_{i}" for i, h in enumerate(rows[0])]
    df = pd.DataFrame(rows[1:], columns=headers)
    return df


def classify_segment(rate_type: str, vendor: str) -> str:
    if "소노회원" in rate_type or "D-멤버스" in rate_type:
        return "소노투웜"
    if rate_type == "FIT":
        if any(v in vendor for v in OVERSEAS_OTA):
            return "FIT(해외OTA)"
        if any(v in vendor for v in DOMESTIC_OTA):
            return "FIT(국내OTA)"
        return "FIT(기타)"
    if rate_type in {"팔라티움(분양회원)", "Direct Call", "Walk-In", "Rack Rate"}:
        return "다이렉트"
    if rate_type in EXCLUDED_RATES:
        return "기타"
    return "기타"


def parse(data_dir: str = "data") -> dict:
    path = find_excel(data_dir)
    df = load_dataframe(path)

    # ── 타입 변환 ─────────────────────────────────────────────
    df["도착일자"]  = pd.to_datetime(df["도착일자"],  errors="coerce")
    df["출발일자"]  = pd.to_datetime(df["출발일자"],  errors="coerce")
    df["등록일시"]  = pd.to_datetime(df["등록일시"],  errors="coerce")
    df["취소일자"]  = pd.to_datetime(df["취소일자"],  errors="coerce")
    df["박수"]      = pd.to_numeric(df["박수"],      errors="coerce").fillna(0).astype(int)
    df["객실수"]    = pd.to_numeric(df["객실수"],    errors="coerce").fillna(1).clip(lower=1).astype(int)
    df["총합계"]    = pd.to_numeric(df["총합계"],    errors="coerce").fillna(0)
    df["객실료"]    = pd.to_numeric(df["객실료"],    errors="coerce").fillna(0)

    df["요금타입"]  = df["요금타입"].fillna("").astype(str).str.strip()
    df["거래처"]    = df["거래처"].fillna("").astype(str).str.strip()
    df["상태"]      = df["상태"].fillna("").astype(str).str.strip()

    # ── 파생 컬럼 ────────────────────────────────────────────
    df["세그먼트"]  = df.apply(lambda r: classify_segment(r["요금타입"], r["거래처"]), axis=1)
    df["RN"]        = df["박수"] * df["객실수"].clip(lower=1)
    df["is_valid"]  = df["상태"].isin(VALID_STATUSES)
    df["is_cancel"] = df["상태"] == "Cancelled Reservation"
    df["월"]        = df["도착일자"].dt.month
    df["년월"]      = df["도착일자"].dt.to_period("M").astype(str)

    # 리드타임
    df["리드타임"] = (df["도착일자"] - df["등록일시"].dt.normalize()).dt.days.clip(lower=0)

    valid = df[df["is_valid"] & (df["세그먼트"] != "기타")].copy()

    # ── KPI ──────────────────────────────────────────────────
    total_revenue = int(valid["총합계"].sum())
    total_rn      = int(valid["RN"].sum())

    # OCC = RN / (날짜 범위 내 영업일 × 57)
    date_min = valid["도착일자"].min()
    date_max = valid["도착일자"].max()
    if pd.notna(date_min) and pd.notna(date_max):
        days_range = max((date_max - date_min).days + 1, 1)
    else:
        days_range = 365
    avail_rn  = days_range * TOTAL_ROOMS
    occ       = total_rn / avail_rn if avail_rn else 0
    adr       = total_revenue / total_rn if total_rn else 0
    revpar    = total_revenue / avail_rn if avail_rn else 0

    # 취소율
    total_bookings    = len(df[~df["is_cancel"]]) + len(df[df["is_cancel"]])
    cancelled_count   = int(df["is_cancel"].sum())
    cancel_rate       = cancelled_count / total_bookings if total_bookings else 0

    # ── 세그먼트별 ────────────────────────────────────────────
    seg_order = ["소노투웜", "FIT(해외OTA)", "FIT(국내OTA)", "FIT(기타)", "다이렉트"]
    seg_colors = {
        "소노투웜":    "#58a6ff",
        "FIT(해외OTA)": "#e3b341",
        "FIT(국내OTA)": "#3fb950",
        "FIT(기타)":   "#79c0ff",
        "다이렉트":    "#f0883e",
    }
    seg_agg = valid.groupby("세그먼트").agg(
        예약건수=("예약번호", "count"),
        매출=("총합계", "sum"),
        RN=("RN", "sum"),
    ).reindex(seg_order, fill_value=0)
    segments = []
    for seg in seg_order:
        r = seg_agg.loc[seg]
        rn_val = int(r["RN"])
        rev_val = int(r["매출"])
        adr_val = rev_val // rn_val if rn_val else 0
        segments.append({
            "name":    seg,
            "color":   seg_colors.get(seg, "#7d8590"),
            "revenue": rev_val,
            "rn":      rn_val,
            "bookings": int(r["예약건수"]),
            "adr":     adr_val,
            "share":   round(rev_val / total_revenue * 100, 1) if total_revenue else 0,
        })

    # ── 월별 트렌드 ───────────────────────────────────────────
    month_names = {1:"1월",2:"2월",3:"3월",4:"4월",5:"5월",6:"6월",
                   7:"7월",8:"8월",9:"9월",10:"10월",11:"11월",12:"12월"}
    monthly_agg = valid.groupby("월").agg(매출=("총합계","sum"), RN=("RN","sum")).reset_index()
    monthly = []
    for _, row in monthly_agg.iterrows():
        m = int(row["월"])
        rn_val  = int(row["RN"])
        rev_val = int(row["매출"])
        # 해당 월의 영업일 * 57
        import calendar
        year = date_min.year if pd.notna(date_min) else 2026
        days_in_month = calendar.monthrange(year, m)[1]
        avail_m = days_in_month * TOTAL_ROOMS
        occ_m   = round(rn_val / avail_m * 100, 1) if avail_m else 0
        adr_m   = rev_val // rn_val if rn_val else 0
        monthly.append({
            "month":   m,
            "label":   month_names[m],
            "revenue": rev_val,
            "rn":      rn_val,
            "occ":     occ_m,
            "adr":     adr_m,
            "avail_rn": avail_m,
        })

    # ── 거래처(채널)별 ────────────────────────────────────────
    channel_agg = valid.groupby("거래처").agg(
        매출=("총합계","sum"), RN=("RN","sum"), 예약건수=("예약번호","count")
    ).sort_values("매출", ascending=False)

    # 취소 포함 전체에서 거래처별 취소율
    cancel_by_vendor = df[df["is_cancel"]].groupby("거래처").size().rename("취소")
    total_by_vendor  = df.groupby("거래처").size().rename("전체")
    vendor_cancel    = pd.concat([total_by_vendor, cancel_by_vendor], axis=1).fillna(0)
    vendor_cancel["취소율"] = vendor_cancel["취소"] / vendor_cancel["전체"]

    channels = []
    for vendor, row in channel_agg.iterrows():
        rn_val  = int(row["RN"])
        rev_val = int(row["매출"])
        cancel_r = float(vendor_cancel.loc[vendor, "취소율"]) if vendor in vendor_cancel.index else 0
        channels.append({
            "name":       vendor,
            "revenue":    rev_val,
            "rn":         rn_val,
            "bookings":   int(row["예약건수"]),
            "adr":        rev_val // rn_val if rn_val else 0,
            "cancel_rate": round(cancel_r * 100, 1),
        })

    # ── 객실타입별 ────────────────────────────────────────────
    rt_agg = valid.groupby("객실타입").agg(매출=("총합계","sum"), RN=("RN","sum")).sort_values("매출", ascending=False)
    room_types = []
    for rt, row in rt_agg.iterrows():
        rn_val  = int(row["RN"])
        rev_val = int(row["매출"])
        room_types.append({
            "name":    rt,
            "revenue": rev_val,
            "rn":      rn_val,
            "adr":     rev_val // rn_val if rn_val else 0,
            "share":   round(rev_val / total_revenue * 100, 1) if total_revenue else 0,
        })

    # ── 리드타임 분포 ─────────────────────────────────────────
    bins  = [0, 7, 14, 30, 60, 90, 999]
    labels = ["당일~7일", "8~14일", "15~30일", "31~60일", "61~90일", "91일+"]
    valid2 = valid[valid["리드타임"].notna()].copy()
    valid2["리드타임_구간"] = pd.cut(valid2["리드타임"], bins=bins, labels=labels, right=True)
    lead_agg = valid2.groupby("리드타임_구간", observed=True)["예약번호"].count()
    lead_time = [{"label": l, "count": int(lead_agg.get(l, 0))} for l in labels]
    avg_lead = round(valid2["리드타임"].mean(), 1) if len(valid2) else 0

    # ── 신규 vs 재방문 (이름 기준 단순 추정) ──────────────────
    guest_counts = valid.groupby("투숙객명")["예약번호"].count()
    repeat = int((guest_counts > 1).sum())
    new_   = int((guest_counts == 1).sum())

    result = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "source_file":  os.path.basename(path),
        "date_range": {
            "min": date_min.strftime("%Y-%m-%d") if pd.notna(date_min) else "",
            "max": date_max.strftime("%Y-%m-%d") if pd.notna(date_max) else "",
        },
        "targets": TARGETS,
        "kpi": {
            "revenue":      total_revenue,
            "rn":           total_rn,
            "adr":          round(adr),
            "occ":          round(occ * 100, 1),
            "revpar":       round(revpar),
            "avail_rn":     avail_rn,
            "days_range":   days_range,
            "cancel_rate":  round(cancel_rate * 100, 1),
            "avg_lead_time": avg_lead,
            "cancelled_count": cancelled_count,
            "total_bookings":  total_bookings,
            "new_guests":      new_,
            "repeat_guests":   repeat,
        },
        "segments":   segments,
        "monthly":    monthly,
        "channels":   channels,
        "room_types": room_types,
        "lead_time":  lead_time,
    }
    return result


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "data"
    out_path  = sys.argv[2] if len(sys.argv) > 2 else "data/palatium_data.json"
    result = parse(data_dir)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"✓ 파싱 완료 → {out_path}")
    kpi = result["kpi"]
    print(f"  매출: {kpi['revenue']:,}원 / 목표 {result['targets']['revenue']:,}원 ({kpi['revenue']/result['targets']['revenue']*100:.1f}%)")
    print(f"  RN:   {kpi['rn']:,} / 목표 {result['targets']['rn']:,} ({kpi['rn']/result['targets']['rn']*100:.1f}%)")
    print(f"  ADR:  {kpi['adr']:,}원 | OCC: {kpi['occ']}% | RevPAR: {kpi['revpar']:,}원")
