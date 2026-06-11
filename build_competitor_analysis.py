#!/usr/bin/env python3
"""
권역별 가격 분석 재생성기 — competitor_analysis.json

경쟁사 "금액" 크롤 산출물(exports/sono_competitor_prices_<날짜>.csv)을 읽어,
GS 매출 리포트(gs-sales-report.html)와 freshness 패널이 소비하는
권역별 가격 분석 JSON을 매일 재생성한다.

입력
  exports/sono_competitor_prices_<YYYYMMDD>.csv   (가격 크롤 산출물)
  config/region_map.json                          (권역 → 자사사업장 canonical 매핑)
출력 (--out 으로 여러 곳 지정, 기본은 레포 루트)
  competitor_analysis.json

소비 필드 계약 (gs-sales-report.html 기준)
  generated_at, data_period
  A_regional[권역] = own_properties, competitors, n_competitors,
                     own_avg_price, comp_avg_price, premium_discount_pct,
                     price_cv, ota_comparison{OTA:{own_avg,comp_avg,gap_pct}}
  B_competitor_profiles[권역] = [{name, avg_price, weekday_avg, weekend_avg,
                                  price_cv, vs_sono_pct}]

집계 규칙
  - 판매가>0 & 판매상태=available 행만 사용
  - 체크인 >= 크롤일(예약 가능한 미래 일자만) 행만 사용
  - 주중 = 일~목(dow 6,0,1,2,3), 주말 = 금·토(dow 4,5)
  - premium_discount_pct = (자사평균-경쟁사평균)/경쟁사평균*100  (+면 자사가 비쌈)
  - vs_sono_pct          = (해당호텔평균-자사평균)/자사평균*100
"""
import argparse
import json
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd

KST = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parent
EXPORTS = ROOT / "exports"
COLS = ["소노사업장", "경쟁사명", "OTA", "체크인", "판매가(원)", "판매상태", "is_own"]
PRICE = "판매가(원)"


def latest_csv(date_arg: str | None) -> Path:
    if date_arg:
        p = EXPORTS / f"sono_competitor_prices_{date_arg}.csv"
        if p.exists():
            return p
        print(f"[warn] {p.name} 없음 → 최신 파일 사용", file=sys.stderr)
    cands = sorted(EXPORTS.glob("sono_competitor_prices_????????.csv"))
    if not cands:
        sys.exit("[error] sono_competitor_prices CSV 없음")
    return cands[-1]


def crawl_date_of(path: Path):
    m = re.search(r"(\d{8})", path.stem)
    return datetime.strptime(m.group(1), "%Y%m%d") if m else None


def ota_label(v: str) -> str:
    """'네이버호텔/Agoda' → 'Agoda', '야놀자' → '야놀자'"""
    return str(v).split("/")[-1].strip()


def cv_pct(s: pd.Series) -> float:
    m = s.mean()
    if not m or pd.isna(m):
        return 0.0
    return round(float(s.std(ddof=0) / m * 100), 1)


def avg(s: pd.Series):
    return int(round(s.mean())) if len(s) and not pd.isna(s.mean()) else None


def gap_pct(own, comp):
    if own is None or comp is None or not comp:
        return None
    return round((own - comp) / comp * 100, 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("date", nargs="?", help="YYYYMMDD (생략 시 최신 CSV)")
    ap.add_argument("--region-map", default=str(ROOT / "config" / "region_map.json"))
    ap.add_argument("--out", action="append", default=[], help="출력 경로(여러 번 가능)")
    args = ap.parse_args()

    csv_path = latest_csv(args.date)
    crawl_dt = crawl_date_of(csv_path)
    region_map = json.load(open(args.region_map, encoding="utf-8"))
    own2region = {p: rk for rk, props in region_map.items() for p in props}

    print(f"[info] CSV  : {csv_path.name} (크롤일 {crawl_dt.date() if crawl_dt else '?'})", file=sys.stderr)
    print(f"[info] 권역 : {len(region_map)}개 / 자사 {len(own2region)}개", file=sys.stderr)

    df = pd.read_csv(csv_path, usecols=COLS, encoding="utf-8-sig", low_memory=False)
    df[PRICE] = pd.to_numeric(df[PRICE], errors="coerce")
    df["체크인_dt"] = pd.to_datetime(df["체크인"], errors="coerce")
    df = df[(df[PRICE] > 0) & (df["판매상태"] == "available") & df["체크인_dt"].notna()].copy()
    if crawl_dt is not None:
        df = df[df["체크인_dt"] >= crawl_dt]              # 예약 가능한 미래 일자만
    df["is_own"] = df["is_own"].astype(str).str.lower().isin(["true", "1"])
    df["ota"] = df["OTA"].map(ota_label)
    df["region"] = df["소노사업장"].map(own2region)
    df["dow"] = df["체크인_dt"].dt.dayofweek               # 월=0 … 금=4,토=5,일=6
    used = df[df["region"].notna()]

    A_regional, B_profiles = {}, {}
    for rk, own_props in region_map.items():
        sub = used[used["region"] == rk]
        if sub.empty:
            continue
        own = sub[sub["is_own"]]
        comp = sub[~sub["is_own"]]
        own_avg, comp_avg = avg(own[PRICE]), avg(comp[PRICE])

        ota_cmp = {}
        for ota, g in sub.groupby("ota"):
            o, c = avg(g[g["is_own"]][PRICE]), avg(g[~g["is_own"]][PRICE])
            if o is None and c is None:
                continue
            ota_cmp[ota] = {"own_avg": o, "comp_avg": c, "gap_pct": gap_pct(o, c)}

        comp_names = sorted(comp["경쟁사명"].dropna().unique().tolist())
        A_regional[rk] = {
            "own_properties": sorted(own["소노사업장"].dropna().unique().tolist()),
            "competitors": comp_names,
            "n_competitors": len(comp_names),
            "own_avg_price": own_avg,
            "comp_avg_price": comp_avg,
            "premium_discount_pct": gap_pct(own_avg, comp_avg),
            "price_cv": cv_pct(own[PRICE]) if len(own) else 0.0,
            "ota_comparison": ota_cmp,
        }

        profiles = []
        for name, g in sub.groupby("경쟁사명"):
            profiles.append({
                "name": name,
                "avg_price": avg(g[PRICE]),
                "weekday_avg": avg(g[g["dow"].isin([6, 0, 1, 2, 3])][PRICE]),  # 일~목
                "weekend_avg": avg(g[g["dow"].isin([4, 5])][PRICE]),           # 금·토
                "price_cv": cv_pct(g[PRICE]),
                "vs_sono_pct": gap_pct(avg(g[PRICE]), own_avg),
            })
        profiles.sort(key=lambda x: x["avg_price"] or 0, reverse=True)
        B_profiles[rk] = profiles

    out = {
        "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M"),
        "data_period": {
            "crawl": crawl_dt.strftime("%Y-%m-%d") if crawl_dt else "",
            "checkin": f'{used["체크인"].min()} ~ {used["체크인"].max()}',
            "total_rows": int(len(used)),
            "own_properties": int(used[used["is_own"]]["소노사업장"].nunique()),
            "competitors": int(used[~used["is_own"]]["경쟁사명"].nunique()),
            "ota_channels": int(used["ota"].nunique()),
        },
        "A_regional": A_regional,
        "B_competitor_profiles": B_profiles,
    }

    outs = args.out or [str(ROOT / "competitor_analysis.json")]
    for o in outs:
        Path(o).parent.mkdir(parents=True, exist_ok=True)
        json.dump(out, open(o, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print(f"[ok] 저장: {o}", file=sys.stderr)
    print(f"[done] 권역 {len(A_regional)}개 · generated_at {out['generated_at']}", file=sys.stderr)


if __name__ == "__main__":
    main()
