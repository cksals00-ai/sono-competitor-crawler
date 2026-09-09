#!/usr/bin/env python3
"""
build_hotel_trends_full.py
data/daily_extract/ (비누적 일자별 원본) → analytics/hotel_trends_full.csv

preprocess_trends.aggregate_hotel_trends 를 그대로 재사용해 기존
analytics/hotel_trends.csv 와 동일한 스키마를 유지한다.

기존 hotel_trends.csv 와의 차이:
  - 누적본 재기록 과정에서 유실된 자사(is_own)/경쟁사 행이 복원되어 있다.
  - 수집일 커버리지가 140일 → 145일.
  - 가용여부를 price>0 추정이 아니라 원본 '판매상태' 컬럼에서 직접 가져온다.

실행: python3 scripts/build_hotel_trends_full.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from preprocess_trends import _normalize_columns, aggregate_hotel_trends  # noqa: E402

SRC = ROOT / "data" / "daily_extract"
OUT = ROOT / "analytics" / "hotel_trends_full.csv"

DEDUP_KEY = ["crawled_at", "property_id", "competitor_name", "ota",
             "checkin_date", "checkout_date", "room_type", "room_category"]


def log(m):
    print(m, flush=True)


def main():
    files = sorted(SRC.glob("sono_daily_????????.csv"))
    if not files:
        sys.exit(f"입력 없음: {SRC} — extract_daily_from_archive.py 를 먼저 실행하세요")

    log(f"입력 {len(files)}일치")
    parts = []
    raw_total = dedup_total = 0
    t0 = time.time()

    for i, p in enumerate(files, 1):
        df = pd.read_csv(p, encoding="utf-8-sig", low_memory=False)
        if df.empty:
            continue
        df = _normalize_columns(df)
        # 원본 '판매상태'를 가용여부로 사용 (available / sold_out / unknown)
        if "판매상태" in df.columns and "availability" not in df.columns:
            df = df.rename(columns={"판매상태": "availability"})
        if "crawled_at" not in df.columns:
            log(f"  건너뜀(수집일시 없음): {p.name}")
            continue

        raw_total += len(df)
        key = [c for c in DEDUP_KEY if c in df.columns]
        if key:
            df = df.drop_duplicates(subset=key, keep="last")
        dedup_total += len(df)

        df["crawl_date"] = pd.to_datetime(df["crawled_at"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df[df["crawl_date"].notna()]

        parts.append(aggregate_hotel_trends(df))
        if i % 20 == 0 or i == len(files):
            log(f"  [{i:3d}/{len(files)}] {p.name}  누적 raw {raw_total:,} → "
                f"dedup {dedup_total:,}  ({time.time()-t0:.0f}s)")

    log("\n병합 중 ...")
    out = pd.concat(parts, ignore_index=True)
    out = out.sort_values(["crawl_date", "property_name", "competitor_name",
                           "ota", "checkin_date"])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False, encoding="utf-8-sig")

    days = out["crawl_date"].nunique()
    log(f"\n완료: {len(out):,} 행 / {days}일 "
        f"({out['crawl_date'].min()} ~ {out['crawl_date'].max()})")
    log(f"  → {OUT.relative_to(ROOT)}  ({OUT.stat().st_size/1048576:.0f}MB)  "
        f"{time.time()-t0:.0f}초")


if __name__ == "__main__":
    main()
