"""
데일리 크롤링 데이터 전처리 및 추이 분석용 집계 스크립트

기능:
  1. exports/ 의 날짜별 CSV를 모두 읽어 날짜(crawl_date) 컬럼 추가
  2. 호텔 가격 추이 집계 → analytics/hotel_trends.csv / .parquet
  3. 골프 그린피 추이 집계 → analytics/golf_trends.csv / .parquet
  4. 빠른 조회용 피벗 테이블 → analytics/price_pivot_{dimension}.csv

Usage:
    python preprocess_trends.py                   # 전체 날짜 처리
    python preprocess_trends.py --days 30         # 최근 30일만
    python preprocess_trends.py --no-parquet      # Parquet 미생성 (pandas 의존성 최소화)
    python preprocess_trends.py --output ./analytics
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

# ── 로거 ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ── 경로 상수 ─────────────────────────────────────────────────────────────────
PROJECT_DIR = Path(__file__).parent.resolve()

# ── 컬럼 정규화 맵 (한글 → 영어) ──────────────────────────────────────────────
_KO_TO_EN: dict[str, str] = {
    "소노사업장": "property_name",
    "사업장ID":   "property_id",
    "경쟁사명":   "competitor_name",
    "OTA":        "ota",
    "채널":       "ota",
    "체크인":     "checkin_date",
    "체크아웃":   "checkout_date",
    "객실유형":   "room_type",
    "객실카테고리": "room_category",
    "판매가(원)": "price",
    "통화":       "currency",
    "가용여부":   "availability",
    "URL":        "url",
    "오류":       "error",
    "자사여부":   "is_own",
    "프로모":     "is_promo",
    "리뷰점수":   "review_score",
    "리뷰수":     "review_count",
    "수집일시":   "crawled_at",
}

_HOTEL_DATE_RE = re.compile(r"sono_competitor_prices_(\d{8})\.csv$", re.IGNORECASE)
_GOLF_DATE_RE  = re.compile(r"golf_prices_(\d{8})\.csv$",            re.IGNORECASE)


# ── 유틸 ─────────────────────────────────────────────────────────────────────

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """한글 컬럼명을 영어로 통일."""
    return df.rename(columns={k: v for k, v in _KO_TO_EN.items() if k in df.columns})


def _parse_date_from_filename(path: Path, pattern: re.Pattern) -> str | None:
    m = pattern.search(path.name)
    if not m:
        return None
    raw = m.group(1)  # YYYYMMDD
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"


def _read_csv_safe(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
        return df
    except Exception as e:
        log.warning(f"CSV 읽기 실패 ({path.name}): {e}")
        return pd.DataFrame()


def _bool_col(series: pd.Series) -> pd.Series:
    """문자열 True/False → bool."""
    if series.dtype == bool:
        return series
    return series.astype(str).str.strip().str.lower().map(
        {"true": True, "1": True, "false": False, "0": False}
    ).fillna(False)


# ── 호텔 데이터 로드 ──────────────────────────────────────────────────────────

def load_hotel_raw(export_dir: Path, since_date: str | None = None) -> pd.DataFrame:
    """exports/ 의 호텔 CSV를 모두 읽어 crawl_date 컬럼과 함께 반환."""
    frames: list[pd.DataFrame] = []
    csvs = sorted(export_dir.glob("sono_competitor_prices_????????.csv"))
    if not csvs:
        log.warning(f"호텔 CSV 없음: {export_dir}")
        return pd.DataFrame()

    for path in csvs:
        crawl_date = _parse_date_from_filename(path, _HOTEL_DATE_RE)
        if crawl_date is None:
            continue
        if since_date and crawl_date < since_date:
            continue

        df = _read_csv_safe(path)
        if df.empty:
            continue

        df = _normalize_columns(df)
        df["crawl_date"] = crawl_date
        frames.append(df)

    if not frames:
        log.warning("로드된 호텔 CSV 없음")
        return pd.DataFrame()

    raw = pd.concat(frames, ignore_index=True)
    log.info(f"호텔 raw: {len(raw):,} 행 / {raw['crawl_date'].nunique()} 일치 / {export_dir}")
    return raw


# ── 골프 데이터 로드 ──────────────────────────────────────────────────────────

def load_golf_raw(export_dir: Path, since_date: str | None = None) -> pd.DataFrame:
    """exports/ 의 골프 CSV를 모두 읽어 crawl_date 컬럼과 함께 반환."""
    frames: list[pd.DataFrame] = []
    csvs = sorted(export_dir.glob("golf_prices_????????.csv"))
    if not csvs:
        log.warning(f"골프 CSV 없음: {export_dir}")
        return pd.DataFrame()

    for path in csvs:
        crawl_date = _parse_date_from_filename(path, _GOLF_DATE_RE)
        if crawl_date is None:
            continue
        if since_date and crawl_date < since_date:
            continue

        df = _read_csv_safe(path)
        if df.empty:
            continue

        df["crawl_date"] = crawl_date
        frames.append(df)

    if not frames:
        log.warning("로드된 골프 CSV 없음")
        return pd.DataFrame()

    raw = pd.concat(frames, ignore_index=True)
    log.info(f"골프 raw: {len(raw):,} 행 / {raw['crawl_date'].nunique()} 일치")
    return raw


# ── 호텔 추이 집계 ────────────────────────────────────────────────────────────

def aggregate_hotel_trends(raw: pd.DataFrame) -> pd.DataFrame:
    """
    crawl_date × property × competitor × channel × checkin_date × room_category 단위로
    가격 및 가용성을 집계한다.

    출력 컬럼:
      crawl_date, property_name, property_id, competitor_name, is_own,
      ota, checkin_date, checkout_date, room_category,
      price_min, price_avg, price_max,
      available_cnt, sold_out_cnt, total_cnt, availability_rate
    """
    if raw.empty:
        return pd.DataFrame()

    df = raw.copy()

    # 타입 정규화
    for col in ["price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    if "is_own" in df.columns:
        df["is_own"] = _bool_col(df["is_own"])

    # 가용성 판정 (price > 0 이면 available, 0이면 sold_out 또는 error)
    if "availability" not in df.columns:
        df["availability"] = df["price"].apply(lambda p: "available" if p > 0 else "sold_out")

    df["is_available"] = df["availability"].str.lower().str.startswith("available")
    df["is_sold_out"]  = df["availability"].str.lower().str.startswith("sold_out")

    # 집계 차원 컬럼 목록 (없는 컬럼은 제외)
    dim_candidates = [
        "crawl_date", "property_name", "property_id",
        "competitor_name", "is_own", "ota",
        "checkin_date", "checkout_date", "room_category",
    ]
    dims = [c for c in dim_candidates if c in df.columns]

    # price > 0 인 행만으로 min/avg/max 집계 (0은 품절/오류)
    available_df = df[df["price"] > 0] if "price" in df.columns else df

    agg_price = (
        available_df.groupby(dims, dropna=False)["price"]
        .agg(price_min="min", price_avg="mean", price_max="max")
        .reset_index()
    )

    agg_avail = (
        df.groupby(dims, dropna=False)
        .agg(
            available_cnt=("is_available", "sum"),
            sold_out_cnt=("is_sold_out", "sum"),
            total_cnt=("is_available", "count"),
        )
        .reset_index()
    )
    agg_avail["availability_rate"] = (
        agg_avail["available_cnt"] / agg_avail["total_cnt"].replace(0, pd.NA)
    ).round(4)

    result = agg_avail.merge(agg_price, on=dims, how="left")
    result["price_avg"] = result["price_avg"].round(0).astype("Int64")

    result = result.sort_values(["crawl_date", "property_name", "competitor_name", "ota", "checkin_date"])
    log.info(f"호텔 추이 집계 완료: {len(result):,} 행")
    return result


# ── 골프 추이 집계 ────────────────────────────────────────────────────────────

def aggregate_golf_trends(raw: pd.DataFrame) -> pd.DataFrame:
    """
    crawl_date × property × channel × play_date × time_of_day 단위로
    그린피를 집계한다.

    출력 컬럼:
      crawl_date, property_name, property_id, competitor_name, is_own,
      channel, course_name, play_date, day_of_week, time_of_day,
      fee_min_krw, fee_avg_krw, fee_max_krw,
      fee_min_usd, fee_avg_usd, fee_max_usd
    """
    if raw.empty:
        return pd.DataFrame()

    df = raw.copy()

    for col in ["green_fee_krw"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    if "green_fee_usd" in df.columns:
        df["green_fee_usd"] = pd.to_numeric(df["green_fee_usd"], errors="coerce").fillna(0.0)

    if "is_own" in df.columns:
        df["is_own"] = _bool_col(df["is_own"])

    dim_candidates = [
        "crawl_date", "property_name", "property_id",
        "competitor_name", "is_own", "channel",
        "course_name", "play_date", "day_of_week", "time_of_day",
    ]
    dims = [c for c in dim_candidates if c in df.columns]

    valid = df[df["green_fee_krw"] > 0] if "green_fee_krw" in df.columns else df

    agg = (
        valid.groupby(dims, dropna=False)
        .agg(
            fee_min_krw=("green_fee_krw", "min"),
            fee_avg_krw=("green_fee_krw", "mean"),
            fee_max_krw=("green_fee_krw", "max"),
            fee_min_usd=("green_fee_usd", "min") if "green_fee_usd" in valid.columns else ("green_fee_krw", lambda x: 0),
            fee_avg_usd=("green_fee_usd", "mean") if "green_fee_usd" in valid.columns else ("green_fee_krw", lambda x: 0),
            fee_max_usd=("green_fee_usd", "max") if "green_fee_usd" in valid.columns else ("green_fee_krw", lambda x: 0),
        )
        .reset_index()
    )

    for col in ["fee_avg_krw"]:
        if col in agg.columns:
            agg[col] = agg[col].round(0).astype("Int64")
    for col in ["fee_avg_usd"]:
        if col in agg.columns:
            agg[col] = agg[col].round(2)

    agg = agg.sort_values(["crawl_date", "property_name", "channel", "play_date"])
    log.info(f"골프 추이 집계 완료: {len(agg):,} 행")
    return agg


# ── 피벗 테이블 생성 ──────────────────────────────────────────────────────────

def make_price_pivot(
    trends: pd.DataFrame,
    *,
    property_id: str | None = None,
    ota: str | None = None,
    checkin_date: str | None = None,
) -> pd.DataFrame:
    """
    날짜(crawl_date)를 열로, 사업장+경쟁사+채널을 행으로 한 가격 피벗.

    필터 인자:
      property_id  - 특정 사업장만
      ota          - 특정 채널만
      checkin_date - 특정 체크인 날짜만
    """
    df = trends.copy()
    if property_id:
        df = df[df["property_id"] == property_id]
    if ota and "ota" in df.columns:
        df = df[df["ota"] == ota]
    if checkin_date and "checkin_date" in df.columns:
        df = df[df["checkin_date"] == checkin_date]

    if df.empty:
        return pd.DataFrame()

    id_cols = [c for c in ["property_name", "competitor_name", "ota", "room_category", "checkin_date"] if c in df.columns]
    pivot = df.pivot_table(
        index=id_cols,
        columns="crawl_date",
        values="price_min",
        aggfunc="min",
    ).reset_index()
    pivot.columns.name = None
    return pivot


def make_golf_pivot(
    golf_trends: pd.DataFrame,
    *,
    property_id: str | None = None,
    channel: str | None = None,
) -> pd.DataFrame:
    """골프 그린피 날짜별 피벗."""
    df = golf_trends.copy()
    if property_id:
        df = df[df["property_id"] == property_id]
    if channel and "channel" in df.columns:
        df = df[df["channel"] == channel]

    if df.empty:
        return pd.DataFrame()

    id_cols = [c for c in ["property_name", "channel", "course_name", "play_date", "time_of_day"] if c in df.columns]
    pivot = df.pivot_table(
        index=id_cols,
        columns="crawl_date",
        values="fee_min_krw",
        aggfunc="min",
    ).reset_index()
    pivot.columns.name = None
    return pivot


# ── 요약 통계 ─────────────────────────────────────────────────────────────────

def make_daily_summary(hotel_trends: pd.DataFrame, golf_trends: pd.DataFrame) -> pd.DataFrame:
    """날짜별 수집 건수 및 가격 요약."""
    rows = []

    if not hotel_trends.empty:
        for d, g in hotel_trends.groupby("crawl_date"):
            own = g[g["is_own"] == True] if "is_own" in g.columns else pd.DataFrame()
            comp = g[g["is_own"] == False] if "is_own" in g.columns else pd.DataFrame()
            rows.append({
                "crawl_date": d,
                "data_type": "hotel",
                "total_records": len(g),
                "own_avg_price": int(own["price_avg"].mean()) if not own.empty and "price_avg" in own.columns else None,
                "comp_avg_price": int(comp["price_avg"].mean()) if not comp.empty and "price_avg" in comp.columns else None,
                "n_properties": g["property_id"].nunique() if "property_id" in g.columns else None,
                "n_competitors": g["competitor_name"].nunique() if "competitor_name" in g.columns else None,
                "n_channels": g["ota"].nunique() if "ota" in g.columns else None,
            })

    if not golf_trends.empty:
        for d, g in golf_trends.groupby("crawl_date"):
            own = g[g["is_own"] == True] if "is_own" in g.columns else pd.DataFrame()
            comp = g[g["is_own"] == False] if "is_own" in g.columns else pd.DataFrame()
            rows.append({
                "crawl_date": d,
                "data_type": "golf",
                "total_records": len(g),
                "own_avg_price": int(own["fee_avg_krw"].mean()) if not own.empty and "fee_avg_krw" in own.columns else None,
                "comp_avg_price": int(comp["fee_avg_krw"].mean()) if not comp.empty and "fee_avg_krw" in comp.columns else None,
                "n_properties": g["property_id"].nunique() if "property_id" in g.columns else None,
                "n_competitors": g["competitor_name"].nunique() if "competitor_name" in g.columns else None,
                "n_channels": g["channel"].nunique() if "channel" in g.columns else None,
            })

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values(["crawl_date", "data_type"]).reset_index(drop=True)


# ── 저장 ─────────────────────────────────────────────────────────────────────

def _save(df: pd.DataFrame, path: Path, *, parquet: bool = True):
    """CSV + 선택적으로 Parquet 저장."""
    if df.empty:
        log.warning(f"저장 건너뜀 (빈 DataFrame): {path}")
        return

    csv_path = path.with_suffix(".csv")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    log.info(f"저장: {csv_path} ({len(df):,} 행)")

    if parquet:
        try:
            pq_path = path.with_suffix(".parquet")
            df.to_parquet(pq_path, index=False)
            log.info(f"저장: {pq_path}")
        except Exception as e:
            log.warning(f"Parquet 저장 실패 ({path.name}): {e}")


# ── 메인 ─────────────────────────────────────────────────────────────────────

def run(
    export_dir: Path,
    analytics_dir: Path,
    since_date: str | None = None,
    parquet: bool = True,
):
    analytics_dir.mkdir(parents=True, exist_ok=True)

    # ── 호텔 ──
    hotel_raw = load_hotel_raw(export_dir, since_date)
    if not hotel_raw.empty:
        hotel_trends = aggregate_hotel_trends(hotel_raw)
        _save(hotel_trends, analytics_dir / "hotel_trends", parquet=parquet)

        # 전체 피벗 (가장 가까운 체크인 날짜 기준)
        hotel_pivot = make_price_pivot(hotel_trends)
        _save(hotel_pivot, analytics_dir / "hotel_price_pivot", parquet=False)

        log.info(f"호텔 추이 저장 완료 → {analytics_dir}")
    else:
        hotel_trends = pd.DataFrame()

    # ── 골프 ──
    golf_raw = load_golf_raw(export_dir, since_date)
    if not golf_raw.empty:
        golf_trends = aggregate_golf_trends(golf_raw)
        _save(golf_trends, analytics_dir / "golf_trends", parquet=parquet)

        golf_pivot = make_golf_pivot(golf_trends)
        _save(golf_pivot, analytics_dir / "golf_price_pivot", parquet=False)

        log.info(f"골프 추이 저장 완료 → {analytics_dir}")
    else:
        golf_trends = pd.DataFrame()

    # ── 데일리 요약 ──
    summary = make_daily_summary(hotel_trends, golf_trends)
    _save(summary, analytics_dir / "daily_summary", parquet=False)

    return hotel_trends, golf_trends, summary


def main():
    parser = argparse.ArgumentParser(description="크롤링 데이터 추이 전처리")
    parser.add_argument("--days", type=int, default=None, help="최근 N일만 처리 (기본: 전체)")
    parser.add_argument("--since", type=str, default=None, help="시작 날짜 YYYY-MM-DD (기본: 전체)")
    parser.add_argument("--export-dir", type=str, default=None, help="exports 디렉토리 경로")
    parser.add_argument("--output", type=str, default=None, help="analytics 출력 디렉토리 경로")
    parser.add_argument("--no-parquet", action="store_true", help="Parquet 파일 미생성")
    args = parser.parse_args()

    # config.yaml에서 export_dir 읽기
    config_path = PROJECT_DIR / "config.yaml"
    if args.export_dir:
        export_dir = Path(args.export_dir).resolve()
    elif config_path.exists():
        with open(config_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        export_dir = (PROJECT_DIR / cfg.get("output", {}).get("export_dir", "./exports")).resolve()
    else:
        export_dir = PROJECT_DIR / "exports"

    analytics_dir = Path(args.output).resolve() if args.output else PROJECT_DIR / "analytics"

    # since_date 결정
    since_date = args.since
    if args.days and not since_date:
        since_date = (datetime.today() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    log.info(f"export_dir : {export_dir}")
    log.info(f"analytics  : {analytics_dir}")
    log.info(f"since_date : {since_date or '전체'}")

    hotel_trends, golf_trends, summary = run(
        export_dir,
        analytics_dir,
        since_date=since_date,
        parquet=not args.no_parquet,
    )

    if not summary.empty:
        print("\n── 데일리 요약 ──────────────────────────────────────────")
        print(summary.to_string(index=False))
    else:
        print("처리된 데이터 없음")


if __name__ == "__main__":
    main()
