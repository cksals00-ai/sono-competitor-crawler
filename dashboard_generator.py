"""
소노 경쟁사 가격 대시보드 생성기
크롤링 결과 DataFrame → 반응형 다크 테마 HTML
저장: dashboard/index.html (브라우저에서 바로 열기 가능)

Usage:
    from dashboard_generator import generate_dashboard, load_previous_df
    prev_df = load_previous_df("./exports")
    generate_dashboard(df, "dashboard/index.html", prev_df=prev_df)
"""

import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# ── 기능 플래그 ───────────────────────────────────────────────────────────────
# 자사몰 API 수정 완료 전까지 자사몰 섹션 숨김
_SHOW_HOMEPAGE_SECTION = False

# ── OTA 설정 ─────────────────────────────────────────────────────────────────
OTA_ORDER   = ["야놀자", "네이버호텔", "Trip.com"]
OTA_SHORT   = {
    "야놀자":   "야놀자",
    "네이버호텔": "네이버호텔",
    "Trip.com": "Trip.com",
    "여기어때":  "여기어때",
    "Agoda":   "Agoda",
    "자사홈":   "자사홈",
}
OTA_CLASS   = {
    "야놀자":   "yanolja",
    "네이버호텔": "naver",
    "Trip.com": "tripcom",
    "여기어때":  "yeogi",
    "Agoda":   "agoda",
    "자사홈":   "homepage",
}

# ── 요일 구분 ─────────────────────────────────────────────────────────────────
DAY_TYPES  = ["전체", "주중", "금", "토", "연휴"]
DAY_LABELS = {
    "전체": "전체",
    "주중": "주중(월~목)",
    "금":   "금요일",
    "토":   "토요일",
    "연휴": "공휴일",
}

# 공휴일 목록 (YYYY-MM-DD, 연휴 전체 기간 포함)
HOLIDAYS = frozenset({
    # 2025
    "2025-01-01",
    "2025-01-28", "2025-01-29", "2025-01-30", "2025-01-31",
    "2025-03-01",
    "2025-05-05", "2025-05-06",
    "2025-06-06",
    "2025-08-15",
    "2025-10-03", "2025-10-05", "2025-10-06", "2025-10-07", "2025-10-08",
    "2025-10-09",
    "2025-12-25",
    # 2026
    "2026-01-01",
    "2026-02-15", "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19",
    "2026-03-01",
    "2026-05-05",
    "2026-05-24",
    "2026-06-06",
    "2026-08-15",
    "2026-09-23", "2026-09-24", "2026-09-25", "2026-09-26", "2026-09-27",
    "2026-10-03",
    "2026-10-09",
    "2026-12-25",
})

# 프로모션 감지 키워드
PROMO_KEYWORDS = frozenset([
    "오픈런", "★특가★", "특가", "할인", "프로모션", "Limited-time", "Genius", "genius",
])
PROMO_PREFIXES = ["[오픈런]", "★특가★", "[특가]", "[프로모션]", "[할인]"]

# ── 지역 분류 ─────────────────────────────────────────────────────────────────
REGION_LABELS = {
    "강원": ["강원"],
    "충청": ["충남", "충북"],
    "경기": ["경기"],
    "전라": ["전북", "전남"],
    "부산": ["부산"],
    "경북": ["경북"],
    "경남": ["경남"],
    "제주": ["제주"],
    "해외": ["베트남", "Vietnam", "미국", "하와이"],
}

# display_region 기준 필터 순서 (config.yaml 의 display_region 필드 값)
DISPLAY_REGION_ORDER = ["아시아퍼시픽", "비발디파크", "한국중부", "한국남부"]


def _get_display_region(prop: dict) -> str:
    """config prop의 display_region 우선, 없으면 region 문자열로 추정."""
    dr = prop.get("display_region", "")
    if dr:
        return dr
    return _get_region(prop.get("region", ""))


# ── 설정·데이터 로드 ──────────────────────────────────────────────────────────

def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_fit_rates(json_path: str = "fit_rates.json") -> dict:
    """자사 브래드닷컴 객실요금 JSON 로드. 없으면 빈 dict 반환."""
    try:
        p = Path(json_path)
        if not p.exists():
            return {}
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"fit_rates.json 로드 실패: {e}")
        return {}


def _load_channel_data(json_path: str = "channel_sales_data.json") -> dict:
    """채널별 판매객실수 JSON 로드. 없으면 빈 dict 반환."""
    try:
        p = Path(json_path)
        if not p.exists():
            return {}
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        # property_name → 데이터 매핑으로 변환 (여러 property_names 지원)
        mapping: dict = {}
        for entry in data.get("properties", []):
            for pname in entry.get("property_names", []):
                mapping[pname] = entry
        return {"entries": mapping, "meta": data}
    except Exception as e:
        logger.warning(f"channel_sales_data.json 로드 실패: {e}")
        return {}


def load_previous_df(export_dir: str) -> pd.DataFrame:
    """어제 날짜 CSV 로드 (가격 변동 계산용). 없으면 빈 DataFrame 반환."""
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    csv_path = Path(export_dir) / f"sono_competitor_prices_{yesterday}.csv"
    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig")
            logger.info(f"전일 데이터 로드: {csv_path}")
            return _normalize_columns(df)
        except Exception as e:
            logger.warning(f"전일 데이터 로드 실패: {e}")
    return pd.DataFrame()


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """한글 컬럼명 → 영문 정규화 (export_powerbi._prepare_df 역변환).
    이미 영문 컬럼이 있는 경우엔 덮어쓰지 않는다."""
    KO_TO_EN = {
        "수집일시":   "crawled_at",
        "소노사업장":  "property_name",
        "사업장ID":   "property_id",
        "경쟁사명":   "competitor_name",
        "OTA":       "ota",
        "체크인":     "checkin_date",
        "체크아웃":   "checkout_date",
        "객실유형":   "room_type",
        "객실카테고리": "room_category",
        "판매가(원)": "price",
        "통화":      "currency",
        "판매상태":   "availability",
        "URL":       "url",
        "오류":      "error",
        "별점(10점)": "review_score",
        "리뷰수":     "review_count",
    }
    rename_map = {
        k: v for k, v in KO_TO_EN.items()
        if k in df.columns and v not in df.columns
    }
    if rename_map:
        return df.rename(columns=rename_map)
    return df


# ── 메인 공개 함수 ────────────────────────────────────────────────────────────

def load_golf_df(export_dir: str = "./exports") -> pd.DataFrame:
    """최신 golf_prices_*.csv 로드. 없으면 빈 DataFrame 반환."""
    paths = sorted(Path(export_dir).glob("golf_prices_*.csv"))
    if not paths:
        return pd.DataFrame()
    csv_path = paths[-1]
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        logger.info(f"골프 데이터 로드: {csv_path}")
        return df
    except Exception as e:
        logger.warning(f"골프 데이터 로드 실패: {e}")
        return pd.DataFrame()


def generate_dashboard(
    df: pd.DataFrame,
    output_path: str = "dashboard/index.html",
    config_path: str = "config.yaml",
    prev_df: pd.DataFrame = None,
    golf_df: pd.DataFrame = None,
) -> str:
    """
    Parameters
    ----------
    df          : 오늘 크롤링 결과 DataFrame
    output_path : 저장할 HTML 경로
    config_path : config.yaml 경로
    prev_df     : 전일 DataFrame (가격 변동 표시용, 없으면 None)

    Returns
    -------
    str : 저장된 파일의 절대 경로
    """
    output_path = str(output_path)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    cfg = load_config(config_path)

    # 한글 컬럼명으로 저장된 CSV를 직접 넘기는 경우 정규화
    df = _normalize_columns(df)
    if prev_df is not None and not prev_df.empty:
        prev_df = _normalize_columns(prev_df)

    # 요일별 summary 빌드 (최저가 + 메타)
    summaries = {dt: _build_price_summary(df, dt) for dt in DAY_TYPES}

    # 전일 기준: 같은 체크인일의 어제 크롤링 가격
    has_prev      = prev_df is not None and not prev_df.empty
    prev_per_date = _build_per_date_prices(prev_df) if has_prev else {}

    # 전일 날짜 레이블 (범례용)
    prev_date = ""
    if has_prev and "crawled_at" in prev_df.columns:
        _prev_ca = prev_df["crawled_at"].dropna().astype(str)
        raw = str(_prev_ca.max())[:10] if len(_prev_ca) > 0 else ""
        try:
            pd_ = datetime.strptime(raw, "%Y-%m-%d")
            prev_date = f"{pd_.month}/{pd_.day}"
        except Exception:
            prev_date = raw

    if not df.empty and "crawled_at" in df.columns:
        _ca = df["crawled_at"].dropna().astype(str)
        crawled_at = str(_ca.max()) if len(_ca) > 0 else ""
    else:
        crawled_at = ""

    # 경쟁사별 OTA 별점 요약 (review_score 컬럼이 있을 때만)
    review_summary = _build_review_summary(df)

    html = _render_html(df, cfg, summaries, prev_per_date, crawled_at, prev_date, review_summary, golf_df)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    abs_path = str(Path(output_path).resolve())
    logger.info(f"대시보드 생성: {abs_path}")

    # GitHub Pages 서빙 경로 (docs/index.html) 동기화
    docs_path = Path(output_path).parent.parent / "docs" / "index.html"
    if docs_path.parent.exists():
        with open(docs_path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info(f"GitHub Pages 동기화: {docs_path}")

    return abs_path


def _build_review_summary(df: pd.DataFrame) -> dict:
    """
    경쟁사별 OTA 별점을 요약한다.
    반환: {competitor_name: {ota: score, ...}, ...}
    review_score > 0 인 데이터만 집계.
    """
    score_col = "review_score" if "review_score" in df.columns else None
    if score_col is None:
        return {}

    valid = df[df[score_col] > 0].copy()
    if valid.empty:
        return {}

    ota_col  = "ota"  if "ota"  in valid.columns else "OTA"
    comp_col = "competitor_name" if "competitor_name" in valid.columns else "경쟁사명"
    valid[ota_col] = valid[ota_col].apply(_normalize_ota)

    result = {}
    for (comp, ota), grp in valid.groupby([comp_col, ota_col]):
        score = round(grp[score_col].mean(), 1)
        if comp not in result:
            result[comp] = {}
        result[comp][ota] = score
    return result


# ── 데이터 처리 ───────────────────────────────────────────────────────────────

def _get_day_type(date_str: str) -> str:
    """날짜 → 요일 구분 (체크인 날짜 기준)
    주중: 월~목, 일  /  금: 금  /  토: 토  /  연휴: 공휴일
    """
    try:
        d  = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
        ds = d.strftime("%Y-%m-%d")
        if ds in HOLIDAYS:
            return "연휴"
        wd = d.weekday()   # 0=Mon … 6=Sun
        if wd == 5:
            return "토"
        if wd == 4:
            return "금"
        return "주중"      # Mon~Thu + Sun
    except Exception:
        return "주중"


def _is_promo(room_type: str) -> bool:
    if not room_type:
        return False
    return any(kw in room_type for kw in PROMO_KEYWORDS)


def _clean_room_type(room_type: str) -> str:
    """객실명 정규화: 프로모션 접두어·옵션 접미사 제거 후 20자 이내로 반환.

    - Trip.com 최저가 레이블('최저가', '최저가(참고)')은 그대로 표시
    - ' - 무료 Wi-Fi', ' - Room Only' 등 옵션 접미사 제거
    - '(취사/스탠다드/침대)' 등 슬래시 포함 괄호 제거
    - '(룸온리)', '(전망없음)' 등 조건 키워드 괄호 제거
    - '[확정예약]', '[13평]' 등 대괄호 태그 제거
    - 3채널(야놀자·네이버호텔·Trip.com) 동일 형식으로 표시
    """
    if not room_type:
        return ""
    if room_type.strip() == "최저가":
        return "최저가 기준"
    if room_type.strip() == "최저가(참고)":
        return "최저가(참고)"
    rt = room_type
    for pfx in PROMO_PREFIXES:
        rt = rt.replace(pfx, "").strip()
    # 옵션 접미사 제거: " - 무료 Wi-Fi", " - Room Only" 등
    if " - " in rt:
        rt = rt.split(" - ")[0].strip()
    # 슬래시 포함 괄호 제거: "(취사/스탠다드/침대)", "(클린/파크뷰/침대)" 등
    rt = re.sub(r'\s*\([^)]*\/[^)]*\)', '', rt).strip()
    # 조건 키워드 괄호 제거: "(룸온리)", "(룸온니)", "(전망없음)", "(No View)" 등
    rt = re.sub(
        r'\s*\([^)]*(?:룸온리|룸온니|없음|Only|Kitchen|Standard|Bed|View|뷰|전망)\s*[^)]*\)',
        '', rt, flags=re.IGNORECASE
    ).strip()
    # 대괄호 태그 제거: "[확정예약]", "[13평]", "[쉿크릿]" 등
    rt = re.sub(r'\s*\[[^\]]*\]\s*', ' ', rt).strip()
    # 연속 공백 정리
    rt = re.sub(r'\s+', ' ', rt).strip()
    if len(rt) > 20:
        rt = rt[:19] + "…"
    return rt


def _build_price_summary(df: pd.DataFrame, day_type: str = "전체") -> dict:
    """
    DataFrame → {(property_name, competitor_name, ota): info_dict}
    info_dict keys: min_price, min_date, sold_out, room_type, is_promo, room_category,
                    category_mismatch (자사 행에서 카테고리 불일치 시 True)
    """
    if df is None or df.empty:
        return {}
    needed = {"property_name", "competitor_name", "ota", "price", "checkin_date"}
    if not needed.issubset(df.columns):
        return {}

    df_ok = df[df["error"].fillna("") == ""].copy() if "error" in df.columns else df.copy()
    df_ok["ota"] = df_ok["ota"].apply(_normalize_ota)

    if day_type and day_type != "전체":
        df_ok = df_ok[df_ok["checkin_date"].apply(_get_day_type) == day_type]

    result = {}
    for (prop, comp, ota), grp in df_ok.groupby(
        ["property_name", "competitor_name", "ota"], sort=False
    ):
        avail = grp[grp["price"].fillna(0) > 0]
        if avail.empty:
            result[(prop, comp, ota)] = {
                "min_price": 0, "min_date": "", "sold_out": True,
                "room_type": "", "is_promo": False, "room_category": "",
                "category_mismatch": False,
            }
        else:
            row  = avail.loc[avail["price"].idxmin()]
            rt_val = row.get("room_type", "")
            rc_val = row.get("room_category", "")
            rt   = "" if pd.isna(rt_val) else str(rt_val)
            rc   = "" if pd.isna(rc_val) else str(rc_val)
            is_p = bool(row.get("is_promo", False)) or _is_promo(rt)
            result[(prop, comp, ota)] = {
                "min_price":        int(row["price"]),
                "min_date":         str(row["checkin_date"])[:10],
                "sold_out":         False,
                "room_type":        rt,
                "room_category":    rc,
                "is_promo":         is_p,
                "category_mismatch": False,
            }

    # ── 자사 행 카테고리 매칭 (property_name == competitor_name) ─────────────
    # 야놀자 최저가의 room_category를 기준으로, 네이버호텔·Trip.com도 같은
    # 카테고리 내 최저가로 재계산. 해당 카테고리 데이터 없으면 category_mismatch=True.
    own_props = {prop for (prop, comp, _ota) in result if prop == comp}
    for prop_name in own_props:
        yanolja_key = (prop_name, prop_name, "야놀자")
        if yanolja_key not in result or result[yanolja_key]["sold_out"]:
            continue
        base_category = result[yanolja_key].get("room_category", "")
        if not base_category:
            continue

        for ota in ["네이버호텔", "Trip.com"]:
            key = (prop_name, prop_name, ota)
            if key not in result or result[key]["sold_out"]:
                continue

            mask = (
                (df_ok["property_name"] == prop_name) &
                (df_ok["competitor_name"] == prop_name) &
                (df_ok["ota"] == ota) &
                (df_ok["price"].fillna(0) > 0)
            )
            cat_avail = df_ok[mask & (df_ok.get("room_category", pd.Series(dtype=str)) == base_category)] \
                if "room_category" in df_ok.columns \
                else df_ok[mask].iloc[0:0]

            if cat_avail.empty:
                result[key]["category_mismatch"] = True
            else:
                row  = cat_avail.loc[cat_avail["price"].idxmin()]
                rt_val = row.get("room_type", "")
                rc_val = row.get("room_category", "")
                rt   = "" if pd.isna(rt_val) else str(rt_val)
                rc   = "" if pd.isna(rc_val) else str(rc_val)
                is_p = bool(row.get("is_promo", False)) or _is_promo(rt)
                result[key] = {
                    "min_price":        int(row["price"]),
                    "min_date":         str(row["checkin_date"])[:10],
                    "sold_out":         False,
                    "room_type":        rt,
                    "room_category":    rc,
                    "is_promo":         is_p,
                    "category_mismatch": False,
                }

    return result


def _build_per_date_prices(df: pd.DataFrame) -> dict:
    """
    전일 대비 비교용: 조회날짜 기준, 동일 체크인일 가격 매핑.
    (어제 크롤링한 같은 체크인일 가격 vs 오늘 크롤링한 같은 체크인일 가격)

    Returns
    -------
    {(property_name, competitor_name, ota, checkin_date): min_price}
    """
    if df is None or df.empty:
        return {}
    needed = {"property_name", "competitor_name", "ota", "price", "checkin_date"}
    if not needed.issubset(df.columns):
        return {}

    df_ok = df[df["error"].fillna("") == ""].copy() if "error" in df.columns else df.copy()
    df_ok["ota"] = df_ok["ota"].apply(_normalize_ota)
    result = {}
    for (prop, comp, ota, date), grp in df_ok.groupby(
        ["property_name", "competitor_name", "ota", "checkin_date"], sort=False
    ):
        avail = grp[grp["price"].fillna(0) > 0]
        if not avail.empty:
            result[(prop, comp, ota, str(date)[:10])] = int(avail["price"].min())
    return result


# ── OTA 이름 정규화 / URL 헬퍼 ────────────────────────────────────────────────

def _normalize_ota(ota) -> str:
    """'네이버호텔/야놀자' → '야놀자', '네이버호텔/Trip.com' → 'Trip.com' 등
    서브채널이 있으면 서브채널 기준, 아니면 메인채널 유지"""
    # Handle NaN, None, float and other non-string types
    if not isinstance(ota, str):
        ota_str = str(ota) if ota is not None and not (isinstance(ota, float) and pd.isna(ota)) else ""
    else:
        ota_str = ota

    if not ota_str:
        return ota_str

    # 서브채널 매핑: "네이버호텔/야놀자" → "야놀자"
    _SUB_MAP = {
        "야놀자": "야놀자",
        "여기어때": "여기어때",
        "Trip.com": "Trip.com",
        "Agoda": "Agoda",
    }

    if "/" in ota_str:
        sub = ota_str.split("/", 1)[1]
        if sub in _SUB_MAP:
            return _SUB_MAP[sub]
        # 알려진 OTA가 아닌 서브채널은 네이버호텔로 통합
        return "네이버호텔"

    return ota_str


def _build_naver_url_map(df: pd.DataFrame) -> dict:
    """CSV 데이터에서 경쟁사명 → 네이버호텔 공개 URL 매핑 반환.
    config.yaml의 naver_id는 GraphQL 내부 ID로 공개 URL과 다르므로 크롤링 데이터를 사용."""
    if df is None or df.empty:
        return {}
    url_col  = "url"             if "url"             in df.columns else None
    comp_col = "competitor_name" if "competitor_name" in df.columns else None
    ota_col  = "ota"             if "ota"             in df.columns else None
    if not all([url_col, comp_col, ota_col]):
        return {}
    # 네이버호텔 관련 OTA 행만 (raw ota 값이 "네이버"로 시작)
    naver_mask = df[ota_col].fillna("").astype(str).str.startswith("네이버")
    naver_df = df[naver_mask]
    result = {}
    for comp_name, grp in naver_df.groupby(comp_col):
        urls = grp[url_col].dropna().astype(str)
        valid = urls[
            urls.str.startswith("https://hotels.naver.com/") &
            (urls.str.len() > len("https://hotels.naver.com/"))
        ]
        if not valid.empty:
            # 쿼리스트링 제거 후 베이스 URL만 보존
            result[comp_name] = valid.iloc[0].split("?")[0]
    return result


def _get_ota_url(entity: dict, ota: str) -> str:
    """competitor dict 또는 own_urls dict에서 OTA 기본 URL 조합.
    entity에는 yanolja_url, agoda_url, naver_url, tripcom_hotel_id 등이 있다."""
    if ota == "야놀자":
        return entity.get("yanolja_url", "")
    if ota == "여기어때":
        return entity.get("yeogiuh_url", "")
    if ota == "Agoda":
        return entity.get("agoda_url", "")
    if ota == "네이버호텔":
        # 크롤링 데이터에서 추출한 실제 공개 URL 우선 사용
        naver_url = entity.get("naver_url", "")
        # ID 없는 URL (https://hotels.naver.com/ 만 있는 경우) 필터링
        if naver_url and naver_url.rstrip("/") != "https://hotels.naver.com":
            return naver_url
        return ""
    if ota == "Trip.com":
        hotel_id = entity.get("tripcom_hotel_id", 0)
        city_id  = entity.get("tripcom_city_id", 0)
        if hotel_id:
            return f"https://kr.trip.com/hotels/detail/?hotelId={hotel_id}&cityId={city_id}"
        return ""
    return ""


# ── OTA 딥링크 ────────────────────────────────────────────────────────────────

def _make_ota_link_url(base_url: str, ota: str, checkin: str) -> str:
    """체크인 날짜가 포함된 OTA 딥링크 URL 생성"""
    if not base_url or not checkin:
        return base_url or "#"
    try:
        checkout = (
            datetime.strptime(checkin[:10], "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")
    except Exception:
        return base_url
    if ota == "야놀자":
        return f"{base_url}?checkInDate={checkin}&checkOutDate={checkout}"
    if ota == "여기어때":
        return f"{base_url}?checkIn={checkin}&checkOut={checkout}&personal=2"
    if ota == "Agoda":
        return f"{base_url}?checkIn={checkin}&checkOut={checkout}&adults=2&rooms=1"
    if ota == "네이버호텔":
        return f"{base_url}?checkIn={checkin}&checkOut={checkout}&adultCnt=2"
    if ota == "Trip.com":
        return f"{base_url}&checkin={checkin}&checkout={checkout}&adult=2&rooms=1"
    return base_url


# ── 포맷 헬퍼 ─────────────────────────────────────────────────────────────────

def _fmt_price(price: int) -> str:
    return f"&#8361;{price:,}"          # ₩ (HTML entity)


def _fmt_date(date_str: str) -> str:
    try:
        d = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
        return f"{d.month}/{d.day}"
    except Exception:
        return str(date_str)


def _change_html(curr: int, prev: int) -> str:
    """조회날짜 기준 전일 대비 변동 — 실제 금액 + 퍼센트 함께 표시"""
    if prev <= 0:
        return '<span class="badge-new">신규</span>'
    diff = curr - prev
    pct  = diff / prev * 100
    if abs(pct) < 0.5:
        return '<span class="change-same">&#8212;</span>'
    abs_diff = abs(diff)
    abs_pct  = abs(pct)
    if diff > 0:
        return (
            f'<span class="change-up">'
            f'&#9650;{abs_diff:,}원 (+{abs_pct:.0f}%)'
            f'</span>'
        )
    return (
        f'<span class="change-down">'
        f'&#9660;{abs_diff:,}원 (-{abs_pct:.0f}%)'
        f'</span>'
    )


def _get_region(region_str: str) -> str:
    for label, keywords in REGION_LABELS.items():
        for kw in keywords:
            if kw in (region_str or ""):
                return label
    return "기타"


# ── HTML 렌더링 ───────────────────────────────────────────────────────────────

def _render_price_cell(
    prop_name: str, comp_name: str, ota: str,
    summaries: dict,        # {day_type: summary_dict}
    prev_per_date: dict,    # {(prop, comp, ota, checkin_date): price}
    ota_url: str,           # 실제 OTA URL (없으면 빈 문자열)
    is_own: bool = False,
    review_summary: dict = None,  # {comp_name: {ota: score}}
) -> str:
    """요일별 레이어를 포함한 <td> 반환. 가격 클릭 시 해당 OTA 딥링크로 이동.
    ota_url이 없어도 summary에 가격 데이터가 있으면 (링크 없이) 가격을 표시한다.
    ota_url도 없고 데이터도 없으면 no-data 셀을 반환한다."""
    key = (prop_name, comp_name, ota)

    # URL도 없고 어떤 요일에도 데이터도 없으면 no-data
    has_data = any(key in summaries.get(dt, {}) for dt in DAY_TYPES)
    if not ota_url and not has_data:
        return '<td class="price-cell no-data">&#8212;</td>'

    # 이 OTA의 별점 (있으면 가격 옆에 인라인 표시)
    review_summary = review_summary or {}
    rating_score = review_summary.get(comp_name, {}).get(ota)

    layers = []

    for dt in DAY_TYPES:
        active_cls = " dt-active" if dt == "전체" else ""
        summary    = summaries.get(dt, {})

        if key not in summary:
            inner = '<div class="price" style="color:#3a3f4b">&#8212;</div>'
        elif summary[key]["sold_out"]:
            inner = '<div class="sold-out-txt">매진</div>'
        else:
            info      = summary[key]
            price     = info["min_price"]
            checkin   = info["min_date"]
            date_disp = _fmt_date(checkin)
            room_type = info.get("room_type", "")
            is_promo  = info.get("is_promo", False)

            # 전일 동일 체크인일 기준 가격 변동 (조회날짜 기준)
            prev_price = prev_per_date.get((prop_name, comp_name, ota, checkin), 0)
            change     = _change_html(price, prev_price) if prev_price > 0 \
                         else ('<span class="badge-new">신규</span>' if prev_per_date else "")

            price_cls  = "own-price" if is_own else ""
            promo_html = ' <span class="badge-promo">특가</span>' if is_promo else ""

            clean_rt         = _clean_room_type(room_type)
            room_category    = info.get("room_category", "")
            cat_mismatch     = info.get("category_mismatch", False)
            cat_badge        = f' <span class="badge-category">{room_category}</span>' if room_category else ""
            mismatch_badge   = ' <span class="badge-cat-mismatch" title="기준 카테고리 없음 — 전체 최저가 표시">!</span>' if cat_mismatch else ""
            room_html        = f'<div class="room-type">{clean_rt}{cat_badge}{mismatch_badge}</div>' if clean_rt else (
                f'<div class="room-type">{mismatch_badge}</div>' if mismatch_badge else ""
            )

            # 별점 인라인 표시: ₩92,000 ⭐4.8
            rating_html = (
                f' <span class="inline-rating">&#11088;{rating_score}</span>'
                if rating_score else ""
            )

            if ota_url:
                link_url   = _make_ota_link_url(ota_url, ota, checkin)
                price_html = (
                    f'<a href="{link_url}" target="_blank" rel="noopener" '
                    f'class="price-link {price_cls}">'
                    f'{_fmt_price(price)}{promo_html}</a>'
                )
            else:
                # URL 미설정이지만 데이터 있음 → 링크 없이 가격만 표시
                price_html = (
                    f'<span class="price-link {price_cls}">'
                    f'{_fmt_price(price)}{promo_html}</span>'
                )

            inner = (
                f'{price_html}'
                f'{rating_html}'
                f'{room_html}'
                f'<div class="price-meta">{date_disp} {change}</div>'
            )

        layers.append(
            f'<div class="dt-layer{active_cls}" data-dt="{dt}">{inner}</div>'
        )

    return f'<td class="price-cell">{"".join(layers)}</td>'


def _render_gauge(label: str, actual: int, budget: int) -> str:
    """
    SVG 반원 게이지 차트 1개 생성.
    달성률 < 80% → 빨강, 80-99% → 노랑, >= 100% → 초록.
    초과달성 시 게이지는 100% 채우고 색상으로 구분.
    """
    if budget <= 0:
        return ""
    pct_raw = actual / budget * 100
    pct     = round(pct_raw, 1)

    color = (
        "#4ade80" if pct_raw >= 100
        else "#fbbf24" if pct_raw >= 80
        else "#f87171"
    )

    # SVG 반원 파라미터
    # viewBox: 0 0 120 70  (반원 + 아래 여백)
    # r=50, cx=60, cy=60 → 반원이 상단에 위치
    cx, cy, r = 60, 60, 48
    stroke_w = 10
    # strokeDasharray for semicircle: 반원 둘레 = π*r ≈ 150.8
    # dasharray = [arc_length, rest]
    # 반원 전체 dash = π*r, 나머지 = 전체 원 - 반원 = π*r
    import math
    circumference = math.pi * r          # 반원 둘레 ≈ 150.8
    fill_ratio    = min(pct_raw / 100.0, 1.0)
    arc_fill      = circumference * fill_ratio
    arc_gap       = circumference - arc_fill
    # 나머지 절반(아래쪽)은 무조건 비워야 하므로 extra gap 추가
    dash_array    = f"{arc_fill:.1f} {arc_gap + circumference:.1f}"

    pct_display = f"{pct}%"
    actual_str  = f"{actual:,}"
    budget_str  = f"{budget:,}"

    return f"""\
<div class="gauge-wrap">
  <div class="gauge-label">{label}</div>
  <svg viewBox="0 0 120 70" class="gauge-svg" aria-label="{label} 달성률 {pct_display}">
    <!-- 배경 반원 -->
    <path d="M {cx-r},{cy} A {r},{r} 0 0,1 {cx+r},{cy}"
          fill="none" stroke="rgba(255,255,255,.12)" stroke-width="{stroke_w}"
          stroke-linecap="round"/>
    <!-- 달성 반원 -->
    <path d="M {cx-r},{cy} A {r},{r} 0 0,1 {cx+r},{cy}"
          fill="none" stroke="{color}" stroke-width="{stroke_w}"
          stroke-linecap="round"
          stroke-dasharray="{dash_array}"
          stroke-dashoffset="0"/>
    <!-- 달성률 % (중앙 상단) -->
    <text x="{cx}" y="{cy - 8}" text-anchor="middle"
          font-size="18" font-weight="700" fill="{color}">{pct_display}</text>
    <!-- 실적 숫자 (달성률 아래) -->
    <text x="{cx}" y="{cy + 8}" text-anchor="middle"
          font-size="9" fill="rgba(255,255,255,.7)">{actual_str}</text>
    <!-- 0 레이블 -->
    <text x="{cx - r - 2}" y="{cy + 14}" text-anchor="end"
          font-size="7" fill="rgba(255,255,255,.4)">0</text>
    <!-- 목표 숫자 -->
    <text x="{cx + r + 2}" y="{cy + 14}" text-anchor="start"
          font-size="7" fill="rgba(255,255,255,.4)">{budget_str}</text>
  </svg>
</div>"""


def _render_budget_gauges(budget_info: dict | None) -> str:
    """
    OTA/GOTA/합계 3개의 반원 게이지 차트 섹션 HTML 생성.
    """
    if not budget_info:
        return ""

    ota          = budget_info.get("OTA",  {})
    gota         = budget_info.get("GOTA", {})
    total_budget = budget_info.get("total_budget", 0)
    total_actual = (ota.get("actual", 0) or 0) + (gota.get("actual", 0) or 0)

    gauges = []
    if ota and ota.get("budget", 0):
        gauges.append(_render_gauge("OTA",  ota.get("actual", 0) or 0,  ota["budget"]))
    if gota and gota.get("budget", 0):
        gauges.append(_render_gauge("GOTA", gota.get("actual", 0) or 0, gota["budget"]))
    if total_budget:
        gauges.append(_render_gauge("합계", total_actual, total_budget))

    gauges = [g for g in gauges if g]
    if not gauges:
        return ""

    return (
        '<div class="bgt-section">'
        '<div class="bgt-title">목표 달성률 (BU)</div>'
        '<div class="gauge-row">'
        + "".join(gauges)
        + "</div></div>"
    )


def _render_channel_section(prop_name: str, channel_data: dict) -> str:
    """채널별 판매객실수 토글 섹션 HTML 생성."""
    if not channel_data:
        return ""
    entries = channel_data.get("entries", {})
    entry = entries.get(prop_name)
    if not entry:
        return ""

    meta       = channel_data.get("meta", {})
    label      = meta.get("label", "금월")
    ch_data    = entry.get("channels", {})
    total      = entry.get("total", {})
    budget_info = entry.get("budget")

    # 사업장에 실제 데이터가 있는 채널만, RNS 내림차순 정렬
    channels = sorted(
        [ch for ch, d in ch_data.items() if d.get("rns", 0) > 0],
        key=lambda ch: ch_data[ch].get("rns", 0),
        reverse=True,
    )

    rows = []
    for ch in channels:
        d = ch_data.get(ch, {})
        rns  = d.get("rns", 0)
        prev = d.get("prev", 0)
        if prev > 0:
            pct    = round((rns - prev) / prev * 100)
            sign   = "+" if pct >= 0 else ""
            cls    = "ch-up" if pct >= 0 else "ch-dn"
            growth = f'<span class="{cls}">{sign}{pct}%</span>'
        else:
            growth = '<span class="ch-na">-</span>'
        rows.append(
            f'<tr>'
            f'<td class="ch-name">{ch}</td>'
            f'<td class="ch-num">{rns:,}</td>'
            f'<td class="ch-num ch-prev">{prev:,}</td>'
            f'<td class="ch-growth">{growth}</td>'
            f'</tr>'
        )

    # 합계 행
    t_rns  = total.get("rns", 0)
    t_prev = total.get("prev", 0)
    if t_prev > 0:
        t_pct   = round((t_rns - t_prev) / t_prev * 100)
        t_sign  = "+" if t_pct >= 0 else ""
        t_cls   = "ch-up" if t_pct >= 0 else "ch-dn"
        t_growth = f'<span class="{t_cls}">{t_sign}{t_pct}%</span>'
    else:
        t_growth = '<span class="ch-na">-</span>'

    rows_html    = "\n".join(rows)
    budget_html  = _render_budget_gauges(budget_info)
    return f"""\
<div class="channel-section">
  <button class="channel-toggle" type="button">
    <span class="channel-toggle-label">채널별 판매객실수</span>
    <span class="channel-toggle-meta">{label}</span>
    <span class="channel-arrow">&#9660;</span>
  </button>
  <div class="channel-body">
{budget_html}
    <table class="channel-table">
      <thead>
        <tr>
          <th class="ch-th-name">채널</th>
          <th class="ch-th-num">금월 RNS</th>
          <th class="ch-th-num">전년동월</th>
          <th class="ch-th-growth">증감</th>
        </tr>
      </thead>
      <tbody>
{rows_html}
        <tr class="ch-total-row">
          <td class="ch-name">합계</td>
          <td class="ch-num">{t_rns:,}</td>
          <td class="ch-num ch-prev">{t_prev:,}</td>
          <td class="ch-growth">{t_growth}</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>"""


def _render_homepage_section(
    prop_name: str,
    df: pd.DataFrame,
    prev_per_date: dict,
) -> str:
    """자사몰 객실가격 토글 섹션 — 사업장 헤더 아래, 경쟁사 표 위에 삽입."""
    if df is None or df.empty:
        return ""
    needed = {"property_name", "competitor_name", "ota", "price", "checkin_date", "room_type"}
    if not needed.issubset(df.columns):
        return ""

    df_ok = df[df["error"].fillna("") == ""].copy() if "error" in df.columns else df.copy()
    mask = (
        (df_ok["property_name"] == prop_name) &
        (df_ok["competitor_name"] == prop_name) &
        (df_ok["ota"] == "자사홈")
    )
    hp_df = df_ok[mask]
    avail = hp_df[hp_df["price"].fillna(0) > 0].copy()
    if avail.empty:
        return ""

    def _make_rows(sub_df: pd.DataFrame) -> str:
        if sub_df.empty:
            return '<tr><td colspan="3" style="text-align:center;color:#3a3f4b;padding:8px">데이터 없음</td></tr>'
        rows = []
        for rt, grp in sub_df.groupby("room_type", sort=True):
            row_min  = grp.loc[grp["price"].idxmin()]
            price    = int(row_min["price"])
            checkin  = str(row_min["checkin_date"])[:10]
            is_p     = bool(row_min.get("is_promo", False)) or _is_promo(str(rt))
            clean_rt = _clean_room_type(str(rt))
            date_disp = _fmt_date(checkin)
            prev_price = prev_per_date.get((prop_name, prop_name, "자사홈", checkin), 0)
            change     = _change_html(price, prev_price) if prev_price > 0 \
                         else ('<span class="badge-new">신규</span>' if prev_per_date else "")
            promo_html = ' <span class="badge-promo">특가</span>' if is_p else ""
            rows.append(
                f'<tr>'
                f'<td class="hp-room">{clean_rt}</td>'
                f'<td class="hp-price">{_fmt_price(price)}{promo_html}</td>'
                f'<td class="hp-meta">{date_disp}&ensp;{change}</td>'
                f'</tr>'
            )
        return "\n".join(rows)

    layers = []
    for dt in DAY_TYPES:
        active_cls = " dt-active" if dt == "전체" else ""
        if dt == "전체":
            sub_df = avail
        else:
            sub_df = avail[avail["checkin_date"].apply(_get_day_type) == dt]
        rows_html = _make_rows(sub_df)
        layers.append(
            f'<div class="dt-layer{active_cls}" data-dt="{dt}">'
            f'<table class="homepage-table">'
            f'<thead><tr>'
            f'<th class="hp-th-room">객실타입</th>'
            f'<th class="hp-th-price">가격</th>'
            f'<th class="hp-th-meta">날짜 / 변동</th>'
            f'</tr></thead>'
            f'<tbody>{rows_html}</tbody>'
            f'</table>'
            f'</div>'
        )

    return f"""\
<div class="homepage-section">
  <button class="homepage-toggle" type="button">
    <span class="homepage-toggle-label">자사몰 객실가격</span>
    <span class="homepage-arrow">&#9660;</span>
  </button>
  <div class="homepage-body">
    {"".join(layers)}
  </div>
</div>"""


def _render_fit_section(prop_name: str, fit_data: dict) -> str:
    """자사 브래드닷컴 객실요금 토글 섹션 HTML 생성.

    fit_data 구조:
    {
      "generated_at": "YYYY-MM-DD",
      "properties": {
        "<prop_name>": {
          "room_types": [...],
          "rates": [{"date": "YYYY-MM-DD", "요일": str, "시즌명": str,
                     "rooms": {rt: price, ...}}, ...]
        }
      }
    }
    """
    if not fit_data:
        return ""
    prop_entry = fit_data.get("properties", {}).get(prop_name)
    if not prop_entry:
        return ""

    room_types: list[str] = prop_entry.get("room_types", [])
    rates: list[dict]     = prop_entry.get("rates", [])
    gen_date: str         = fit_data.get("generated_at", "")

    if not room_types or not rates:
        return ""

    # 요일별로 가장 가까운 날짜 찾기
    def nearest_for_dt(dt_filter: str) -> dict | None:
        for r in rates:
            dt = _get_day_type(r["date"])
            if dt_filter == "전체" or dt == dt_filter:
                return r
        return None

    layers = []
    for dt in DAY_TYPES:
        active_cls = " dt-active" if dt == "전체" else ""
        row = nearest_for_dt(dt)

        if row is None:
            inner = '<tr><td colspan="3" style="text-align:center;color:#3a3f4b;padding:8px">해당 요일 데이터 없음</td></tr>'
        else:
            date_disp = _fmt_date(row["date"])
            시즌명    = row.get("시즌명", "")
            rooms     = row.get("rooms", {})
            tr_rows   = []
            for rt in room_types:
                price = rooms.get(rt, 0)
                if price <= 0:
                    continue
                short_rt = rt if len(rt) <= 20 else rt[:19] + "…"
                tr_rows.append(
                    f'<tr>'
                    f'<td class="hp-room">{short_rt}</td>'
                    f'<td class="hp-price">{_fmt_price(price)}</td>'
                    f'<td class="hp-meta">{date_disp}'
                    + (f'&ensp;<span class="fit-season">{시즌명}</span>' if 시즌명 else "")
                    + f'</td></tr>'
                )
            if tr_rows:
                inner = "\n".join(tr_rows)
            else:
                inner = '<tr><td colspan="3" style="text-align:center;color:#3a3f4b;padding:8px">가격 없음</td></tr>'

        layers.append(
            f'<div class="dt-layer{active_cls}" data-dt="{dt}">'
            f'<table class="homepage-table">'
            f'<thead><tr>'
            f'<th class="hp-th-room">객실타입</th>'
            f'<th class="hp-th-price">객실요금</th>'
            f'<th class="hp-th-meta">날짜 / 시즌</th>'
            f'</tr></thead>'
            f'<tbody>{inner}</tbody>'
            f'</table>'
            f'</div>'
        )

    gen_label = f"기준일 {gen_date}" if gen_date else ""
    return f"""\
<div class="fit-section">
  <button class="fit-toggle" type="button">
    <span class="fit-toggle-label">객실요금</span>
    <span class="fit-toggle-meta">{gen_label}</span>
    <span class="fit-arrow">&#9660;</span>
  </button>
  <div class="fit-body">
    {"".join(layers)}
  </div>
</div>"""


def _render_property_card(
    prop: dict,
    summaries: dict,        # {day_type: summary_dict}
    prev_per_date: dict,    # {(prop, comp, ota, checkin_date): price}
    df: pd.DataFrame,
    review_summary: dict = None,  # {comp_name: {ota: score}}
    channel_data: dict = None,    # _load_channel_data() 결과
    fit_data: dict = None,        # _load_fit_rates() 결과
) -> str:
    prop_name    = prop["name"]
    region_str   = prop.get("region", "")
    region_label = _get_display_region(prop)
    competitors  = prop.get("competitors", [])
    own_urls     = prop.get("own_urls", {})
    has_own      = any([
        own_urls.get("yanolja_url", ""),
        own_urls.get("yeogiuh_url", ""),
        own_urls.get("agoda_url", ""),
        own_urls.get("naver_id", ""),
        own_urls.get("tripcom_hotel_id", 0),
    ])
    review_summary = review_summary or {}

    # 네이버호텔 실제 공개 URL 맵 (크롤링 데이터 기반, config naver_id와 공개 ID가 다름)
    naver_url_map = _build_naver_url_map(df)

    if not df.empty and "property_name" in df.columns:
        prop_df  = df[df["property_name"] == prop_name]
        ok_count = int((prop_df["error"].fillna("") == "").sum()) if "error" in prop_df.columns else len(prop_df)
    else:
        ok_count = 0

    ota_ths = "".join(
        f'<th class="ota-{OTA_CLASS[ota]}">{OTA_SHORT[ota]}</th>'
        for ota in OTA_ORDER
    )

    rows = []

    # ── 자사 가격 행 ──────────────────────────────────────────────────────────
    if has_own:
        own_urls_with_naver = {**own_urls, "naver_url": naver_url_map.get(prop_name, "")}
        own_cells = "".join(
            _render_price_cell(
                prop_name, prop_name, ota,
                summaries, prev_per_date,
                _get_ota_url(own_urls_with_naver, ota),
                is_own=True,
                review_summary=review_summary,
            )
            for ota in OTA_ORDER
        )
        rows.append(
            f'<tr class="own-row">'
            f'<td class="competitor-name own-label">'
            f'<span class="badge-sono">자사</span>{prop_name}'
            f'</td>'
            f'{own_cells}'
            f'</tr>'
        )

    # ── 경쟁사 행 ─────────────────────────────────────────────────────────────
    for comp in competitors:
        comp_name = comp["name"]
        comp_with_naver = {**comp, "naver_url": naver_url_map.get(comp_name, "")}
        cells = "".join(
            _render_price_cell(
                prop_name, comp_name, ota,
                summaries, prev_per_date,
                _get_ota_url(comp_with_naver, ota),
                review_summary=review_summary,
            )
            for ota in OTA_ORDER
        )
        rows.append(
            f'<tr><td class="competitor-name">{comp_name}</td>{cells}</tr>'
        )

    rows_html    = "\n          ".join(rows)
    comp_label   = f"{len(competitors)}개 경쟁사"
    if has_own:
        comp_label = "자사 포함 · " + comp_label

    channel_html  = _render_channel_section(prop_name, channel_data)
    homepage_html = _render_homepage_section(prop_name, df, prev_per_date)
    fit_html      = _render_fit_section(prop_name, fit_data or {})

    return f"""\
<div class="property-card" data-region="{region_label}">
  <div class="property-header">
    <div>
      <div class="property-title">{prop_name}</div>
      <div class="property-region">{region_str}</div>
    </div>
    <div class="property-stats">{comp_label}&nbsp;&middot;&nbsp;{ok_count:,}건</div>
  </div>
{fit_html}
{homepage_html}
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th class="competitor-col">구분</th>
        {ota_ths}
      </tr></thead>
      <tbody>
          {rows_html}
      </tbody>
    </table>
  </div>
{channel_html}</div>"""


def _render_review_cell(comp_name: str, review_summary: dict) -> str:
    """경쟁사 별점을 OTA별로 표시하는 TD 셀 렌더링."""
    comp_scores = review_summary.get(comp_name, {})
    if not comp_scores:
        return '<td class="review-cell review-na">-</td>'

    parts = []
    for ota in OTA_ORDER:
        score = comp_scores.get(ota)
        if score:
            ota_cls = OTA_CLASS[ota]
            color_class = (
                "review-high" if score >= 9.0
                else "review-mid" if score >= 8.0
                else "review-low"
            )
            parts.append(
                f'<span class="review-badge ota-{ota_cls} {color_class}" '
                f'title="{OTA_SHORT[ota]}: {score}점">'
                f'{OTA_SHORT[ota]}&nbsp;{score}'
                f'</span>'
            )

    if not parts:
        return '<td class="review-cell review-na">-</td>'
    return f'<td class="review-cell">{"&nbsp;".join(parts)}</td>'


def _render_golf_property_card(property_name: str, golf_df: pd.DataFrame) -> str:
    """골프 사업장 카드 HTML 생성 - 경쟁사를 열(column)로 배치, 호텔 카드와 동일한 스타일."""
    prop_df = golf_df[golf_df["property_name"] == property_name].copy()
    if prop_df.empty:
        return ""

    region_map = {
        "하이퐁": "베트남 하이퐁",
        "망길라오": "괌 망길라오",
        "탈로포포": "괌 탈로포포",
    }
    region = next((v for k, v in region_map.items() if k in property_name), "")

    # 경쟁사를 열(column)로 배치: 자사 먼저, 이후 경쟁사 가나다순
    all_comps = prop_df["competitor_name"].dropna().unique().tolist()
    own_comps = [c for c in all_comps if c == "자사"]
    other_comps = sorted([c for c in all_comps if c != "자사"])
    competitors_ordered = own_comps + other_comps

    # 채널을 행(row)으로 배치
    channel_order = ["몽키트래블", "AGL", "KKday"]
    present_channels = prop_df["channel"].dropna().unique().tolist()
    channels = [ch for ch in channel_order if ch in present_channels]
    channels += [ch for ch in present_channels if ch not in channel_order]

    weekday_df = prop_df[prop_df["day_of_week"] == "주중"]
    weekend_df = prop_df[prop_df["day_of_week"] == "주말"]

    def _min_krw(sub):
        if sub.empty:
            return None
        v = sub["green_fee_krw"].min()
        return None if pd.isna(v) else int(v)

    def _min_usd(sub):
        if sub.empty:
            return None
        v = sub["green_fee_usd"].min()
        return None if pd.isna(v) else round(float(v), 0)

    def _price_html(krw, usd):
        if krw is None:
            return "<span class='golf-na'>-</span>"
        s = f"{krw:,}원"
        if usd:
            s += f'<span class="golf-usd"> (${usd:.0f})</span>'
        return s

    # 헤더 행: 경쟁사 열 + 채널별 열
    chan_header_cells = ""
    for channel in channels:
        chan_header_cells += f'<th class="golf-th-comp">{channel}</th>'

    # 데이터 행: 경쟁사별 1행, 각 셀에 채널별 코스명+주중/주말 가격
    rows = []
    for comp in competitors_ordered:
        is_own = comp == "자사"
        if is_own:
            badge = '<span class="badge-sono">자사</span>'
            comp_cell = f'<td class="golf-chan-col golf-own-col">{badge}{property_name}</td>'
        else:
            comp_cell = f'<td class="golf-chan-col">{comp}</td>'

        cells = comp_cell
        for channel in channels:
            cell_cls = "golf-cell" + (" golf-own-cell" if is_own else "")

            sub = prop_df[
                (prop_df["competitor_name"] == comp) &
                (prop_df["channel"] == channel)
            ]
            if sub.empty:
                cells += f'<td class="{cell_cls} golf-na-cell">&#8212;</td>'
                continue

            course_name = sub["course_name"].iloc[0]
            holes = sub["holes"].dropna()
            holes_str = f" {int(holes.iloc[0])}H" if not holes.empty else ""

            wkd  = weekday_df[(weekday_df["competitor_name"] == comp) & (weekday_df["channel"] == channel)]
            wknd = weekend_df[(weekend_df["competitor_name"] == comp) & (weekend_df["channel"] == channel)]

            wkd_krw  = _min_krw(wkd)
            wkd_usd  = _min_usd(wkd)
            wknd_krw = _min_krw(wknd)
            wknd_usd = _min_usd(wknd)

            cart = sub["cart_included"].dropna()
            cart_str = "카트포함" if not cart.empty and bool(cart.any()) else "카트별도"

            urls = sub["url"].dropna()
            url = urls.iloc[0] if not urls.empty else ""

            inner = (
                f'<div class="golf-course-label">{course_name}{holes_str}</div>'
                f'<div class="golf-day-row">'
                f'<span class="golf-day-badge wkd">주중</span>{_price_html(wkd_krw, wkd_usd)}'
                f'</div>'
                f'<div class="golf-day-row">'
                f'<span class="golf-day-badge wknd">주말</span>{_price_html(wknd_krw, wknd_usd)}'
                f'</div>'
                f'<div class="golf-cart-info">{cart_str}</div>'
            )
            if url:
                inner = f'<a href="{url}" target="_blank" rel="noopener" class="golf-cell-link">{inner}</a>'

            cells += f'<td class="{cell_cls}">{inner}</td>'

        rows.append(f'<tr>{cells}</tr>')

    rows_html = "\n          ".join(rows)
    comp_label = f"{len(other_comps)}개 경쟁사"
    channels_str = " &middot; ".join(channels)

    return f"""\
<div class="property-card">
  <div class="property-header">
    <div>
      <div class="property-title">{property_name}</div>
      <div class="property-region">{region}</div>
    </div>
    <div class="property-stats">{comp_label}&nbsp;&middot;&nbsp;{channels_str}</div>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th class="golf-th-chan">경쟁사</th>
        {chan_header_cells}
      </tr></thead>
      <tbody>
          {rows_html}
      </tbody>
    </table>
  </div>
</div>"""


def _render_golf_section(golf_df: pd.DataFrame) -> str:
    """골프 전체 섹션 HTML 생성."""
    if golf_df is None or golf_df.empty:
        return (
            '<div id="golf-section" style="display:none">'
            '<p style="padding:40px;text-align:center;color:var(--muted)">골프 데이터가 없습니다.</p>'
            '</div>'
        )

    properties = golf_df["property_name"].dropna().unique().tolist()

    def _sort_key(p):
        return (0, p) if "괌" in p else (1, p)

    properties = sorted(properties, key=_sort_key)

    crawled_at = str(golf_df["crawled_at"].max())[:16] if "crawled_at" in golf_df.columns else ""

    date_range_html = ""
    if "play_date" in golf_df.columns:
        valid = golf_df["play_date"].dropna()
        if not valid.empty:
            try:
                dm = datetime.strptime(str(valid.min())[:10], "%Y-%m-%d")
                dx = datetime.strptime(str(valid.max())[:10], "%Y-%m-%d")
                date_range_html = f"대상기간: {dm.month}/{dm.day} ~ {dx.month}/{dx.day}&ensp;&middot;&ensp;"
            except Exception:
                pass

    cards_html = "\n\n".join(_render_golf_property_card(p, golf_df) for p in properties)

    return f"""\
<div id="golf-section" style="display:none">
  <div class="golf-meta-bar">
    수집: {crawled_at}&ensp;&middot;&ensp;{date_range_html}채널: 몽키트래블 &middot; AGL &middot; KKday
  </div>
  <div class="legend-bar">
    <span class="legend-label">골프 그린피 최저가</span>
    &emsp;&middot;&emsp;<span class="legend-note">주중(월~금) / 주말(토·일) 기준 오전·오후 통합 최저가</span>
    &emsp;&middot;&emsp;<span class="legend-note">카트비 포함 여부는 해당 채널 기준</span>
  </div>
  <main class="main">
    <div class="property-grid">
{cards_html}
    </div>
  </main>
</div>"""


def _render_html(
    df: pd.DataFrame, cfg: dict,
    summaries: dict, prev_per_date: dict,
    crawled_at: str, prev_date: str = "",
    review_summary: dict = None,
    golf_df: pd.DataFrame = None,
) -> str:
    properties = cfg.get("properties", [])
    review_summary = review_summary or {}

    total_props = len(properties)
    total_comps = sum(len(p.get("competitors", [])) for p in properties)
    if not df.empty and "error" in df.columns:
        total_ok = int((df["error"].fillna("") == "").sum())
    else:
        total_ok = len(df) if not df.empty else 0
    prices_ok = int((df["price"].fillna(0) > 0).sum()) if not df.empty and "price" in df.columns else 0

    channel_data = _load_channel_data()
    fit_data     = _load_fit_rates()

    cards_html = "\n\n".join(
        _render_property_card(p, summaries, prev_per_date, df, review_summary, channel_data, fit_data)
        for p in properties
    )

    # 지역 필터 버튼 — display_region 기준, DISPLAY_REGION_ORDER 순서 유지
    prop_regions = [_get_display_region(p) for p in properties]
    region_set   = set(prop_regions)
    ordered      = [r for r in DISPLAY_REGION_ORDER if r in region_set]
    remaining    = sorted(r for r in region_set if r not in DISPLAY_REGION_ORDER)
    regions      = ["전체"] + ordered + remaining
    filter_btns = "\n    ".join(
        f'<button class="filter-btn{"  active" if r == "전체" else ""}" data-region="{r}">{r}</button>'
        for r in regions
    )

    # 요일 필터 버튼
    dt_btns = "\n    ".join(
        f'<button class="filter-btn dt-btn{"  active" if dt == "전체" else ""}" '
        f'data-dt="{dt}">{DAY_LABELS[dt]}</button>'
        for dt in DAY_TYPES
    )

    # 범례
    prev_date_disp = f"전일({prev_date})" if prev_date else "전일"
    legend_html = f"""\
<div class="legend-bar">
  <span class="legend-label">{prev_date_disp} 대비(조회일 기준):</span>
  <span class="change-down">&#9660; 하락</span>
  <span class="change-up">&#9650; 상승</span>
  <span class="change-same">&#8212; 변동없음</span>
  <span class="badge-new">신규</span><span class="legend-note">&thinsp;전일 없음</span>
  &emsp;&middot;&emsp;<span class="badge-promo">특가</span><span class="legend-note">&thinsp;프로모션 진행중</span>
  &emsp;&middot;&emsp;<span class="badge-cat-mismatch">!</span><span class="legend-note">&thinsp;카테고리 불일치(전체 최저가)</span>
  &emsp;&middot;&emsp;<span class="legend-note">※ 자사 행은 야놀자 기준 카테고리로 동일 비교</span>
</div>"""

    crawled_disp = crawled_at[:16] if crawled_at else "&#8212;"
    gen_time     = datetime.now().strftime("%Y-%m-%d %H:%M")

    # 골프 섹션
    golf_section_html = _render_golf_section(golf_df)
    has_golf = golf_df is not None and not golf_df.empty
    golf_tab_btn = '<button class="cat-btn" data-cat="golf">골프 그린피</button>' if has_golf else ""

    if not df.empty and "checkin_date" in df.columns:
        try:
            dm = datetime.strptime(str(df["checkin_date"].min())[:10], "%Y-%m-%d")
            dx = datetime.strptime(str(df["checkin_date"].max())[:10], "%Y-%m-%d")
            date_range = f"{dm.month}/{dm.day} ~ {dx.month}/{dx.day}"
        except Exception:
            date_range = "&#8212;"
    else:
        date_range = "&#8212;"

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
  <title>GS Monitor | SONO Hotels &amp; Resorts</title>
  <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&family=Space+Grotesk:wght@500;700&display=swap" rel="stylesheet">
  <style>{_CSS}</style>
</head>
<body>

<header class="header">
  <div class="header-inner">
    <div class="header-left">
      <div class="header-title-wrap">
        <span class="header-title"><span class="header-gs">GS</span> Monitor</span>
        <span class="header-team-badge">SONO Hotels &amp; Resorts</span>
      </div>
      <div class="header-meta">수집: {crawled_disp}&ensp;&middot;&ensp;생성: {gen_time}&ensp;&middot;&ensp;대상기간: {date_range}&ensp;&middot;&ensp;비교기준: {prev_date_disp}</div>
    </div>
    <div class="header-badge">LIVE</div>
  </div>
</header>

<div class="stats-bar">
  <div class="stat-item">
    <span class="stat-label">사업장</span>
    <span class="stat-value">{total_props}</span>
  </div>
  <div class="stat-item">
    <span class="stat-label">경쟁사</span>
    <span class="stat-value">{total_comps}</span>
  </div>
  <div class="stat-item">
    <span class="stat-label">수집건수</span>
    <span class="stat-value">{total_ok:,}</span>
  </div>
  <div class="stat-item">
    <span class="stat-label">가격정보</span>
    <span class="stat-value">{prices_ok:,}</span>
  </div>
</div>

<div class="cat-tab-bar">
  <button class="cat-btn active" data-cat="hotel">호텔 OTA</button>
  {golf_tab_btn}
</div>

<div id="hotel-section">
<div class="filter-section">
  <div class="filter-group">
    <span class="filter-group-label">지역</span>
    <div class="filter-bar">
    {filter_btns}
    </div>
  </div>
  <div class="filter-group">
    <span class="filter-group-label">요일</span>
    <div class="filter-bar">
    {dt_btns}
    <span class="holiday-notice">※ 공휴일: 한국 공휴일 기준</span>
    </div>
  </div>
</div>

{legend_html}

<main class="main">
  <div class="property-grid" id="grid">
{cards_html}
  </div>
</main>
</div>

{golf_section_html}

<footer class="footer">
  소노호텔앤리조트 경쟁사 가격 모니터링&ensp;&middot;&ensp;매일 04:00 자동 업데이트<br>
  <small>각 OTA 기준 30일 내 최저가 (1박, 성인 2인)&ensp;&middot;&ensp;요일 탭은 체크인 날짜 기준</small>
</footer>
<div class="copyright-bar">
  Copyright &copy; GS Team Chanmin Park. All rights reserved.
</div>

<script>{_JS}</script>
</body>
</html>"""


# ── CSS ───────────────────────────────────────────────────────────────────────

_CSS = """
:root {
  --bg:          #0d1117;
  --card-bg:     #161b22;
  --card-header: #21262d;
  --border:      #30363d;
  --text:        #e6edf3;
  --muted:       #7d8590;
  --accent:      #58a6ff;
  --green:       #3fb950;
  --red:         #f85149;
  --yellow:      #e3b341;
  --orange:      #f0883e;
  --c-yanolja:   #ff4081;
  --c-naver:     #03c75a;
  --c-tripcom:   #0066cc;
  --c-yeogi:     #4285f4;
  --c-agoda:     #e85d3e;
  --c-homepage:  #3fb950;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: 'Noto Sans KR', -apple-system, "Apple SD Gothic Neo", "Malgun Gothic",
               BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: var(--bg);
  color: var(--text);
  font-size: 14px;
  line-height: 1.5;
}

/* ── Header ── */
.header {
  background: rgba(22,27,34,.97);
  border-bottom: 1px solid var(--border);
  padding: 14px 20px;
  position: sticky;
  top: 0;
  z-index: 100;
  backdrop-filter: blur(12px);
  -webkit-backdrop-filter: blur(12px);
}
.header-inner {
  display: flex;
  justify-content: space-between;
  align-items: center;
  max-width: 1400px;
  margin: 0 auto;
}
.header-left { display: flex; flex-direction: column; gap: 3px; }
.header-title-wrap { display: flex; align-items: center; gap: 10px; }
.header-title {
  font-family: 'Space Grotesk', 'Noto Sans KR', sans-serif;
  font-size: 22px;
  font-weight: 700;
  color: #ffffff;
  letter-spacing: -.3px;
  line-height: 1;
}
.header-gs {
  background: linear-gradient(135deg, #e3b341 0%, #f0d060 50%, #e3b341 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
  font-weight: 700;
  font-size: 24px;
  letter-spacing: 2px;
}
.header-team-badge {
  display: inline-flex;
  align-items: center;
  background: rgba(227,179,65,.12);
  color: rgba(227,179,65,.7);
  font-family: 'Space Grotesk', sans-serif;
  font-size: 9px;
  font-weight: 500;
  letter-spacing: 1.5px;
  text-transform: uppercase;
  padding: 3px 10px;
  border-radius: 4px;
  border: 1px solid rgba(227,179,65,.2);
  flex-shrink: 0;
}
.header-meta { font-size: 11px; color: var(--muted); margin-top: 1px; }
.header-badge {
  background: var(--green);
  color: #0d1117;
  font-size: 10px;
  font-weight: 800;
  padding: 3px 10px;
  border-radius: 20px;
  letter-spacing: 1.5px;
  flex-shrink: 0;
  animation: blink 2.5s ease-in-out infinite;
}
@keyframes blink { 0%,100%{opacity:1} 50%{opacity:.45} }

/* ── Stats bar ── */
.stats-bar {
  display: flex;
  background: var(--card-header);
  border-bottom: 1px solid var(--border);
  overflow-x: auto;
}
.stat-item {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 10px 24px;
  border-right: 1px solid var(--border);
  min-width: 88px;
  flex-shrink: 0;
}
.stat-item:last-child { border-right: none; }
.stat-label { font-size: 10px; color: var(--muted); letter-spacing: .5px; }
.stat-value { font-size: 22px; font-weight: 700; color: var(--text); font-variant-numeric: tabular-nums; }

/* ── Filter section ── */
.filter-section {
  border-bottom: 1px solid var(--border);
  background: var(--bg);
}
.filter-group {
  display: flex;
  align-items: center;
  padding: 7px 16px;
  border-bottom: 1px solid rgba(48,54,61,.5);
}
.filter-group:last-child { border-bottom: none; }
.filter-group-label {
  font-size: 10px;
  color: var(--muted);
  letter-spacing: .5px;
  width: 28px;
  flex-shrink: 0;
  margin-right: 8px;
}
.filter-bar {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
}
.filter-btn {
  padding: 5px 13px;
  border: 1px solid var(--border);
  border-radius: 20px;
  background: transparent;
  color: var(--muted);
  cursor: pointer;
  font-size: 12px;
  font-family: inherit;
  transition: all .15s;
  white-space: nowrap;
  -webkit-tap-highlight-color: transparent;
  touch-action: manipulation;
  user-select: none;
  -webkit-user-select: none;
}
.filter-btn:hover        { border-color: var(--accent); color: var(--accent); }
.filter-btn.active       { background: var(--accent); border-color: var(--accent); color: #0d1117; font-weight: 700; }
.filter-btn.dt-btn.active { background: var(--yellow); border-color: var(--yellow); color: #0d1117; font-weight: 700; }
.holiday-notice { font-size: 0.75em; color: #888; align-self: center; white-space: nowrap; padding-left: 4px; }

/* ── Category Tab Bar ── */
.cat-tab-bar {
  display: flex;
  gap: 0;
  background: var(--card-header);
  border-bottom: 2px solid var(--border);
  padding: 0 16px;
}
.cat-btn {
  padding: 10px 20px;
  border: none;
  border-bottom: 2px solid transparent;
  background: transparent;
  color: var(--muted);
  cursor: pointer;
  font-size: 13px;
  font-family: inherit;
  font-weight: 600;
  letter-spacing: -.2px;
  transition: all .15s;
  margin-bottom: -2px;
  -webkit-tap-highlight-color: transparent;
}
.cat-btn:hover { color: var(--text); }
.cat-btn.active {
  color: var(--accent);
  border-bottom-color: var(--accent);
}

/* ── Golf ── */
.golf-meta-bar {
  padding: 7px 16px;
  font-size: 11px;
  color: var(--muted);
  background: var(--bg);
  border-bottom: 1px solid var(--border);
}
/* 채널 열 헤더 (호텔 competitor-col 기준: min-width 120px) */
.golf-th-chan   { min-width: 120px; text-align: left; }
/* 경쟁사 열 헤더 (호텔 price-cell 기준: min-width 110px) */
.golf-th-comp   { min-width: 110px; text-align: center; }
th.golf-own-col { color: var(--accent); }
/* 채널 이름 셀 (호텔 competitor-name 기준: max-width 136px, word-break: keep-all) */
td.golf-chan-col {
  color: var(--muted);
  font-size: 12px;
  font-weight: 600;
  word-break: keep-all;
  max-width: 136px;
  vertical-align: middle;
}
/* 가격 셀 공통 */
td.golf-cell {
  text-align: center;
  vertical-align: top;
  padding: 8px 10px;
}
td.golf-own-cell  { background: rgba(88,166,255,.04); }
td.golf-na-cell   { text-align: center; color: var(--muted); vertical-align: middle; }
/* 셀 내부 링크 */
.golf-cell-link   { text-decoration: none; color: inherit; display: block; }
.golf-cell-link:hover .golf-course-label { color: var(--accent); }
/* 코스명 */
.golf-course-label {
  font-size: 11px;
  color: var(--muted);
  margin-bottom: 5px;
  transition: color .15s;
}
/* 주중/주말 가격 행 */
.golf-day-row {
  font-size: 12px;
  line-height: 2;
  font-variant-numeric: tabular-nums;
}
/* 주중/주말 뱃지 */
.golf-day-badge {
  display: inline-block;
  font-size: 10px;
  border-radius: 3px;
  padding: 0 4px;
  margin-right: 4px;
  font-weight: 600;
  vertical-align: middle;
}
.golf-day-badge.wkd  { background: rgba(80,200,120,.15); color: #50c878; }
.golf-day-badge.wknd { background: rgba(255,150,50,.15);  color: #ff9632; }
/* 카트 정보 */
.golf-cart-info { font-size: 10px; color: var(--muted); margin-top: 3px; }
.golf-usd       { color: var(--muted); font-size: 11px; }
.golf-na        { color: var(--muted); }

/* ── Legend bar ── */
.legend-bar {
  padding: 7px 16px;
  font-size: 11px;
  color: var(--muted);
  border-bottom: 1px solid var(--border);
  background: rgba(13,17,23,.8);
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
  line-height: 1.8;
}
.legend-label { font-weight: 600; color: var(--text); font-size: 11px; }
.legend-note  { color: var(--muted); font-size: 11px; }

/* ── Main / Grid ── */
.main { padding: 16px; max-width: 1800px; margin: 0 auto; }
.property-grid {
  display: grid;
  grid-template-columns: 1fr;
  gap: 16px;
  align-items: stretch;
}

/* ── Property card ── */
.property-card {
  background: var(--card-bg);
  border: 1px solid var(--border);
  border-radius: 12px;
  overflow: hidden;
  transition: border-color .2s, box-shadow .2s;
  display: flex;
  flex-direction: column;
}
.table-wrap { flex: 1; }
.channel-section { margin-top: auto; }
.property-card:hover {
  border-color: rgba(88,166,255,.35);
  box-shadow: 0 0 24px rgba(88,166,255,.08);
}
.property-card.hidden { display: none; }
.property-header {
  background: var(--card-header);
  padding: 12px 16px;
  border-bottom: 1px solid var(--border);
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
}
.property-title  { font-size: 14px; font-weight: 700; }
.property-region { font-size: 11px; color: var(--muted); margin-top: 2px; }
.property-stats  { font-size: 11px; color: var(--muted); white-space: nowrap; margin-left: 8px; padding-top: 2px; }

/* ── Table ── */
.table-wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; }
table { width: 100%; border-collapse: collapse; }
th {
  padding: 8px 10px;
  text-align: center;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: .3px;
  border-bottom: 1px solid var(--border);
  background: rgba(0,0,0,.18);
  color: var(--muted);
  white-space: nowrap;
}
th.competitor-col { text-align: left; min-width: 120px; }
th.ota-yanolja  { color: var(--c-yanolja);  }
th.ota-naver    { color: var(--c-naver);    }
th.ota-tripcom  { color: var(--c-tripcom);  }
th.ota-yeogi    { color: var(--c-yeogi);    }
th.ota-agoda    { color: var(--c-agoda);    }
th.ota-homepage { color: var(--c-homepage); }
td {
  padding: 8px 10px;
  border-bottom: 1px solid rgba(48,54,61,.4);
  vertical-align: middle;
}
tr:last-child td { border-bottom: none; }
tr:hover td { background: rgba(88,166,255,.04); }
td.competitor-name {
  font-size: 12px;
  font-weight: 500;
  max-width: 136px;
  line-height: 1.4;
  word-break: keep-all;
}

/* ── Price cell ── */
.price-cell   { text-align: center; min-width: 110px; }
.price-link {
  display: inline-block;
  font-size: 13px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
  text-decoration: none;
  color: var(--text);
  -webkit-tap-highlight-color: transparent;
  transition: opacity .15s;
}
.price-link:hover, .price-link:active { opacity: .7; }
.price-link.own-price { color: var(--accent) !important; }
.price-meta   { font-size: 10px; color: var(--muted); margin-top: 2px; line-height: 1.5; }
.room-type    { font-size: 10px; color: #6e7681; margin-top: 1px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 110px; }
td.no-data    { text-align: center; color: #3a3f4b; font-size: 16px; }
.sold-out-txt { color: #555; font-size: 11px; letter-spacing: .3px; }

/* ── Day-type layers ── */
.dt-layer          { display: none; }
.dt-layer.dt-active { display: block; }

/* ── Own (자사) row ── */
tr.own-row td {
  background: rgba(88,166,255,.10);
  border-bottom: 2px solid rgba(88,166,255,.25);
}
tr.own-row:hover td { background: rgba(88,166,255,.16); }
.own-label {
  font-weight: 700;
  color: var(--accent) !important;
  font-size: 12px;
}
.badge-sono {
  display: inline-block;
  background: var(--accent);
  color: #0d1117;
  font-size: 9px;
  font-weight: 800;
  padding: 1px 6px;
  border-radius: 4px;
  margin-right: 6px;
  letter-spacing: .3px;
  vertical-align: middle;
}

/* ── Price change ── */
.change-up   { color: var(--red);   font-size: 10px; font-weight: 700; }
.change-down { color: var(--green); font-size: 10px; font-weight: 700; }
.change-same { color: var(--muted); font-size: 10px; }
.badge-new {
  background: var(--yellow);
  color: #0d1117;
  font-size: 9px;
  padding: 1px 5px;
  border-radius: 3px;
  font-weight: 700;
  vertical-align: middle;
}

/* ── Promo badge ── */
.badge-promo {
  display: inline-block;
  background: var(--orange);
  color: #0d1117;
  font-size: 9px;
  font-weight: 800;
  padding: 1px 5px;
  border-radius: 4px;
  margin-left: 3px;
  letter-spacing: .2px;
  vertical-align: middle;
}

/* ── Room category badge ── */
.badge-category {
  display: inline-block;
  background: #2a3a4a;
  color: #7db8e8;
  font-size: 9px;
  font-weight: 700;
  padding: 1px 4px;
  border-radius: 3px;
  margin-left: 3px;
  letter-spacing: .1px;
  vertical-align: middle;
  border: 1px solid #3a5068;
}

/* ── Category mismatch badge ── */
.badge-cat-mismatch {
  display: inline-block;
  background: #3d2a00;
  color: #e3a008;
  font-size: 9px;
  font-weight: 800;
  padding: 1px 4px;
  border-radius: 3px;
  margin-left: 3px;
  letter-spacing: .1px;
  vertical-align: middle;
  border: 1px solid #6b4a00;
  cursor: help;
}

/* ── 별점 인라인 표시 (가격 바로 옆) ── */
.inline-rating {
  font-size: 10px;
  font-weight: 600;
  color: #d29922;
  margin-left: 3px;
  vertical-align: middle;
  white-space: nowrap;
}

/* ── Footer ── */
.footer {
  padding: 20px 24px;
  text-align: center;
  color: var(--muted);
  font-size: 12px;
  line-height: 2;
  border-top: 1px solid var(--border);
  margin-top: 8px;
}
.copyright-bar {
  background: #0a0d11;
  color: rgba(255,255,255,.55);
  text-align: center;
  font-size: 12px;
  padding: 14px 24px;
  border-top: 1px solid var(--border);
  letter-spacing: .3px;
}

/* ── Responsive ── */
@media (max-width: 768px) {
  .property-grid { gap: 12px; }
  .main { padding: 10px; }
  .header { padding: 12px 14px; }
  .header-title { font-size: 16px; }
  .header-gs { font-size: 18px; }
  .header-team-badge { display: none; }
  .header-meta  { font-size: 10px; }
  .stat-item { padding: 8px 14px; min-width: 72px; }
  .stat-value { font-size: 18px; }
  th, td { padding: 6px 7px; }
  td.competitor-name { font-size: 11px; max-width: 90px; }
  .price-link { font-size: 12px; }
  .price-cell { min-width: 90px; }
  .room-type  { max-width: 90px; }
}
@media (max-width: 400px) {
  .filter-btn { padding: 4px 10px; font-size: 11px; }
  .property-title { font-size: 13px; }
  .header-title { font-size: 14px; }
  .header-gs { font-size: 16px; }
  .legend-bar { font-size: 10px; }
}
@media (min-width: 768px) {
  .property-grid { grid-template-columns: repeat(2, 1fr); }
}
@media (min-width: 1200px) {
  .property-grid { grid-template-columns: repeat(3, 1fr); }
}
@media (min-width: 1920px) {
  .main { max-width: 1900px; padding: 16px 40px; }
  .header-inner { max-width: 1900px; }
  .property-grid { grid-template-columns: repeat(3, 1fr); }
}

/* ── Channel Sales Section ── */
.channel-section {
  border-top: 1px solid var(--border);
}
.channel-toggle {
  width: 100%;
  background: none;
  border: none;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 9px 14px;
  color: var(--muted);
  font-size: 12px;
  font-family: inherit;
  text-align: left;
  transition: background .15s, color .15s;
  touch-action: manipulation;
}
.channel-toggle:hover { background: rgba(255,255,255,.04); color: var(--text); }
.channel-toggle-label { font-weight: 600; color: var(--text); font-size: 12px; }
.channel-toggle-meta {
  background: rgba(88,166,255,.15);
  color: var(--accent);
  font-size: 10px;
  font-weight: 600;
  padding: 1px 7px;
  border-radius: 10px;
  border: 1px solid rgba(88,166,255,.25);
}
.channel-arrow {
  margin-left: auto;
  font-size: 10px;
  color: var(--muted);
  transition: transform .2s;
}
.channel-section.open .channel-arrow { transform: rotate(180deg); }
.channel-body {
  display: none;
  padding: 0 14px 12px;
}
.channel-section.open .channel-body { display: block; }
.channel-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}
.channel-table th {
  background: rgba(255,255,255,.04);
  color: var(--muted);
  font-weight: 600;
  padding: 5px 8px;
  border-bottom: 1px solid var(--border);
  white-space: nowrap;
}
.ch-th-name { text-align: left; }
.ch-th-num, .ch-th-growth { text-align: right; }
.channel-table td { padding: 4px 8px; border-bottom: 1px solid rgba(48,54,61,.6); }
.ch-name { color: var(--text); font-weight: 500; }
.ch-num  { text-align: right; font-variant-numeric: tabular-nums; color: var(--text); }
.ch-prev { color: var(--muted); }
.ch-growth { text-align: right; font-variant-numeric: tabular-nums; font-weight: 600; }
.ch-up   { color: var(--green); }
.ch-dn   { color: var(--red); }
.ch-na   { color: var(--muted); }
.ch-total-row td {
  font-weight: 700;
  border-top: 1px solid var(--border);
  border-bottom: none;
  color: var(--text);
  padding-top: 6px;
}

/* ── Budget 달성률 게이지 섹션 ── */
.bgt-section {
  padding: 10px 8px 6px;
  border-bottom: 1px solid var(--border);
}
.bgt-title {
  font-size: 10px;
  font-weight: 700;
  letter-spacing: .06em;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 4px;
  text-align: center;
}
.gauge-row {
  display: flex;
  justify-content: space-around;
  align-items: flex-end;
  gap: 4px;
}
.gauge-wrap {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  min-width: 0;
}
.gauge-label {
  font-size: 10px;
  font-weight: 700;
  color: var(--muted);
  margin-bottom: 2px;
  text-align: center;
}
.gauge-svg {
  width: 100%;
  max-width: 110px;
  height: auto;
  overflow: visible;
}

/* ── Homepage section (자사몰 객실가격 토글) ── */
.homepage-section {
  border-bottom: 1px solid var(--border);
}
.homepage-toggle {
  width: 100%;
  background: none;
  border: none;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 9px 14px;
  color: var(--muted);
  font-size: 12px;
  font-family: inherit;
  text-align: left;
  transition: background .15s, color .15s;
  touch-action: manipulation;
}
.homepage-toggle:hover { background: rgba(63,185,80,.06); color: var(--c-homepage); }
.homepage-toggle-label { font-weight: 600; color: var(--c-homepage); font-size: 12px; }
.homepage-arrow {
  margin-left: auto;
  font-size: 10px;
  color: var(--muted);
  transition: transform .2s;
}
.homepage-section.open .homepage-arrow { transform: rotate(180deg); }
.homepage-body {
  display: none;
  padding: 0 14px 12px;
}
.homepage-section.open .homepage-body { display: block; }
.homepage-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}
.homepage-table th {
  background: rgba(63,185,80,.08);
  color: var(--c-homepage);
  font-weight: 600;
  padding: 5px 8px;
  border-bottom: 1px solid rgba(63,185,80,.2);
  white-space: nowrap;
}
.hp-th-room { text-align: left; }
.hp-th-price, .hp-th-meta { text-align: right; }
.homepage-table td { padding: 4px 8px; border-bottom: 1px solid rgba(48,54,61,.6); }
.homepage-table tr:last-child td { border-bottom: none; }
.hp-room  { color: var(--text); font-weight: 500; }
.hp-price { text-align: right; font-variant-numeric: tabular-nums; color: var(--c-homepage); font-weight: 700; }
.hp-meta  { text-align: right; font-size: 10px; color: var(--muted); }

/* ── 브래드닷컴 객실요금 Section ── */
.fit-section {
  border-bottom: 1px solid var(--border);
}
.fit-toggle {
  width: 100%;
  background: none;
  border: none;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 9px 14px;
  color: var(--muted);
  font-size: 12px;
  font-family: inherit;
  text-align: left;
  transition: background .15s, color .15s;
  touch-action: manipulation;
}
.fit-toggle:hover { background: rgba(227,179,65,.06); color: var(--yellow); }
.fit-toggle-label { font-weight: 600; color: var(--yellow); font-size: 12px; }
.fit-toggle-meta {
  background: rgba(227,179,65,.15);
  color: var(--yellow);
  font-size: 10px;
  font-weight: 600;
  padding: 1px 7px;
  border-radius: 10px;
  border: 1px solid rgba(227,179,65,.25);
}
.fit-arrow {
  margin-left: auto;
  font-size: 10px;
  color: var(--muted);
  transition: transform .2s;
}
.fit-section.open .fit-arrow { transform: rotate(180deg); }
.fit-body {
  display: none;
  padding: 0 14px 12px;
}
.fit-section.open .fit-body { display: block; }
.fit-season {
  background: rgba(227,179,65,.15);
  color: var(--yellow);
  font-size: 9px;
  font-weight: 600;
  padding: 1px 5px;
  border-radius: 3px;
  border: 1px solid rgba(227,179,65,.2);
}
"""


# ── JavaScript (iOS Safari 호환) ──────────────────────────────────────────────
# NodeList.forEach는 구형 iOS에서 미지원 → Array.prototype.slice.call 사용
# dataset 대신 getAttribute 사용 (호환성)
# click + touchend 중복 방지 → click만 사용, touch-action: manipulation으로 딜레이 제거

_JS = """
(function () {
  function toArr(nl) { return Array.prototype.slice.call(nl); }

  // ── 카테고리 탭 (호텔 / 골프) ─────────────────────────────────────────────
  var catBtns      = toArr(document.querySelectorAll('.cat-btn'));
  var hotelSection = document.getElementById('hotel-section');
  var golfSection  = document.getElementById('golf-section');

  catBtns.forEach(function (btn) {
    btn.addEventListener('click', function () {
      catBtns.forEach(function (b) { b.classList.remove('active'); });
      btn.classList.add('active');
      var cat = btn.getAttribute('data-cat');
      if (hotelSection) hotelSection.style.display = (cat === 'hotel') ? '' : 'none';
      if (golfSection)  golfSection.style.display  = (cat === 'golf')  ? '' : 'none';
    });
  });

  // ── 지역 필터 ─────────────────────────────────────────────────────────────
  var regionBtns = toArr(document.querySelectorAll('.filter-btn[data-region]'));
  var cards      = toArr(document.querySelectorAll('.property-card'));

  regionBtns.forEach(function (btn) {
    btn.addEventListener('click', function () {
      regionBtns.forEach(function (b) { b.classList.remove('active'); });
      btn.classList.add('active');
      var r = btn.getAttribute('data-region');
      cards.forEach(function (c) {
        var show = (r === '전체') || (c.getAttribute('data-region') === r);
        c.classList.toggle('hidden', !show);
      });
    });
  });

  // ── 요일 필터 ─────────────────────────────────────────────────────────────
  var dtBtns = toArr(document.querySelectorAll('.dt-btn'));
  var layers = toArr(document.querySelectorAll('.dt-layer'));

  function activateDt(dt) {
    var i;
    for (i = 0; i < dtBtns.length; i++) {
      dtBtns[i].classList.toggle('active', dtBtns[i].getAttribute('data-dt') === dt);
    }
    for (i = 0; i < layers.length; i++) {
      layers[i].classList.toggle('dt-active', layers[i].getAttribute('data-dt') === dt);
    }
  }

  for (var j = 0; j < dtBtns.length; j++) {
    (function (btn) {
      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        activateDt(btn.getAttribute('data-dt'));
      });
    })(dtBtns[j]);
  }

  // ── 채널별 판매객실수 토글 ────────────────────────────────────────────────
  var chToggles = toArr(document.querySelectorAll('.channel-toggle'));
  chToggles.forEach(function (btn) {
    btn.addEventListener('click', function () {
      var section = btn.parentElement;
      section.classList.toggle('open');
    });
  });

  // ── 자사몰 객실가격 토글 ──────────────────────────────────────────────────
  var hpToggles = toArr(document.querySelectorAll('.homepage-toggle'));
  hpToggles.forEach(function (btn) {
    btn.addEventListener('click', function () {
      var section = btn.parentElement;
      section.classList.toggle('open');
    });
  });

  // ── 브래드닷컴 객실요금 토글 ─────────────────────────────────────────────────────────
  var fitToggles = toArr(document.querySelectorAll('.fit-toggle'));
  fitToggles.forEach(function (btn) {
    btn.addEventListener('click', function () {
      var section = btn.parentElement;
      section.classList.toggle('open');
    });
  });
})();
"""


# ── 단독 실행: 최신 실제 CSV로 대시보드 재생성 ────────────────────────────────────
# python3 dashboard_generator.py [csv_path]

if __name__ == "__main__":
    import glob
    import subprocess

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # CLI에서 CSV 경로를 직접 지정하거나, exports/ 에서 최신 파일 자동 선택
    import sys
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        csvs = sorted(glob.glob("exports/sono_competitor_prices_*.csv"))
        # _yanolja_only 같은 suffix 파일 제외
        csvs = [c for c in csvs if not any(s in c for s in ("_yanolja_only", "_agoda", "_yeogi"))]
        if not csvs:
            logger.error("exports/ 에 CSV 파일이 없습니다.")
            sys.exit(1)
        csv_path = csvs[-1]

    logger.info(f"CSV 로드: {csv_path}")
    df_today = pd.read_csv(csv_path, encoding="utf-8-sig")
    prev_df  = load_previous_df("./exports")
    golf_df  = load_golf_df("./exports")

    out = generate_dashboard(df_today, "dashboard/index.html", prev_df=prev_df, golf_df=golf_df)
    print(f"\n대시보드 생성 완료: {out}")
    subprocess.run(["open", out])
