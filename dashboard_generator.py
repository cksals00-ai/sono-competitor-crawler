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
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# ── OTA 설정 ─────────────────────────────────────────────────────────────────
OTA_ORDER   = ["야놀자", "여기어때", "Booking.com", "Agoda"]
OTA_SHORT   = {"야놀자": "야놀자", "여기어때": "여기어때", "Booking.com": "Booking", "Agoda": "Agoda", "자사홈": "자사홈"}
OTA_URL_KEY = {"야놀자": "yanolja_url", "여기어때": "yeogiuh_url", "Booking.com": "booking_url", "Agoda": "agoda_url", "자사홈": ""}
OTA_CLASS   = {"야놀자": "yanolja",  "여기어때": "yeogi",       "Booking.com": "booking",     "Agoda": "agoda",  "자사홈": "homepage"}

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
    "해외": ["베트남", "Vietnam"],
}


# ── 설정·데이터 로드 ──────────────────────────────────────────────────────────

def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


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
            return df
        except Exception as e:
            logger.warning(f"전일 데이터 로드 실패: {e}")
    return pd.DataFrame()


# ── 메인 공개 함수 ────────────────────────────────────────────────────────────

def generate_dashboard(
    df: pd.DataFrame,
    output_path: str = "dashboard/index.html",
    config_path: str = "config.yaml",
    prev_df: pd.DataFrame = None,
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

    # 요일별 summary 빌드 (최저가 + 메타)
    summaries = {dt: _build_price_summary(df, dt) for dt in DAY_TYPES}

    # 전일 기준: 같은 체크인일의 어제 크롤링 가격
    has_prev      = prev_df is not None and not prev_df.empty
    prev_per_date = _build_per_date_prices(prev_df) if has_prev else {}

    # 전일 날짜 레이블 (범례용)
    prev_date = ""
    if has_prev and "crawled_at" in prev_df.columns:
        raw = str(prev_df["crawled_at"].max())[:10]
        try:
            pd_ = datetime.strptime(raw, "%Y-%m-%d")
            prev_date = f"{pd_.month}/{pd_.day}"
        except Exception:
            prev_date = raw

    crawled_at = (
        str(df["crawled_at"].max())
        if not df.empty and "crawled_at" in df.columns
        else ""
    )

    # 경쟁사별 OTA 별점 요약 (review_score 컬럼이 있을 때만)
    review_summary = _build_review_summary(df)

    html = _render_html(df, cfg, summaries, prev_per_date, crawled_at, prev_date, review_summary)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    abs_path = str(Path(output_path).resolve())
    logger.info(f"대시보드 생성: {abs_path}")
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
    """프로모션 접두어 제거 후 16자 이내로 반환"""
    if not room_type:
        return ""
    rt = room_type
    for pfx in PROMO_PREFIXES:
        rt = rt.replace(pfx, "").strip()
    if len(rt) > 16:
        rt = rt[:15] + "…"
    return rt


def _build_price_summary(df: pd.DataFrame, day_type: str = "전체") -> dict:
    """
    DataFrame → {(property_name, competitor_name, ota): info_dict}
    info_dict keys: min_price, min_date, sold_out, room_type, is_promo
    """
    if df is None or df.empty:
        return {}
    needed = {"property_name", "competitor_name", "ota", "price", "checkin_date"}
    if not needed.issubset(df.columns):
        return {}

    df_ok = df[df["error"].fillna("") == ""].copy() if "error" in df.columns else df.copy()

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
                "room_type": "", "is_promo": False,
            }
        else:
            row  = avail.loc[avail["price"].idxmin()]
            rt   = str(row.get("room_type", "") or "")
            is_p = bool(row.get("is_promo", False)) or _is_promo(rt)
            result[(prop, comp, ota)] = {
                "min_price": int(row["price"]),
                "min_date":  str(row["checkin_date"])[:10],
                "sold_out":  False,
                "room_type": rt,
                "is_promo":  is_p,
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
    result = {}
    for (prop, comp, ota, date), grp in df_ok.groupby(
        ["property_name", "competitor_name", "ota", "checkin_date"], sort=False
    ):
        avail = grp[grp["price"].fillna(0) > 0]
        if not avail.empty:
            result[(prop, comp, ota, str(date)[:10])] = int(avail["price"].min())
    return result


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
    if ota == "Booking.com":
        return (
            f"{base_url}?checkin={checkin}&checkout={checkout}"
            "&group_adults=2&no_rooms=1&lang=ko"
        )
    if ota == "Agoda":
        return f"{base_url}?checkIn={checkin}&checkOut={checkout}&adults=2&rooms=1"
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
) -> str:
    """요일별 레이어를 포함한 <td> 반환. 가격 클릭 시 해당 OTA 딥링크로 이동."""
    if not ota_url:
        return '<td class="price-cell no-data">&#8212;</td>'

    key    = (prop_name, comp_name, ota)
    layers = []

    for dt in DAY_TYPES:
        active_cls = " dt-active" if dt == "전체" else ""
        summary    = summaries.get(dt, {})

        if key not in summary:
            # URL은 있지만 해당 요일 데이터 없음
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
                         else '<span class="badge-new">신규</span>'

            price_cls  = "own-price" if is_own else ""
            promo_html = ' <span class="badge-promo">특가</span>' if is_promo else ""

            clean_rt   = _clean_room_type(room_type)
            room_html  = f'<div class="room-type">{clean_rt}</div>' if clean_rt else ""

            link_url   = _make_ota_link_url(ota_url, ota, checkin)

            inner = (
                f'<a href="{link_url}" target="_blank" rel="noopener" '
                f'class="price-link {price_cls}">'
                f'{_fmt_price(price)}{promo_html}</a>'
                f'{room_html}'
                f'<div class="price-meta">{date_disp} {change}</div>'
            )

        layers.append(
            f'<div class="dt-layer{active_cls}" data-dt="{dt}">{inner}</div>'
        )

    return f'<td class="price-cell">{"".join(layers)}</td>'


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
    channels   = meta.get("channels", list(entry.get("channels", {}).keys()))
    ch_data    = entry.get("channels", {})
    total      = entry.get("total", {})

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

    rows_html = "\n".join(rows)
    return f"""\
<div class="channel-section">
  <button class="channel-toggle" type="button">
    <span class="channel-toggle-label">채널별 판매객실수</span>
    <span class="channel-toggle-meta">{label}</span>
    <span class="channel-arrow">&#9660;</span>
  </button>
  <div class="channel-body">
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
                         else '<span class="badge-new">신규</span>'
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


def _render_property_card(
    prop: dict,
    summaries: dict,        # {day_type: summary_dict}
    prev_per_date: dict,    # {(prop, comp, ota, checkin_date): price}
    df: pd.DataFrame,
    review_summary: dict = None,  # {comp_name: {ota: score}}
    channel_data: dict = None,    # _load_channel_data() 결과
) -> str:
    prop_name    = prop["name"]
    region_str   = prop.get("region", "")
    region_label = _get_region(region_str)
    competitors  = prop.get("competitors", [])
    own_urls     = prop.get("own_urls", {})
    has_own      = any(own_urls.get(k, "") for k in ("yanolja_url", "yeogiuh_url", "booking_url"))
    review_summary = review_summary or {}

    if not df.empty and "property_name" in df.columns:
        prop_df  = df[df["property_name"] == prop_name]
        ok_count = int((prop_df["error"].fillna("") == "").sum()) if "error" in prop_df.columns else len(prop_df)
    else:
        ok_count = 0

    has_reviews = bool(review_summary)

    ota_ths = "".join(
        f'<th class="ota-{OTA_CLASS[ota]}">{OTA_SHORT[ota]}</th>'
        for ota in OTA_ORDER
    )
    # 별점 헤더 (별점 데이터 있을 때만 추가)
    review_ths = (
        '<th class="review-col" title="OTA 별점 (10점 만점 · 수집된 OTA만 표시)">별점</th>'
        if has_reviews else ""
    )

    rows = []

    # ── 자사 가격 행 ──────────────────────────────────────────────────────────
    if has_own:
        own_cells = "".join(
            _render_price_cell(
                prop_name, prop_name, ota,
                summaries, prev_per_date,
                own_urls.get(OTA_URL_KEY[ota], ""),
                is_own=True,
            )
            for ota in OTA_ORDER
        )
        review_cell = _render_review_cell(prop_name, review_summary) if has_reviews else ""
        rows.append(
            f'<tr class="own-row">'
            f'<td class="competitor-name own-label">'
            f'<span class="badge-sono">자사</span>{prop_name}'
            f'</td>'
            f'{own_cells}'
            f'{review_cell}'
            f'</tr>'
        )

    # ── 경쟁사 행 ─────────────────────────────────────────────────────────────
    for comp in competitors:
        comp_name = comp["name"]
        cells = "".join(
            _render_price_cell(
                prop_name, comp_name, ota,
                summaries, prev_per_date,
                comp.get(OTA_URL_KEY[ota], ""),
            )
            for ota in OTA_ORDER
        )
        review_cell = _render_review_cell(comp_name, review_summary) if has_reviews else ""
        rows.append(
            f'<tr><td class="competitor-name">{comp_name}</td>{cells}{review_cell}</tr>'
        )

    rows_html    = "\n          ".join(rows)
    comp_label   = f"{len(competitors)}개 경쟁사"
    if has_own:
        comp_label = "자사 포함 · " + comp_label

    channel_html  = _render_channel_section(prop_name, channel_data)
    homepage_html = _render_homepage_section(prop_name, df, prev_per_date)

    return f"""\
<div class="property-card" data-region="{region_label}">
  <div class="property-header">
    <div>
      <div class="property-title">{prop_name}</div>
      <div class="property-region">{region_str}</div>
    </div>
    <div class="property-stats">{comp_label}&nbsp;&middot;&nbsp;{ok_count:,}건</div>
  </div>
{homepage_html}
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th class="competitor-col">구분</th>
        {ota_ths}{review_ths}
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


def _render_html(
    df: pd.DataFrame, cfg: dict,
    summaries: dict, prev_per_date: dict,
    crawled_at: str, prev_date: str = "",
    review_summary: dict = None,
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

    cards_html = "\n\n".join(
        _render_property_card(p, summaries, prev_per_date, df, review_summary, channel_data)
        for p in properties
    )

    # 지역 필터 버튼
    regions     = ["전체"] + sorted(set(_get_region(p.get("region", "")) for p in properties))
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
  &emsp;&middot;&emsp;<span class="legend-note">※ 객실타입·가격은 해당 OTA 최저가 기준</span>
</div>"""

    crawled_disp = crawled_at[:16] if crawled_at else "&#8212;"
    gen_time     = datetime.now().strftime("%Y-%m-%d %H:%M")

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
  <title>소노 경쟁사 모니터링 | GS Team</title>
  <style>{_CSS}</style>
</head>
<body>

<header class="header">
  <div class="header-inner">
    <div class="header-left">
      <div class="header-title-wrap">
        <span class="header-title">소노 경쟁사 모니터링</span>
        <span class="header-team-badge">GS Team</span>
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
    </div>
  </div>
</div>

{legend_html}

<main class="main">
  <div class="property-grid" id="grid">
{cards_html}
  </div>
</main>

<footer class="footer">
  소노호텔앤리조트 경쟁사 가격 모니터링&ensp;&middot;&ensp;매일 07:00 자동 업데이트<br>
  <small>각 OTA 기준 30일 내 최저가 (1박, 성인 2인)&ensp;&middot;&ensp;요일 탭은 체크인 날짜 기준</small><br>
  <small class="copyright">&copy; GS Alfred Park</small>
</footer>

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
  --c-yeogi:     #4285f4;
  --c-booking:   #5cb8ff;
  --c-agoda:     #e85d3e;
  --c-homepage:  #3fb950;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, "Apple SD Gothic Neo", "Malgun Gothic",
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
  font-size: 20px;
  font-weight: 800;
  color: #ffffff;
  letter-spacing: -.5px;
  line-height: 1;
}
.header-team-badge {
  display: inline-flex;
  align-items: center;
  background: linear-gradient(135deg, #238636 0%, #1a7f37 100%);
  color: #ffffff;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: .8px;
  text-transform: uppercase;
  padding: 3px 9px;
  border-radius: 20px;
  border: 1px solid rgba(255,255,255,.15);
  box-shadow: 0 1px 4px rgba(0,0,0,.3);
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
.main { padding: 16px; max-width: 1400px; margin: 0 auto; }
.property-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
  gap: 16px;
}

/* ── Property card ── */
.property-card {
  background: var(--card-bg);
  border: 1px solid var(--border);
  border-radius: 12px;
  overflow: hidden;
  transition: border-color .2s, box-shadow .2s;
}
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
th.ota-yeogi    { color: var(--c-yeogi);    }
th.ota-booking  { color: var(--c-booking);  }
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

/* ── Review / 별점 ── */
.review-col   { text-align: center; min-width: 80px; font-size: 11px; color: var(--muted); }
.review-cell  { text-align: center; padding: 4px 6px; white-space: nowrap; }
.review-na    { color: #3a3f4b; }
.review-badge {
  display: inline-block;
  font-size: 10px;
  font-weight: 700;
  padding: 2px 5px;
  border-radius: 4px;
  margin: 1px 0;
  letter-spacing: .2px;
}
.review-high  { background: rgba(46,160,67,.20); color: #3fb950; }
.review-mid   { background: rgba(210,153,34,.20); color: #d29922; }
.review-low   { background: rgba(248,81,73,.20);  color: #f85149; }

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
.copyright { font-size: 11px; color: #444; letter-spacing: .2px; }

/* ── Responsive ── */
@media (max-width: 768px) {
  .property-grid { grid-template-columns: 1fr; gap: 12px; }
  .main { padding: 10px; }
  .header { padding: 12px 14px; }
  .header-title { font-size: 16px; }
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
  .legend-bar { font-size: 10px; }
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
"""


# ── JavaScript (iOS Safari 호환) ──────────────────────────────────────────────
# NodeList.forEach는 구형 iOS에서 미지원 → Array.prototype.slice.call 사용
# dataset 대신 getAttribute 사용 (호환성)
# click + touchend 중복 방지 → click만 사용, touch-action: manipulation으로 딜레이 제거

_JS = """
(function () {
  function toArr(nl) { return Array.prototype.slice.call(nl); }

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
})();
"""


# ── 단독 실행: 더미 데이터로 대시보드 생성 ──────────────────────────────────────

if __name__ == "__main__":
    import random
    import subprocess

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    cfg_data = load_config()
    random.seed(42)
    today = datetime.today()

    ROOM_TYPES_NORMAL = ["스탠다드 더블", "디럭스 트윈", "슈페리어 오션뷰", "프리미어 더블", "스위트", "패밀리 룸"]
    ROOM_TYPES_PROMO  = ["[오픈런] 스탠다드 더블", "★특가★ 디럭스", "특가 패밀리룸"]

    rows, prev_rows = [], []

    for prop in cfg_data["properties"]:
        is_overseas = "베트남" in prop.get("region", "")
        price_min, price_max = (150000, 700000) if is_overseas else (80000, 480000)

        own_urls = prop.get("own_urls", {})
        for ota_name, url_key in [("야놀자", "yanolja_url"), ("여기어때", "yeogiuh_url"), ("Booking.com", "booking_url")]:
            if not own_urls.get(url_key):
                continue
            for day in range(1, 15):
                checkin  = (today + timedelta(days=day)).strftime("%Y-%m-%d")
                checkout = (today + timedelta(days=day + 1)).strftime("%Y-%m-%d")
                wd       = (today + timedelta(days=day)).weekday()
                surcharge = 1.25 if wd in (4, 5) else 1.0
                price     = int(random.randint(price_min, price_max) * surcharge // 1000 * 1000)
                is_p      = random.random() < 0.15
                rt        = random.choice(ROOM_TYPES_PROMO if is_p else ROOM_TYPES_NORMAL)
                row = {
                    "crawled_at": today.strftime("%Y-%m-%d %H:%M:%S"),
                    "property_name": prop["name"], "property_id": prop["id"],
                    "competitor_name": prop["name"], "ota": ota_name,
                    "checkin_date": checkin, "checkout_date": checkout,
                    "room_type": rt, "price": price, "currency": "KRW",
                    "availability": "available", "url": own_urls[url_key],
                    "error": "", "is_own": True, "is_promo": is_p,
                }
                rows.append(row)
                prev_price = max(50000, int(price * random.uniform(0.88, 1.12) // 1000 * 1000))
                prev_rows.append({**row, "price": prev_price})

        for comp in prop["competitors"]:
            for ota_name, url_key in [("야놀자", "yanolja_url"), ("여기어때", "yeogiuh_url"), ("Booking.com", "booking_url")]:
                if not comp.get(url_key):
                    continue
                for day in range(1, 15):
                    checkin  = (today + timedelta(days=day)).strftime("%Y-%m-%d")
                    checkout = (today + timedelta(days=day + 1)).strftime("%Y-%m-%d")
                    wd       = (today + timedelta(days=day)).weekday()
                    surcharge = 1.3 if wd in (4, 5) else 1.0
                    price     = int(random.randint(price_min, price_max) * surcharge // 1000 * 1000)
                    is_p      = random.random() < 0.12
                    rt        = random.choice(ROOM_TYPES_PROMO if is_p else ROOM_TYPES_NORMAL)
                    row = {
                        "crawled_at": today.strftime("%Y-%m-%d %H:%M:%S"),
                        "property_name": prop["name"], "property_id": prop["id"],
                        "competitor_name": comp["name"], "ota": ota_name,
                        "checkin_date": checkin, "checkout_date": checkout,
                        "room_type": rt, "price": price, "currency": "KRW",
                        "availability": "available", "url": comp[url_key],
                        "error": "", "is_own": False, "is_promo": is_p,
                    }
                    rows.append(row)
                    prev_price = max(50000, int(price * random.uniform(0.88, 1.12) // 1000 * 1000))
                    prev_rows.append({**row, "price": prev_price})

    df_today = pd.DataFrame(rows)
    df_prev  = pd.DataFrame(prev_rows)
    out = generate_dashboard(df_today, "dashboard/index.html", prev_df=df_prev)
    print(f"\n대시보드 생성 완료: {out}")
    subprocess.run(["open", out])
