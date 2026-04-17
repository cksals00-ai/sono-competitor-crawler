"""
골프장 그린피 크롤러 v2
작성: 2026-04-18

채널별 구현 상태:
  ✅ 몽키트래블 (MonkeyTravel) : JSON REST API — 검증 완료
  ✅ AGL (Tiger Booking)       : requests 기반 listing 최저가 — 검증 완료
                                 (날짜별 API 미노출 → 일일 최저가 방식)
  ✅ KKday                     : 상품 페이지 JSON-LD 파싱 — 검증 완료
                                 (망길라오 156010, 탈로포포 156016)

출력 DataFrame 컬럼:
  crawled_at, property_name, property_id, competitor_name, channel,
  course_name, holes, play_date, day_of_week, time_of_day,
  green_fee_krw, green_fee_usd, cart_included, caddy_included,
  url, error, is_own

환율 적용:
  run_golf_crawl() 완료 후 green_fee_usd 컬럼 자동 채움
  소스: open.er-api.com/v6/latest/USD (일 1회 갱신 캐시)
"""

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

import requests
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

MONKEY_BASE = "https://www.monkeytravel.com"
AGL_BASE    = "https://www.tigerbooking.com"
KKDAY_BASE  = "https://www.kkday.com"

_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Accept": "application/json, text/plain, */*",
}

_TIME_OF_DAY_KO = {
    "Morning":   "오전",
    "Afternoon": "오후",
    "Twilight":  "트와일라잇",
    "Night":     "야간",
}

# AGL·KKday는 날짜 필터 없이 일일 최저가만 제공 → 날짜 루프 불필요
_DATE_INDEPENDENT_CHANNELS = {"AGL", "KKday"}


# ---------------------------------------------------------------------------
# 데이터 모델
# ---------------------------------------------------------------------------

@dataclass
class GolfPriceRecord:
    crawled_at:     str
    property_name:  str     # 소노 사업장명 (예: 소노펠리체CC 괌 망길라오)
    property_id:    str     # 사업장 ID
    competitor_name: str    # 경쟁사명 ("자사" if own)
    channel:        str     # 몽키트래블 / AGL / KKday
    course_name:    str     # 골프장 실제 이름 (예: 망길라오 골프 클럽)
    holes:          int     # 홀수 (18)
    play_date:      str     # YYYY-MM-DD
    day_of_week:    str     # 주중 / 주말
    time_of_day:    str     # 오전 / 오후 / 트와일라잇 / 야간 / 최저가
    green_fee_krw:  int     # 1인 그린피 (KRW, 0 if unavailable)
    green_fee_usd:  float   # 1인 그린피 (USD, 0 if unavailable)
    cart_included:  bool    # 카트비 포함 여부
    caddy_included: bool    # 캐디비 포함 여부
    url:            str
    error:          str = ""
    is_own:         bool = False


# ---------------------------------------------------------------------------
# 설정 로드
# ---------------------------------------------------------------------------

def load_golf_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# 환율 조회 (일일 캐시)
# ---------------------------------------------------------------------------

_EXCHANGE_RATE_CACHE: dict = {}   # {"rates": {KRW: ..., VND: ...}, "ts": datetime}
_EXCHANGE_RATE_TTL_HOURS = 12     # 12시간 캐시


def get_exchange_rates() -> dict:
    """
    USD 기준 환율 반환 (KRW, VND, JPY 등 포함).

    Returns:
        dict: {"KRW": 1478.0, "VND": 26247.0, ...}  USD 대비 비율
              조회 실패 시 빈 dict (변환 건너뜀)

    소스: open.er-api.com/v6/latest/USD (무료, 일 1회 갱신)
    폴백: fawazahmed0/currency-api
    """
    global _EXCHANGE_RATE_CACHE

    # 캐시 유효성 확인
    cached_ts = _EXCHANGE_RATE_CACHE.get("ts")
    if cached_ts and (datetime.now() - cached_ts).total_seconds() < _EXCHANGE_RATE_TTL_HOURS * 3600:
        return _EXCHANGE_RATE_CACHE.get("rates", {})

    # 1차: open.er-api.com
    try:
        r = requests.get(
            "https://open.er-api.com/v6/latest/USD",
            headers={"Accept": "application/json"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("result") == "success":
            rates = data["rates"]
            _EXCHANGE_RATE_CACHE = {"rates": rates, "ts": datetime.now()}
            logger.info(f"[환율] open.er-api.com 로드: 1 USD = {rates.get('KRW', '?')} KRW")
            return rates
    except Exception as e:
        logger.warning(f"[환율] open.er-api.com 실패: {e}")

    # 2차: fawazahmed0/currency-api (폴백)
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        r = requests.get(
            f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{today}/v1/currencies/usd.json",
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        usd_rates = data.get("usd", {})
        # 통화 코드를 대문자로 변환
        rates = {k.upper(): v for k, v in usd_rates.items()}
        _EXCHANGE_RATE_CACHE = {"rates": rates, "ts": datetime.now()}
        logger.info(f"[환율] fawazahmed0 폴백 로드: 1 USD = {rates.get('KRW', '?')} KRW")
        return rates
    except Exception as e:
        logger.warning(f"[환율] fawazahmed0 폴백도 실패: {e}")

    return {}


def krw_to_usd(krw: int, rates: dict) -> float:
    """KRW → USD 변환. rates 없으면 0.0 반환"""
    krw_rate = rates.get("KRW", 0)
    if not krw_rate or not krw:
        return 0.0
    return round(krw / krw_rate, 2)


def usd_to_krw(usd: float, rates: dict) -> int:
    """USD → KRW 변환. rates 없으면 0 반환"""
    krw_rate = rates.get("KRW", 0)
    if not krw_rate or not usd:
        return 0
    return int(round(usd * krw_rate))


# ---------------------------------------------------------------------------
# 몽키트래블 크롤러
# ---------------------------------------------------------------------------
#
# API: POST https://www.monkeytravel.com/api/search/golfTeeoffPrice.php
# Body (JSON):
#   checkInDate, authToken, memberSaleCurrency, memberCodePayCurrency,
#   priceSite, adultCount, translateLang, hole ("18H"), product_id
#
# Response: { result: bool, data: [ {etc:{...}, timeData:[...]} ] }
#   data[i].etc.dayOfWeek              : "weekday" | "weekend"
#   data[i].etc.breakdownDisplay       : 명세 breakdown (detailList, totalPrice)
#   data[i].etc.includeExclude.include : "GreenFee,CartFee" 등
#   data[i].timeData[0].golfTimeOfDay  : "Morning" | "Afternoon" | "Twilight" | "Night"
# ---------------------------------------------------------------------------

_MONKEY_PRICE_URL = f"{MONKEY_BASE}/api/search/golfTeeoffPrice.php"
_MONKEY_HEADERS = {
    **_HTTP_HEADERS,
    "Content-Type": "application/json",
    "Referer": f"{MONKEY_BASE}/gu/ko/golf/guam-golf/product/product_detail.php",
}


def crawl_monkey_travel(course_info: dict, play_date: str, cfg: dict) -> list:
    """
    몽키트래블에서 특정 날짜의 그린피 조회.

    course_info 필수 키: monkey_product_id, course_name
    """
    product_id = str(course_info.get("monkey_product_id", "") or "")
    if not product_id:
        return []

    golf_cfg    = cfg.get("golf_crawl", {})
    adult_count = golf_cfg.get("adult_count", 2)
    holes       = course_info.get("holes", golf_cfg.get("holes", 18))
    timeout     = golf_cfg.get("timeout", 30)
    url         = f"{MONKEY_BASE}/gu/ko/golf/guam-golf/product/product_detail.php?product_id={product_id}"

    payload = {
        "checkInDate":           play_date,
        "authToken":             "",
        "memberSaleCurrency":    "KRW",
        "memberCodePayCurrency": "KRW",
        "priceSite":             "monkey",
        "adultCount":            adult_count,
        "translateLang":         "ko",
        "hole":                  f"{holes}H",
        "product_id":            product_id,
    }

    session = requests.Session()
    session.headers.update(_MONKEY_HEADERS)

    try:
        resp = session.post(_MONKEY_PRICE_URL, json=payload, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"[몽키트래블] {product_id} {play_date} 요청 실패: {e}")
        return [_make_golf_error("몽키트래블", url, str(e)[:120])]

    if not data.get("result"):
        err = str(data.get("errorMsg", "result=false"))[:120]
        logger.warning(f"[몽키트래블] {product_id} {play_date}: {err}")
        return [_make_golf_error("몽키트래블", url, err)]

    items = data.get("data", [])
    if not items:
        logger.info(f"[몽키트래블] {product_id} {play_date}: 데이터 없음 (예약 불가 날짜)")
        return [_make_golf_error("몽키트래블", url, "no_data")]

    records = []
    seen_tod = set()  # (time_of_day, day_of_week) 중복 방지

    for item in items:
        etc       = item.get("etc", {})
        time_data = item.get("timeData", [])

        # 요일 구분
        day_of_week_raw = etc.get("dayOfWeek", "")
        day_of_week = "주중" if day_of_week_raw == "weekday" else "주말"

        # 시간대 (첫 번째 tee time의 시간대로 대표)
        tod_raw    = time_data[0].get("golfTimeOfDay", "") if time_data else ""
        time_of_day = _TIME_OF_DAY_KO.get(tod_raw, tod_raw)

        key = (time_of_day, day_of_week)
        if key in seen_tod:
            continue  # 같은 시간대·요일 중복 제거
        seen_tod.add(key)

        # 1인 그린피: breakdownDisplay.detailList[type=GreenFee].priceOne
        green_fee_krw = 0
        breakdown = etc.get("breakdownDisplay", {})
        for detail in breakdown.get("detailList", []):
            if detail.get("type") == "GreenFee":
                green_fee_krw = int(detail.get("priceOne") or 0)
                break

        if green_fee_krw == 0:
            # fallback: 전체 총액 / 인원
            total = int(etc.get("greenPirce") or etc.get("price") or 0)
            n     = int(breakdown.get("adultCount") or adult_count or 1)
            green_fee_krw = total // n if n else 0

        # 포함 여부
        include = (etc.get("includeExclude") or {}).get("include", "")
        cart_included  = "CartFee"  in include
        caddy_included = "CaddyFee" in include

        records.append(GolfPriceRecord(
            crawled_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            property_name="",
            property_id="",
            competitor_name="",
            channel="몽키트래블",
            course_name=course_info.get("course_name", ""),
            holes=holes,
            play_date=play_date,
            day_of_week=day_of_week,
            time_of_day=time_of_day,
            green_fee_krw=green_fee_krw,
            green_fee_usd=0.0,
            cart_included=cart_included,
            caddy_included=caddy_included,
            url=url,
        ))

    if not records:
        logger.warning(f"[몽키트래블] {product_id} {play_date}: 파싱된 레코드 없음")
        return [_make_golf_error("몽키트래블", url, "no_parsed_records")]

    logger.info(f"[몽키트래블] {product_id} {play_date}: {len(records)}건 수집")
    return records


# ---------------------------------------------------------------------------
# AGL (Tiger Booking) 크롤러 — requests 기반 listing 최저가
# ---------------------------------------------------------------------------
#
# Tiger Booking (www.tigerbooking.com) 의 구조:
#   - 국가별 목록: /ko/golf-course/teetime/1/{country_code}
#   - 각 상품에 tigerbooking_prod_id (예: TOGU0004, TOGU0006, TOGU0001)
#   - 응답 HTML의 RSC payload에 "sales_price" (일일 최저가, 날짜 무관) 포함
#   - 날짜별 필터 API 미공개 → 현재가 최저가(최근 가용 슬롯) 방식으로 수집
#
# 조사 결과 (2026-04-18):
#   GU 목록 URL: /ko/golf-course/teetime/1/GU
#   TOGU0004 = 소노 펠리체 컨트리 클럽 괌 망길라오 (실시간, book_type=2)
#   TOGU0006 = 소노 펠리체 컨트리 클럽 괌 탈로포포 (실시간, book_type=2)
#   TOGU0001 = 레오팔레스 CC                      (실시간, book_type=2)
#   TOGU0005 = 컨트리 클럽 오브 더 퍼시픽          (예약문의, book_type=3)
#   QOGU0001 = 파인이스트 괌 골프앤리조트           (예약문의, book_type=3)
# ---------------------------------------------------------------------------

_AGL_LISTING_URL = f"{AGL_BASE}/ko/golf-course/teetime/1/{{country_code}}"
_AGL_RSC_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Referer": f"{AGL_BASE}/",
}

# 국가별 listing 캐시: country_code → (products list, fetch_time)
_AGL_LISTING_CACHE: dict = {}
_AGL_LISTING_TTL_HOURS = 1


def _fetch_agl_listing(country_code: str, timeout: int = 30) -> list:
    """
    Tiger Booking 국가별 목록 페이지에서 상품 목록 조회.

    Returns:
        list[dict]: products (prod_id, gcprd_seq, product_name, price, book_type, ...)
    """
    global _AGL_LISTING_CACHE

    # 캐시 확인
    cached = _AGL_LISTING_CACHE.get(country_code)
    if cached:
        products, ts = cached
        if (datetime.now() - ts).total_seconds() < _AGL_LISTING_TTL_HOURS * 3600:
            return products

    url = _AGL_LISTING_URL.format(country_code=country_code)
    try:
        resp = requests.get(url, headers=_AGL_RSC_HEADERS, timeout=timeout)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"[AGL] {country_code} 목록 조회 실패: {e}")
        return []

    products = _parse_agl_rsc_products(resp.text)
    _AGL_LISTING_CACHE[country_code] = (products, datetime.now())
    logger.info(f"[AGL] {country_code} 목록 로드: {len(products)}개 상품")
    return products


def _parse_agl_rsc_products(html: str) -> list:
    """
    Tiger Booking HTML의 Next.js RSC payload에서 상품 목록 추출.

    Returns:
        list[dict]: [{prod_id, product_name, price, book_type, ...}, ...]
    """
    scripts_raw = re.findall(
        r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)',
        html,
        re.DOTALL,
    )

    for raw in scripts_raw:
        try:
            # RSC payload는 JSON unicode escape로 인코딩됨
            # raw 문자열을 utf-8 bytes로 해석 후 unicode_escape 디코딩
            decoded = bytes(raw, "utf-8").decode("unicode_escape")
        except Exception:
            decoded = raw

        if '"prod_id"' not in decoded or '"products"' not in decoded:
            continue

        start_idx = decoded.find('"products"')
        if start_idx == -1:
            continue

        arr_start = decoded.find("[", start_idx)
        if arr_start == -1:
            continue

        depth = 0
        end_idx = arr_start
        for j, ch in enumerate(decoded[arr_start:]):
            if ch in "[{":
                depth += 1
            elif ch in "]}":
                depth -= 1
            if depth == 0:
                end_idx = arr_start + j + 1
                break

        try:
            products = json.loads(decoded[arr_start:end_idx])
            if products:
                return products
        except json.JSONDecodeError:
            continue

    return []


def crawl_agl(course_info: dict, play_date: str, cfg: dict) -> list:
    """
    Tiger Booking (AGL) 그린피 크롤러.

    Tiger Booking은 날짜별 API를 공개하지 않으므로
    국가별 listing 페이지의 RSC 페이로드에서 일일 최저가를 수집합니다.

    - play_date: 수집 기준일 (실제로는 listing 최저가 = 가장 가까운 가용 슬롯 가격)
    - time_of_day: "최저가" (특정 시간대 아님)

    course_info 필수 키: tigerbooking_prod_id, tigerbooking_country, course_name
    """
    prod_id      = str(course_info.get("tigerbooking_prod_id", "") or "")
    country_code = str(course_info.get("tigerbooking_country", "GU") or "GU")

    if not prod_id:
        return []

    golf_cfg = cfg.get("golf_crawl", {})
    timeout  = golf_cfg.get("timeout", 30)
    holes    = course_info.get("holes", golf_cfg.get("holes", 18))
    url      = _AGL_LISTING_URL.format(country_code=country_code)

    products = _fetch_agl_listing(country_code, timeout)
    if not products:
        return [_make_golf_error("AGL", url, "listing_fetch_failed")]

    # prod_id로 코스 찾기
    product = next((p for p in products if p.get("prod_id") == prod_id), None)
    if product is None:
        return [_make_golf_error("AGL", url, f"prod_id_not_found:{prod_id}")]

    price_info = product.get("price") or {}
    sales_price = int(price_info.get("sales_price") or 0)
    book_type = int(product.get("book_type") or 0)

    # book_type=3 = 예약문의 전용 (실시간 가격 없음)
    if book_type == 3 or sales_price == 0:
        return [_make_golf_error("AGL", url, "inquiry_only_no_price")]

    # 요일 계산
    try:
        dt = datetime.strptime(play_date, "%Y-%m-%d")
        day_of_week = "주말" if dt.weekday() >= 5 else "주중"
    except Exception:
        day_of_week = ""

    record = GolfPriceRecord(
        crawled_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        property_name="",
        property_id="",
        competitor_name="",
        channel="AGL",
        course_name=course_info.get("course_name") or product.get("product_name", ""),
        holes=holes,
        play_date=play_date,
        day_of_week=day_of_week,
        time_of_day="최저가",      # Tiger Booking은 최저가만 노출
        green_fee_krw=sales_price,
        green_fee_usd=0.0,
        cart_included=False,       # 포함 여부 정보 없음
        caddy_included=False,
        url=url,
    )

    logger.info(f"[AGL] {prod_id} {play_date}: {sales_price:,}원 (최저가)")
    return [record]


# ---------------------------------------------------------------------------
# KKday 크롤러
# ---------------------------------------------------------------------------
#
# KKday (www.kkday.com) 상품 페이지에서 JSON-LD schema.org 마크업으로
# AggregateOffer.lowPrice (USD) 를 파싱합니다.
#
# 확인된 상품 (2026-04-18):
#   156010 = 소노 펠리체 컨트리 클럽 괌 망길라오 ✅ (~203 USD)
#   156016 = 소노 펠리체 컨트리 클럽 괌 탈로포포 ✅ (~155 USD)
#   156004 = Country Club of the Pacific Golf     ⚠️ 현재 404 (일시 중단)
#
# 주의:
#   - Chrome UA → HTTP 403 / iPhone Mobile UA → HTTP 200
#   - 'en-au' 로케일 URL → AUD 가격 반환 → 반드시 'en' 로케일 사용
#   - 가격은 날짜 독립적 최저가(starting from)이므로 _DATE_INDEPENDENT_CHANNELS 등록
# ---------------------------------------------------------------------------

_KKDAY_MOBILE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Mobile/15E148 Safari/604.1"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}


def crawl_kkday(course_info: dict, play_date: str, cfg: dict) -> list:
    """
    KKday 상품 페이지에서 그린피 조회.

    JSON-LD (schema.org AggregateOffer.lowPrice) 파싱 방식.
    가격은 날짜 독립적 최저가(starting from)이므로 play_date별 루프 없이
    오늘 기준 1회만 호출됩니다 (_DATE_INDEPENDENT_CHANNELS 참조).

    course_info 필수 키: kkday_product_id, course_name
    """
    product_id = str(course_info.get("kkday_product_id", "") or "")
    if not product_id:
        return []

    golf_cfg = cfg.get("golf_crawl", {})
    timeout  = golf_cfg.get("timeout", 30)
    holes    = course_info.get("holes", golf_cfg.get("holes", 18))
    url      = f"{KKDAY_BASE}/en/product/{product_id}"

    try:
        resp = requests.get(url, headers=_KKDAY_MOBILE_HEADERS, timeout=timeout)
        if resp.status_code == 404:
            logger.warning(f"[KKday] {product_id}: 상품 없음 (404)")
            return [_make_golf_error("KKday", url, "product_not_found_404")]
        resp.raise_for_status()
    except requests.HTTPError as e:
        return [_make_golf_error("KKday", url, str(e)[:120])]
    except Exception as e:
        logger.error(f"[KKday] {product_id} 요청 실패: {e}")
        return [_make_golf_error("KKday", url, str(e)[:120])]

    # JSON-LD (schema.org) 에서 가격 추출
    price_raw = 0.0
    currency  = "USD"
    jsonld_blocks = re.findall(
        r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>',
        resp.text,
        re.DOTALL,
    )
    for block in jsonld_blocks:
        try:
            data  = json.loads(block)
            graph = data.get("@graph", [data])
            for node in graph:
                offers = node.get("offers", {})
                if offers:
                    p = offers.get("lowPrice") or offers.get("price")
                    if p:
                        price_raw = float(p)
                        currency  = offers.get("priceCurrency", "USD")
                        break
        except Exception:
            continue
        if price_raw:
            break

    if not price_raw:
        logger.warning(f"[KKday] {product_id}: 가격 정보 없음")
        return [_make_golf_error("KKday", url, "price_not_found")]

    # 환율 변환: USD → KRW
    rates = get_exchange_rates()
    if currency == "USD":
        green_fee_usd = price_raw
        green_fee_krw = usd_to_krw(price_raw, rates)
    elif currency == "KRW":
        green_fee_krw = int(price_raw)
        green_fee_usd = krw_to_usd(green_fee_krw, rates)
    else:
        # 기타 통화: USD로 간주
        green_fee_usd = price_raw
        green_fee_krw = usd_to_krw(price_raw, rates)

    try:
        dt = datetime.strptime(play_date, "%Y-%m-%d")
        day_of_week = "주말" if dt.weekday() >= 5 else "주중"
    except Exception:
        day_of_week = ""

    record = GolfPriceRecord(
        crawled_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        property_name="",
        property_id="",
        competitor_name="",
        channel="KKday",
        course_name=course_info.get("course_name", ""),
        holes=holes,
        play_date=play_date,
        day_of_week=day_of_week,
        time_of_day="최저가",      # KKday는 starting from 최저가
        green_fee_krw=green_fee_krw,
        green_fee_usd=green_fee_usd,
        cart_included=False,
        caddy_included=False,
        url=url,
    )

    logger.info(
        f"[KKday] {product_id}: {price_raw} {currency} → {green_fee_krw:,}원"
    )
    return [record]


# ---------------------------------------------------------------------------
# 유틸리티
# ---------------------------------------------------------------------------

def _make_golf_error(channel: str, url: str, error: str) -> GolfPriceRecord:
    return GolfPriceRecord(
        crawled_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        property_name="",
        property_id="",
        competitor_name="",
        channel=channel,
        course_name="",
        holes=18,
        play_date="",
        day_of_week="",
        time_of_day="",
        green_fee_krw=0,
        green_fee_usd=0.0,
        cart_included=False,
        caddy_included=False,
        url=url,
        error=error,
    )


# ---------------------------------------------------------------------------
# 채널 등록
# ---------------------------------------------------------------------------

_CHANNEL_CRAWLERS = {
    "몽키트래블": crawl_monkey_travel,
    "AGL":        crawl_agl,
    "KKday":      crawl_kkday,
}

# 채널 → course_info에서 사용하는 ID 키
_CHANNEL_ID_KEY = {
    "몽키트래블": "monkey_product_id",
    "AGL":        "tigerbooking_prod_id",
    "KKday":      "kkday_product_id",
}


# ---------------------------------------------------------------------------
# 메인 실행
# ---------------------------------------------------------------------------

def run_golf_crawl(
    channel_filter: list = None,
    days_ahead: int = None,
    config_path: str = "config.yaml",
) -> pd.DataFrame:
    """
    골프 경쟁사 그린피 크롤링 메인 함수.

    Args:
        channel_filter: 크롤링할 채널 목록. None이면 전체.
                        예: ["몽키트래블"] / ["AGL", "몽키트래블"]
        days_ahead:     오늘로부터 며칠 후까지 조회. None이면 config 값 사용.
                        AGL은 날짜 독립적이므로 days_ahead와 무관하게 1회 수집.
        config_path:    config.yaml 경로.

    Returns:
        pd.DataFrame: GolfPriceRecord 컬럼 DataFrame (오류 레코드 포함).
                      빈 경우 빈 DataFrame 반환.
    """
    cfg      = load_golf_config(config_path)
    golf_cfg = cfg.get("golf_crawl", {})

    if days_ahead is None:
        days_ahead = golf_cfg.get("days_ahead", 14)

    channels = channel_filter if channel_filter is not None else list(_CHANNEL_CRAWLERS.keys())
    delay    = golf_cfg.get("request_delay", 1.5)

    golf_props = cfg.get("golf_properties", [])
    if not golf_props:
        logger.warning("config.yaml에 golf_properties 섹션 없음")
        return pd.DataFrame()

    today = datetime.today()
    play_dates = [
        (today + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(1, days_ahead + 1)
    ]

    # 환율 미리 로드 (run 시작 시 1회)
    exchange_rates = get_exchange_rates()
    if exchange_rates:
        logger.info(f"[환율] 1 USD = {exchange_rates.get('KRW', '?')} KRW / "
                    f"{exchange_rates.get('VND', '?')} VND")

    all_records: list = []

    for prop in golf_props:
        prop_name = prop.get("name", "")
        prop_id   = prop.get("id", "")
        holes     = golf_cfg.get("holes", 18)

        # 자사 + 경쟁사를 통합 처리
        candidates = []
        own_info = prop.get("own")
        if own_info:
            candidates.append({
                **own_info,
                "_is_own": True,
                "_competitor_name": "자사",
                "holes": holes,
            })
        for comp in prop.get("competitors", []):
            candidates.append({
                **comp,
                "_is_own": False,
                "_competitor_name": comp.get("name", ""),
                "holes": holes,
            })

        for course in candidates:
            is_own      = course.get("_is_own", False)
            comp_name   = course.get("_competitor_name", "")
            course_name = course.get("course_name") or course.get("name", "")

            for ch in channels:
                id_key = _CHANNEL_ID_KEY.get(ch, "")
                if not course.get(id_key):
                    continue  # 해당 채널 ID 없음 — 스킵

                crawl_fn  = _CHANNEL_CRAWLERS[ch]
                date_indep = ch in _DATE_INDEPENDENT_CHANNELS

                # 날짜 루프: 날짜 독립 채널(AGL)은 오늘 날짜 한 번만 실행
                dates_to_run = [today.strftime("%Y-%m-%d")] if date_indep else play_dates

                for play_date in dates_to_run:
                    try:
                        records = crawl_fn(course, play_date, cfg)
                    except Exception as e:
                        logger.error(
                            f"[{ch}] {course_name} {play_date}: {e}",
                            exc_info=True,
                        )
                        records = [_make_golf_error(ch, "", str(e)[:120])]

                    for rec in records:
                        rec.property_name   = prop_name
                        rec.property_id     = prop_id
                        rec.competitor_name = comp_name
                        if not rec.course_name:
                            rec.course_name = course_name
                        rec.is_own = is_own

                        # 환율 적용: green_fee_usd 채움
                        if rec.green_fee_krw and rec.green_fee_usd == 0.0:
                            rec.green_fee_usd = krw_to_usd(rec.green_fee_krw, exchange_rates)

                    all_records.extend(records)

                    if not date_indep:
                        time.sleep(delay)

    if not all_records:
        logger.warning("수집된 골프 가격 데이터 없음")
        return pd.DataFrame()

    df = pd.DataFrame([r.__dict__ for r in all_records])
    success = df[df["error"] == ""].shape[0]
    logger.info(f"골프 크롤링 완료: 전체 {len(df)}행 / 성공 {success}행")
    return df


def export_golf_df(df: pd.DataFrame, export_dir: str = "./exports") -> None:
    """DataFrame을 CSV + Excel로 저장"""
    if df.empty:
        logger.warning("내보낼 골프 가격 데이터 없음")
        return

    import os
    os.makedirs(export_dir, exist_ok=True)
    today = datetime.today().strftime("%Y%m%d")
    csv_path   = f"{export_dir}/golf_prices_{today}.csv"
    excel_path = f"{export_dir}/golf_prices_{today}.xlsx"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(excel_path, index=False)
    logger.info(f"골프 가격 저장: {csv_path} ({len(df)}행)")


# ---------------------------------------------------------------------------
# 독립 실행 (python golf_crawler.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="골프 그린피 크롤러")
    parser.add_argument(
        "--channel",
        nargs="+",
        default=None,
        help="크롤링 채널 (몽키트래블 / AGL / KKday). 생략 시 전체.",
    )
    parser.add_argument(
        "--days", type=int, default=None,
        help="오늘로부터 며칠 후까지 조회 (기본: config golf_crawl.days_ahead)",
    )
    parser.add_argument(
        "--export", action="store_true",
        help="CSV + Excel 저장",
    )
    args = parser.parse_args()

    df = run_golf_crawl(
        channel_filter=args.channel,
        days_ahead=args.days,
    )

    if not df.empty:
        ok = df[df["error"] == ""]
        print(ok[["property_name", "competitor_name", "channel", "course_name",
                   "play_date", "day_of_week", "time_of_day",
                   "green_fee_krw", "green_fee_usd",
                   "cart_included"]].to_string(max_rows=30))
        print(f"\n총 {len(df)}행 (성공: {len(ok)}건 / 오류: {len(df)-len(ok)}건)")
        if args.export:
            export_golf_df(df)
    else:
        print("수집된 데이터 없음")
