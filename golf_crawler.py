"""
골프장 그린피 크롤러 v1
작성: 2026-04-18

채널별 구현 상태:
  ✅ 몽키트래블 (MonkeyTravel) : JSON REST API — 검증 완료
  ⚠️  AGL (Tiger Booking)       : Selenium headless Chrome — SPA, 선택기 확인 필요
  ❌ BaiGolf                    : API 미접근 (2026-04-18 기준 모든 엔드포인트 404)

출력 DataFrame 컬럼:
  crawled_at, property_name, property_id, competitor_name, channel,
  course_name, holes, play_date, day_of_week, time_of_day,
  green_fee_krw, green_fee_usd, cart_included, caddy_included,
  url, error, is_own
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

import requests
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

MONKEY_BASE = "https://www.monkeytravel.com"
AGL_BASE    = "https://www.tigerbooking.com"

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


# ---------------------------------------------------------------------------
# 데이터 모델
# ---------------------------------------------------------------------------

@dataclass
class GolfPriceRecord:
    crawled_at:     str
    property_name:  str     # 소노 사업장명 (예: 소노펠리체CC 괌 망길라오)
    property_id:    str     # 사업장 ID
    competitor_name: str    # 경쟁사명 ("자사" if own)
    channel:        str     # 몽키트래블 / AGL / BaiGolf
    course_name:    str     # 골프장 실제 이름 (예: 망길라오 골프 클럽)
    holes:          int     # 홀수 (18)
    play_date:      str     # YYYY-MM-DD
    day_of_week:    str     # 주중 / 주말
    time_of_day:    str     # 오전 / 오후 / 트와일라잇 / 야간
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
# AGL (Tiger Booking) 크롤러 — Selenium
# ---------------------------------------------------------------------------
#
# URL: https://www.tigerbooking.com/ko/golf-course/teetime/{field_id}
# 방식: Selenium headless Chrome (SPA — 가격 데이터는 JavaScript로만 로드됨)
#
# 구현 상태:
#   - 페이지 로드 + 날짜 설정까지 구현
#   - 가격 파싱 선택기(CSS selector)는 실제 Selenium 실행 후 확인 필요
#   - _parse_agl_prices() 함수에서 TODO 표시된 부분 업데이트 필요
# ---------------------------------------------------------------------------

def crawl_agl(course_info: dict, play_date: str, cfg: dict) -> list:
    """
    Tiger Booking (AGL) 그린피 크롤러.
    Selenium headless Chrome을 사용하여 SPA 페이지에서 가격 추출.

    course_info 필수 키: agl_field_id, course_name
    """
    field_id = course_info.get("agl_field_id", "")
    if not field_id:
        return []

    golf_cfg    = cfg.get("golf_crawl", {})
    adult_count = golf_cfg.get("adult_count", 2)
    holes       = course_info.get("holes", golf_cfg.get("holes", 18))
    url         = f"{AGL_BASE}/ko/golf-course/teetime/{field_id}"

    driver = None
    try:
        driver = _make_agl_driver()
        records = _run_agl_session(driver, course_info, play_date, adult_count, holes, url, cfg)
        return records

    except ImportError:
        logger.warning("[AGL] selenium 미설치 — AGL 크롤링 건너뜀")
        return [_make_golf_error("AGL", url, "selenium_not_installed")]
    except Exception as e:
        logger.error(f"[AGL] {field_id} {play_date} 실패: {e}")
        return [_make_golf_error("AGL", url, str(e)[:120])]
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def _make_agl_driver():
    """AGL 전용 headless Chrome 드라이버 생성"""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    options.add_argument("--lang=ko-KR")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    d = webdriver.Chrome(options=options)
    d.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )
    return d


def _run_agl_session(
    driver,
    course_info: dict,
    play_date: str,
    adult_count: int,
    holes: int,
    url: str,
    cfg: dict,
) -> list:
    """
    Selenium 드라이버로 Tiger Booking 페이지를 조작하여 가격 추출.

    페이지 인터랙션 순서:
      1. 페이지 로드 및 React hydration 대기
      2. 날짜 선택 (calendar picker 조작)
      3. 인원 수 설정
      4. 검색 버튼 클릭
      5. 결과 로딩 대기
      6. 가격 데이터 파싱

    NOTE: Tiger Booking SPA의 정확한 CSS 선택기는 Selenium 실행 후
          실제 렌더링된 HTML을 확인하여 _parse_agl_prices()에서 업데이트 필요.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup
    import re

    field_id = course_info.get("agl_field_id", "")

    # ── 1. 페이지 로드 ────────────────────────────────────────────────────────
    driver.get(url)

    # React 앱 초기화 대기 (최대 15초)
    try:
        WebDriverWait(driver, 15).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except Exception:
        pass
    time.sleep(3)  # 추가 hydration 대기

    # ── 2. 날짜 설정 ─────────────────────────────────────────────────────────
    # Tiger Booking은 React date picker를 사용하므로 JS로 직접 값 설정 시도
    # 형식: YYYY-MM-DD
    date_selectors = [
        "input[type='date']",
        "input[name='date']",
        "input[name='checkInDate']",
        "[data-testid='date-input'] input",
        ".date-input input",
        "input[placeholder]",  # 날짜 입력 필드 (placeholder 있는 input)
    ]

    date_set = False
    for selector in date_selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, selector)
            driver.execute_script(
                """
                arguments[0].value = arguments[1];
                arguments[0].dispatchEvent(new Event('input',  {bubbles: true}));
                arguments[0].dispatchEvent(new Event('change', {bubbles: true}));
                """,
                el,
                play_date,
            )
            date_set = True
            logger.debug(f"[AGL] {field_id}: 날짜 설정 ({selector})")
            break
        except Exception:
            continue

    if not date_set:
        logger.warning(f"[AGL] {field_id} {play_date}: 날짜 입력 요소 미발견")

    time.sleep(1)

    # ── 3. 인원 설정 ─────────────────────────────────────────────────────────
    adult_selectors = [
        "input[name='adultCount']",
        "select[name='adults']",
        "[data-testid='adult-count'] input",
    ]
    for selector in adult_selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, selector)
            driver.execute_script(
                "arguments[0].value = arguments[1]; "
                "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                el,
                str(adult_count),
            )
            logger.debug(f"[AGL] {field_id}: 인원 설정 ({adult_count}명)")
            break
        except Exception:
            continue

    # ── 4. 검색 버튼 클릭 ────────────────────────────────────────────────────
    btn_selectors = [
        "button[type='submit']",
        "[data-testid='search-button']",
        "button.search-btn",
        # 텍스트로 찾기 (XPath)
    ]
    btn_clicked = False
    for selector in btn_selectors:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, selector)
            btn.click()
            btn_clicked = True
            logger.debug(f"[AGL] {field_id}: 검색 버튼 클릭 ({selector})")
            break
        except Exception:
            continue

    if not btn_clicked:
        # XPath로 "검색" 텍스트 버튼 시도
        try:
            btn = driver.find_element(
                By.XPATH, "//button[contains(text(),'검색') or contains(text(),'Search')]"
            )
            btn.click()
            btn_clicked = True
        except Exception:
            pass

    time.sleep(4)  # 결과 로딩 대기

    # ── 5. 가격 파싱 ─────────────────────────────────────────────────────────
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    records = _parse_agl_prices(soup, course_info, play_date, holes, url)

    if not records:
        # 파싱 실패 시 페이지 소스 일부를 디버그 로그에 남김
        logger.warning(
            f"[AGL] {field_id} {play_date}: 가격 파싱 실패\n"
            f"  페이지 소스 (500자): {html[:500]}"
        )
        return [_make_golf_error("AGL", url, "no_parsed_data")]

    logger.info(f"[AGL] {field_id} {play_date}: {len(records)}건 수집")
    return records


def _parse_agl_prices(
    soup,
    course_info: dict,
    play_date: str,
    holes: int,
    url: str,
) -> list:
    """
    Tiger Booking 렌더링 HTML에서 그린피 파싱.

    TODO: Tiger Booking의 실제 가격 HTML 구조를 Selenium으로 확인 후
          아래 선택기를 업데이트해야 합니다.

    현재 구현:
      - 숫자+KRW/USD 패턴으로 가격 후보 탐색
      - 테이블/카드 구조에서 시간대별 파싱 시도
    """
    import re
    records = []

    # ── 패턴 1: 테이블 행에서 시간대 + 가격 추출 ────────────────────────────
    # Tiger Booking의 결과는 tbody > tr 구조일 가능성이 높음
    for row in soup.select("tbody tr, .price-row, .teetime-row"):
        cells = row.find_all(["td", "th"])
        text_cells = [c.get_text(strip=True) for c in cells]

        price_val = 0
        time_of_day = ""

        for txt in text_cells:
            # 시간대 감지
            for en, ko in _TIME_OF_DAY_KO.items():
                if en.lower() in txt.lower() or ko in txt:
                    time_of_day = ko
                    break
            # 가격 감지
            m = re.search(r"([\d,]{4,})", txt)
            if m:
                candidate = int(m.group(1).replace(",", ""))
                if 10_000 <= candidate <= 5_000_000:
                    price_val = candidate

        if price_val:
            records.append(GolfPriceRecord(
                crawled_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                property_name="",
                property_id="",
                competitor_name="",
                channel="AGL",
                course_name=course_info.get("course_name", ""),
                holes=holes,
                play_date=play_date,
                day_of_week="",
                time_of_day=time_of_day or "오전",
                green_fee_krw=price_val,
                green_fee_usd=0.0,
                cart_included=False,
                caddy_included=False,
                url=url,
            ))

    if records:
        return records

    # ── 패턴 2: 모든 텍스트에서 숫자 + 통화 패턴 ────────────────────────────
    price_pattern = re.compile(r"([\d,]{5,})\s*(?:원|KRW)")
    candidates = []
    for txt in soup.stripped_strings:
        m = price_pattern.search(txt)
        if m:
            val = int(m.group(1).replace(",", ""))
            if 10_000 <= val <= 5_000_000:
                candidates.append(val)

    if candidates:
        # 가장 낮은 가격을 대표값으로 사용
        price_val = min(candidates)
        records.append(GolfPriceRecord(
            crawled_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            property_name="",
            property_id="",
            competitor_name="",
            channel="AGL",
            course_name=course_info.get("course_name", ""),
            holes=holes,
            play_date=play_date,
            day_of_week="",
            time_of_day="오전",
            green_fee_krw=price_val,
            green_fee_usd=0.0,
            cart_included=False,
            caddy_included=False,
            url=url,
        ))

    return records


# ---------------------------------------------------------------------------
# BaiGolf 크롤러 — 미구현
# ---------------------------------------------------------------------------
#
# 2026-04-18 기준 조사 결과:
#   www.baigolf.com/api.php 엔드포인트 모두 404 응답
#   w.baigolf.com (모바일) 응답 없음
#   SPA 기반으로 추정 — 추후 접근 방법 파악 후 구현 필요
# ---------------------------------------------------------------------------

def crawl_baigolf(course_info: dict, play_date: str, cfg: dict) -> list:
    """
    BaiGolf 그린피 크롤러 (미구현).

    TODO: API 접근 방법 확인 후 구현
    """
    course_id = str(course_info.get("baigolf_course_id", "") or "")
    if not course_id:
        return []

    url = f"https://w.baigolf.com/course.php?act=detail&golf_course_id={course_id}"
    logger.debug(f"[BaiGolf] {course_id}: 미구현 채널 — 건너뜀")
    return [_make_golf_error("BaiGolf", url, "not_implemented")]


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
    "BaiGolf":    crawl_baigolf,
}

# 채널 → course_info에서 사용하는 ID 키
_CHANNEL_ID_KEY = {
    "몽키트래블": "monkey_product_id",
    "AGL":        "agl_field_id",
    "BaiGolf":    "baigolf_course_id",
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

                crawl_fn = _CHANNEL_CRAWLERS[ch]

                for play_date in play_dates:
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

                    all_records.extend(records)
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
        help="크롤링 채널 (몽키트래블 / AGL / BaiGolf). 생략 시 전체.",
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
        print(df[df["error"] == ""].to_string(max_rows=20))
        print(f"\n총 {len(df)}행 (성공: {(df['error']=='').sum()}건)")
        if args.export:
            export_golf_df(df)
    else:
        print("수집된 데이터 없음")
