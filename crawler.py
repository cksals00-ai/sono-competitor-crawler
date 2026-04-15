"""
소노 경쟁사 OTA 가격 크롤러 v2
- 야놀자 (nol.yanolja.com): requests + Next.js RSC JSON 파싱
- 여기어때 (yeogi.com): Selenium headless Chrome
- Booking.com: Selenium headless Chrome
"""

import re
import json
import time
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field

import requests
from bs4 import BeautifulSoup
import pandas as pd
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://nol.yanolja.com/",
}

# Selenium 드라이버 싱글톤 (재사용)
_driver = None


@dataclass
class PriceRecord:
    crawled_at: str
    property_name: str
    property_id: str
    competitor_name: str
    ota: str
    checkin_date: str
    checkout_date: str
    room_type: str = ""
    price: int = 0
    currency: str = "KRW"
    availability: str = "unknown"
    url: str = ""
    error: str = ""
    is_own: bool = False    # True = 소노 자사 가격
    is_promo: bool = False  # True = 프로모션/특가 진행중


# ---------------------------------------------------------------------------
# 설정 로드
# ---------------------------------------------------------------------------

def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# 야놀자 크롤러 (requests + Next.js RSC JSON 파싱)
# ---------------------------------------------------------------------------

def crawl_yanolja(competitor: dict, checkin: str, checkout: str, cfg: dict) -> list:
    base_url = competitor.get("yanolja_url", "")
    if not base_url:
        return []

    url = f"{base_url}?checkInDate={checkin}&checkOutDate={checkout}"
    records = []

    try:
        session = requests.Session()
        session.headers.update(HEADERS)
        resp = session.get(url, timeout=cfg["crawl"]["timeout"])
        resp.raise_for_status()
        html = resp.text

        rooms = _parse_yanolja_rooms(html)

        if not rooms:
            logger.warning(f"[야놀자] {competitor['name']} ({checkin}): 객실 데이터 없음")
            records.append(_make_record(competitor, "야놀자", checkin, checkout, url, error="no_room_data"))
            return records

        for room in rooms:
            records.append(PriceRecord(
                crawled_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                property_name="",
                property_id="",
                competitor_name=competitor["name"],
                ota="야놀자",
                checkin_date=checkin,
                checkout_date=checkout,
                room_type=room.get("name", ""),
                price=room.get("price", 0),
                availability=room.get("availability", "unknown"),
                url=url,
            ))

    except requests.RequestException as e:
        logger.error(f"[야놀자] {competitor['name']} 요청 실패: {e}")
        records.append(_make_record(competitor, "야놀자", checkin, checkout, url, error=str(e)[:100]))

    return records


def _parse_yanolja_rooms(html: str) -> list:
    """
    야놀자 Next.js App Router RSC 페이로드에서 객실/가격 데이터 추출.
    구조: self.__next_f.push([1, "..."]) 청크 → json.loads() 디코딩
          → bestPrice 섹션에서 totalRate 추출
          → 역방향 탐색으로 가장 가까운 roomTypeName 연결
    """
    chunks = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html)

    # 가장 큰 청크(메인 데이터)에서 파싱
    for raw_chunk in sorted(chunks, key=len, reverse=True):
        try:
            decoded = json.loads(f'"{raw_chunk}"')
        except Exception:
            continue

        if '"bestPrice"' not in decoded:
            continue

        rooms = _extract_rooms_from_decoded_chunk(decoded)
        if rooms:
            logger.debug(f"[야놀자] RSC 청크 파싱 성공 ({len(rooms)}개 객실)")
            return rooms

    return []


def _extract_rooms_from_decoded_chunk(decoded: str) -> list:
    """
    디코딩된 RSC 청크에서 bestPrice 섹션을 찾고
    역방향으로 roomTypeName을 연결하여 객실 리스트 반환.
    """
    rooms = []
    seen_names = set()

    for bp_m in re.finditer(r'"bestPrice"\s*:\s*\{', decoded):
        bp_start = bp_m.start()
        region = decoded[bp_start: bp_start + 600]

        # bestPrice 안의 totalRate 추출
        rate_m = re.search(r'"totalRate"\s*:\s*"(\d[\d,]+)"', region)

        # closed: null=판매가능, true=품절, "예약마감"/기타 문자열=품절
        closed_m = re.search(r'"closed"\s*:\s*(true|null|"[^"]*")', region)
        is_closed = False
        if closed_m:
            cv = closed_m.group(1)
            if cv == "true" or (cv.startswith('"') and cv != '"null"'):
                is_closed = True

        # totalRate가 없으면 품절로 처리 (예약마감 등)
        if not rate_m:
            if not is_closed:
                continue  # 가격도 없고 품절도 아니면 스킵
            price = 0
        else:
            price = _parse_price_str(rate_m.group(1))

        # 역방향으로 가장 가까운 roomTypeName 탐색
        lookback = decoded[max(0, bp_start - 8000): bp_start]
        name_matches = list(re.finditer(r'"roomTypeName"\s*:\s*"([^"]+)"', lookback))
        if not name_matches:
            continue

        room_name = name_matches[-1].group(1)
        if room_name in seen_names:
            continue
        seen_names.add(room_name)

        rooms.append({
            "name": room_name,
            "price": price if not is_closed else 0,
            "availability": "sold_out" if is_closed else "available",
        })

    return rooms


def _parse_price_str(s: str) -> int:
    """'130,000원', '98100', '\\u20a9 120,000' 등에서 숫자 추출"""
    digits = re.sub(r"[^\d]", "", s)
    if digits and 4 <= len(digits) <= 7:  # 1000 ~ 9999999 원 범위
        return int(digits)
    return 0


# ---------------------------------------------------------------------------
# 여기어때 크롤러 (Selenium)
# ---------------------------------------------------------------------------

def crawl_yeogiuh(competitor: dict, checkin: str, checkout: str, cfg: dict) -> list:
    base_url = competitor.get("yeogiuh_url", "")
    if not base_url:
        return []

    url = f"{base_url}?checkIn={checkin}&checkOut={checkout}&personal=2"
    records = []

    # 여기어때는 Cloudflare가 재사용 드라이버를 봇으로 탐지하여 두 번째 요청부터 차단함.
    # 요청마다 새 드라이버를 생성해서 각 방문이 "새 브라우저 세션"처럼 보이게 함.
    yeogi_driver = None
    try:
        yeogi_driver = _make_fresh_driver()
        yeogi_driver.get(url)
        time.sleep(7)

        html = yeogi_driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        # Cloudflare 차단 감지
        title_el = soup.find("title")
        if title_el and "Cloudflare" in title_el.get_text():
            logger.warning(f"[여기어때] {competitor['name']}: Cloudflare 차단 감지")
            records.append(_make_record(competitor, "여기어때", checkin, checkout, url, error="cloudflare_blocked"))
            return records

        rooms = _parse_yeogi_rooms(soup)

        if not rooms:
            logger.warning(f"[여기어때] {competitor['name']} ({checkin}): 객실 데이터 없음")
            records.append(_make_record(competitor, "여기어때", checkin, checkout, url, error="no_room_data"))
            return records

        for room in rooms:
            records.append(PriceRecord(
                crawled_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                property_name="",
                property_id="",
                competitor_name=competitor["name"],
                ota="여기어때",
                checkin_date=checkin,
                checkout_date=checkout,
                room_type=room.get("name", ""),
                price=room.get("price", 0),
                availability=room.get("availability", "unknown"),
                url=url,
                is_promo=room.get("is_promo", False),
            ))

    except Exception as e:
        logger.error(f"[여기어때] {competitor['name']} 실패: {e}")
        records.append(_make_record(competitor, "여기어때", checkin, checkout, url, error=str(e)[:100]))

    finally:
        if yeogi_driver:
            try:
                yeogi_driver.quit()
            except Exception:
                pass

    return records


def _parse_yeogi_rooms(soup: BeautifulSoup) -> list:
    """
    여기어때 숙소 상세 페이지에서 객실/가격 추출.
    __NEXT_DATA__ → props.pageProps.accommodationInfo.rooms 배열에서 파싱.
    구조: room.stay.price.discountPrice (쿠폰 적용가) / salePrice (정가)
         room.additional.status ("OPEN"/"CLOSED"), isDayUse
    패키지 상품 필터링: 객실명이 "["로 시작하는 항목 제외 (패키지 add-on)
    """
    rooms = []
    next_data_tag = soup.find("script", id="__NEXT_DATA__")
    if not next_data_tag:
        return rooms

    try:
        data = json.loads(next_data_tag.string)
        raw_rooms = (
            data.get("props", {})
                .get("pageProps", {})
                .get("accommodationInfo", {})
                .get("rooms", [])
        )
    except Exception:
        return rooms

    seen_names = set()
    for room in raw_rooms:
        additional = room.get("additional") or {}
        stay = room.get("stay") or {}
        prices = stay.get("price") or {}

        # 필터: 대실 제외, 패키지 add-on 제외, CLOSED 제외
        if additional.get("isDayUse"):
            continue
        if additional.get("status") == "CLOSED":
            continue

        name = room.get("name", "")
        if name in seen_names:
            continue
        seen_names.add(name)

        # 가격: discountPrice(쿠폰 최종가) → salePrice(정상 판매가) 순서
        discount_price = prices.get("discountPrice") or 0
        sale_price     = prices.get("salePrice") or 0
        price_val      = discount_price or sale_price or 0

        # 할인율 10% 이상이면 프로모션으로 판단
        is_promo_yeogi = (
            discount_price > 0
            and sale_price > 0
            and discount_price < sale_price * 0.90
        )

        # stockCount 0 이거나 status CLOSED → sold_out
        stock = stay.get("stockCount", 1)
        is_sold = (stock == 0)

        rooms.append({
            "name":     name,
            "price":    int(price_val) if not is_sold else 0,
            "availability": "sold_out" if is_sold else "available",
            "is_promo": is_promo_yeogi,
        })

    return rooms


# ---------------------------------------------------------------------------
# Agoda 크롤러 (Selenium)
# ---------------------------------------------------------------------------

def crawl_agoda(competitor: dict, checkin: str, checkout: str, cfg: dict) -> list:
    """Agoda 숙소 상세 페이지에서 객실/가격 수집 (Selenium 싱글톤)"""
    base_url = competitor.get("agoda_url", "")
    if not base_url:
        return []

    url = (
        f"{base_url}?checkIn={checkin}&checkOut={checkout}"
        "&adults=2&rooms=1&children=0"
    )
    records = []

    try:
        driver = _get_driver()
        driver.get("about:blank")
        time.sleep(0.3)
        driver.get(url)
        time.sleep(9)  # Agoda 동적 콘텐츠 로드 대기

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        # 접근 차단 감지
        title_el = soup.find("title")
        title_text = title_el.get_text(strip=True) if title_el else ""
        if any(kw in title_text for kw in ["Access Denied", "Just a moment", "Cloudflare", "Error"]):
            logger.warning(f"[Agoda] {competitor['name']}: 접근 차단 감지")
            records.append(_make_record(competitor, "Agoda", checkin, checkout, url, error="access_denied"))
            return records

        rooms = _parse_agoda_rooms(soup)

        if not rooms:
            logger.warning(f"[Agoda] {competitor['name']} ({checkin}): 객실 데이터 없음")
            records.append(_make_record(competitor, "Agoda", checkin, checkout, url, error="no_room_data"))
            return records

        for room in rooms:
            records.append(PriceRecord(
                crawled_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                property_name="",
                property_id="",
                competitor_name=competitor["name"],
                ota="Agoda",
                checkin_date=checkin,
                checkout_date=checkout,
                room_type=room.get("name", ""),
                price=room.get("price", 0),
                availability=room.get("availability", "unknown"),
                url=url,
                is_promo=room.get("is_promo", False),
            ))

    except Exception as e:
        logger.error(f"[Agoda] {competitor['name']} 실패: {e}")
        records.append(_make_record(competitor, "Agoda", checkin, checkout, url, error=str(e)[:100]))

    return records


def _parse_agoda_rooms(soup: BeautifulSoup) -> list:
    """Agoda 숙소 페이지에서 객실/가격 추출 (다중 셀렉터 fallback)"""
    rooms = []
    seen  = set()

    # ── 방법 1: data-selenium 속성 기반 (현행 Agoda 레이아웃) ────────────────
    row_selectors = [
        "[data-selenium='room-grid-row']",
        ".RoomCellContainer",
        ".MasterRoom",
        "[data-element-name='room-cell']",
        "[class*='RoomRow']",
    ]
    for sel in row_selectors:
        row_els = soup.select(sel)
        if not row_els:
            continue
        for row in row_els:
            name_el = row.select_one(
                "[data-selenium='room-type-feature-name'], "
                ".RoomCell-info-RoomName, [data-element-name='room-type-name'], "
                ".RoomName, [class*='roomTypeName'], [class*='room-name']"
            )
            price_el = row.select_one(
                "[data-selenium='display-price'], "
                ".priceValue, .Price__value, .price-exclusive-display, "
                "[data-element-name='price'], .totalPrice, "
                "[class*='pricePerRoom'], [class*='displayPrice']"
            )
            name       = name_el.get_text(strip=True) if name_el else ""
            price_text = price_el.get_text(strip=True) if price_el else ""
            price      = _parse_price_str(price_text)
            if name and name not in seen:
                seen.add(name)
                promo = any(kw in row.get_text() for kw in
                            ["Special Deal", "Limited-time", "Genius", "할인", "특가"])
                rooms.append({
                    "name":         name,
                    "price":        price,
                    "availability": "available" if price > 0 else "sold_out",
                    "is_promo":     promo,
                })
        if rooms:
            return rooms

    # ── 방법 2: __NEXT_DATA__ JSON ───────────────────────────────────────────
    next_data_tag = soup.find("script", id="__NEXT_DATA__")
    if next_data_tag:
        try:
            data = json.loads(next_data_tag.string)
            rooms_json = _extract_agoda_json_rooms(data)
            if rooms_json:
                return rooms_json
        except Exception:
            pass

    # ── 방법 3: 숫자 가격이 있는 큰 텍스트 요소에서 추출 (최후 수단) ──────────
    # 여러 가격 요소를 찾아 첫 번째 유효한 가격만 반환
    for el in soup.select("[class*='price'], [class*='Price']")[:30]:
        price = _parse_price_str(el.get_text(strip=True))
        if price > 10000 and "fallback" not in seen:
            seen.add("fallback")
            rooms.append({"name": "객실", "price": price,
                          "availability": "available", "is_promo": False})
            break

    return rooms


def _extract_agoda_json_rooms(data: dict) -> list:
    """__NEXT_DATA__ JSON 트리에서 객실/가격 재귀 탐색"""
    rooms = []

    def _walk(obj, depth=0):
        if depth > 10 or len(rooms) >= 10:
            return
        if isinstance(obj, list):
            for item in obj[:20]:
                _walk(item, depth + 1)
        elif isinstance(obj, dict):
            name  = obj.get("roomName") or obj.get("roomTypeName") or obj.get("name", "")
            price = (obj.get("perNightPrice") or obj.get("totalPrice")
                     or obj.get("price") or obj.get("displayPrice") or 0)
            if name and isinstance(price, (int, float)) and price > 1000:
                rooms.append({
                    "name":         str(name),
                    "price":        int(price),
                    "availability": "available",
                    "is_promo":     False,
                })
                return
            for v in obj.values():
                _walk(v, depth + 1)

    _walk(data)
    return rooms


# ---------------------------------------------------------------------------
# Booking.com 크롤러 (Selenium)
# ---------------------------------------------------------------------------

def crawl_booking(competitor: dict, checkin: str, checkout: str, cfg: dict) -> list:
    base_url = competitor.get("booking_url", "")
    if not base_url:
        return []

    url = (
        f"{base_url}?checkin={checkin}&checkout={checkout}"
        "&group_adults=2&no_rooms=1&lang=ko"
    )
    records = []

    try:
        driver = _get_driver()
        driver.get("about:blank")
        time.sleep(0.5)
        driver.get(url)
        time.sleep(7)  # JS 렌더링 대기

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        rooms = _parse_booking_rooms(soup)

        if not rooms:
            logger.warning(f"[Booking.com] {competitor['name']} ({checkin}): 객실 데이터 없음")
            records.append(_make_record(competitor, "Booking.com", checkin, checkout, url, error="no_room_data"))
            return records

        for room in rooms:
            records.append(PriceRecord(
                crawled_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                property_name="",
                property_id="",
                competitor_name=competitor["name"],
                ota="Booking.com",
                checkin_date=checkin,
                checkout_date=checkout,
                room_type=room.get("name", ""),
                price=room.get("price", 0),
                availability=room.get("availability", "unknown"),
                url=url,
            ))

    except Exception as e:
        logger.error(f"[Booking.com] {competitor['name']} 실패: {e}")
        records.append(_make_record(competitor, "Booking.com", checkin, checkout, url, error=str(e)[:100]))

    return records


def _parse_booking_rooms(soup: BeautifulSoup) -> list:
    """Booking.com 숙소 상세 페이지 #hprt-table 에서 객실/가격 추출"""
    rooms = []

    # 방법 1: 객실 테이블 (#hprt-table)
    room_rows = soup.select("tr.js-rt-block-row, tr[data-block-id]")
    for row in room_rows:
        # 객실명
        name_el = row.select_one(
            "span.hprt-roomtype-icon-link, .hprt-roomtype-link, [data-testid='roomtype-title']"
        )
        room_name = name_el.get_text(strip=True) if name_el else ""

        # 가격
        price_el = row.select_one(
            ".bui-price-display__value, .prco-valign-middle-helper, "
            "[data-testid='price-and-discounted-price'], .bp-price"
        )
        price_text = price_el.get_text(strip=True) if price_el else ""
        price_val = _parse_price_str(price_text)

        # 판매 불가
        sold_el = row.select_one(".soldout_overlay, .sold-out, [class*='soldout']")
        no_avail_text = any(kw in row.get_text() for kw in ["이용 불가", "No availability", "매진"])
        is_sold = sold_el is not None or no_avail_text

        if room_name or price_val:
            rooms.append({
                "name": room_name or "객실",
                "price": price_val if not is_sold else 0,
                "availability": "sold_out" if is_sold else "available",
            })

    if rooms:
        return rooms

    # 방법 2: data-testid 기반 (검색결과 혼용 레이아웃)
    price_els = soup.select("[data-testid='price-and-discounted-price']")
    name_els = soup.select("[data-testid='roomtype-title'], [data-testid='title']")
    for i, price_el in enumerate(price_els):
        price_val = _parse_price_str(price_el.get_text(strip=True))
        name = name_els[i].get_text(strip=True) if i < len(name_els) else f"객실_{i+1}"
        if price_val:
            rooms.append({"name": name, "price": price_val, "availability": "available"})

    return rooms


# ---------------------------------------------------------------------------
# Selenium 드라이버 관리
# ---------------------------------------------------------------------------

def _build_chrome_options() -> "Options":
    """공통 Chrome 옵션 생성"""
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
    return options


def _make_fresh_driver():
    """매 요청마다 새 Chrome 드라이버 생성 (Cloudflare 우회용)"""
    from selenium import webdriver
    options = _build_chrome_options()
    d = webdriver.Chrome(options=options)
    d.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )
    return d


def _get_driver():
    """헤드리스 Chrome 드라이버 싱글톤 반환 (Booking.com 등 재사용 가능한 OTA용)"""
    global _driver
    if _driver is not None:
        try:
            _ = _driver.current_url
            return _driver
        except Exception:
            _driver = None

    from selenium import webdriver
    _driver = webdriver.Chrome(options=_build_chrome_options())
    _driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )
    logger.info("[Selenium] Chrome 드라이버 초기화 완료")
    return _driver


def close_driver():
    global _driver
    if _driver:
        try:
            _driver.quit()
        except Exception:
            pass
        _driver = None


# ---------------------------------------------------------------------------
# 유틸리티
# ---------------------------------------------------------------------------

def _make_record(competitor, ota, checkin, checkout, url, error="") -> PriceRecord:
    return PriceRecord(
        crawled_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        property_name="",
        property_id="",
        competitor_name=competitor["name"],
        ota=ota,
        checkin_date=checkin,
        checkout_date=checkout,
        url=url,
        error=error,
    )


def generate_date_pairs(days_ahead: int) -> list:
    today = datetime.today()
    return [
        (
            (today + timedelta(days=i)).strftime("%Y-%m-%d"),
            (today + timedelta(days=i + 1)).strftime("%Y-%m-%d"),
        )
        for i in range(1, days_ahead + 1)
    ]


# ---------------------------------------------------------------------------
# 메인 실행
# ---------------------------------------------------------------------------

def run_crawl(config_path: str = "config.yaml", test_mode: bool = False) -> pd.DataFrame:
    """
    test_mode=True: 첫 사업장 첫 경쟁사만, 2일치만 수집 (빠른 검증용)
    각 사업장의 own_urls가 있으면 자사 가격도 함께 수집 (is_own=True)
    """
    cfg = load_config(config_path)
    all_records = []

    date_pairs = generate_date_pairs(2 if test_mode else cfg["crawl"]["days_ahead"])
    delay = cfg["crawl"]["request_delay"]

    crawlers = [
        (crawl_yanolja, "야놀자"),
        (crawl_yeogiuh, "여기어때"),
        (crawl_booking, "Booking.com"),
        (crawl_agoda,   "Agoda"),
    ]

    properties = cfg["properties"][:1] if test_mode else cfg["properties"]

    try:
        for prop in properties:
            logger.info(f"=== [{prop['name']}] 크롤링 시작 ===")

            # ── 자사 가격 수집 ──────────────────────────────────────────────
            own_urls = prop.get("own_urls", {})
            if any(own_urls.get(k, "") for k in ("yanolja_url", "yeogiuh_url", "booking_url")):
                own_entry = {
                    "name":        prop["name"],
                    "yanolja_url": own_urls.get("yanolja_url", ""),
                    "yeogiuh_url": own_urls.get("yeogiuh_url", ""),
                    "booking_url": own_urls.get("booking_url", ""),
                }
                logger.info(f"  [자사] {prop['name']}")
                for checkin, checkout in date_pairs:
                    for crawl_fn, label in crawlers:
                        try:
                            records = crawl_fn(own_entry, checkin, checkout, cfg)
                            for r in records:
                                r.property_name = prop["name"]
                                r.property_id   = prop["id"]
                                r.is_own        = True
                            all_records.extend(records)
                        except Exception as e:
                            logger.error(f"[{label}][자사] {prop['name']} {checkin} 오류: {e}")
                        time.sleep(delay)

            # ── 경쟁사 가격 수집 ────────────────────────────────────────────
            competitors = prop["competitors"][:1] if test_mode else prop["competitors"]
            for competitor in competitors:
                logger.info(f"  경쟁사: {competitor['name']}")
                for checkin, checkout in date_pairs:
                    for crawl_fn, label in crawlers:
                        try:
                            records = crawl_fn(competitor, checkin, checkout, cfg)
                            for r in records:
                                r.property_name = prop["name"]
                                r.property_id   = prop["id"]
                            all_records.extend(records)
                        except Exception as e:
                            logger.error(f"[{label}] {competitor['name']} {checkin} 오류: {e}")
                        time.sleep(delay)

    finally:
        close_driver()

    # 객실명 키워드로 프로모션 감지 (야놀자 "[오픈런]", "★특가★" 등 Booking.com "Limited-time")
    _PROMO_KW = {"오픈런", "★특가★", "특가", "할인", "Limited-time", "genius", "Genius"}
    for r in all_records:
        if not r.is_promo and any(kw in (r.room_type or "") for kw in _PROMO_KW):
            r.is_promo = True

    df = pd.DataFrame([r.__dict__ for r in all_records])
    logger.info(f"크롤링 완료. 총 {len(df)} 건 수집")
    return df


if __name__ == "__main__":
    import sys
    test = "--test" in sys.argv
    df = run_crawl(test_mode=test)
    if not df.empty:
        print("\n=== 수집 결과 ===")
        print(df.to_string(max_rows=50))
        print(f"\n총 {len(df)}건 | 오류: {(df.get('error', pd.Series(dtype=str)) != '').sum()}건")
    else:
        print("수집된 데이터 없음")
