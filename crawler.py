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
    review_score: float = 0.0  # OTA 별점 (0 = 미수집, 1.0~10.0 범위)
    review_count: int = 0      # 리뷰 수


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
        review_score, review_count = _parse_yanolja_review(html)

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
                review_score=review_score,
                review_count=review_count,
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


def _parse_yanolja_review(html: str) -> tuple:
    """
    야놀자 RSC 청크에서 별점과 리뷰 수를 추출한다.
    반환: (review_score: float, review_count: int)
    야놀자는 10점 만점 기준 (예: 9.5).
    """
    chunks = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html)
    for raw_chunk in sorted(chunks, key=len, reverse=True):
        try:
            decoded = json.loads(f'"{raw_chunk}"')
        except Exception:
            continue

        # 별점 키워드 탐색
        score_m = re.search(
            r'"(?:avgRating|averageRating|reviewScore|ratingScore|rating|score)"'
            r'\s*:\s*([0-9]+(?:\.[0-9]+)?)',
            decoded,
        )
        count_m = re.search(
            r'"(?:reviewCount|totalReviewCount|ratingCount|reviewCnt)"'
            r'\s*:\s*(\d+)',
            decoded,
        )

        if score_m:
            score = float(score_m.group(1))
            # 야놀자는 10점 만점이지만 가끔 100점 만점 형태로 오는 경우 정규화
            if score > 10:
                score = round(score / 10, 1)
            count = int(count_m.group(1)) if count_m else 0
            return score, count

    return 0.0, 0


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
        review_score, review_count = _parse_yeogi_review(soup)

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
                review_score=review_score,
                review_count=review_count,
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


def _parse_yeogi_review(soup: BeautifulSoup) -> tuple:
    """
    여기어때 __NEXT_DATA__에서 별점과 리뷰 수를 추출한다.
    반환: (review_score: float, review_count: int)
    여기어때는 10점 만점 기준.
    """
    next_data_tag = soup.find("script", id="__NEXT_DATA__")
    if not next_data_tag:
        return 0.0, 0
    try:
        data = json.loads(next_data_tag.string)
        info = (
            data.get("props", {})
                .get("pageProps", {})
                .get("accommodationInfo", {})
        )
        # 여기어때 별점 필드 탐색 (ratingAvg, ratingScore, avgRating 등)
        score = (
            info.get("ratingAvg")
            or info.get("ratingScore")
            or info.get("avgRating")
            or info.get("rating")
            or 0
        )
        count = (
            info.get("reviewCount")
            or info.get("totalReviewCount")
            or info.get("ratingCount")
            or 0
        )
        return float(score), int(count)
    except Exception:
        return 0.0, 0


# ---------------------------------------------------------------------------
# Agoda 크롤러 (Selenium)
# ---------------------------------------------------------------------------

def crawl_agoda(competitor: dict, checkin: str, checkout: str, cfg: dict) -> list:
    """
    Agoda 숙소 상세 페이지에서 객실/가격 수집.
    - 싱글톤 드라이버를 버리고 매 요청 신규 드라이버 사용 (세션 누적 Bot 탐지 방지)
    - 로드 대기 15s + 스크롤 다운 → 레이지 로딩 트리거
    - 팝업/오버레이 닫기 시도
    - 기존 CSS 셀렉터 3단계 + 추가 JSON API 파싱
    """
    base_url = competitor.get("agoda_url", "")
    if not base_url:
        return []

    url = (
        f"{base_url}?checkIn={checkin}&checkOut={checkout}"
        "&adults=2&rooms=1&children=0&isVR=false"
    )
    records = []
    agoda_driver = None

    try:
        agoda_driver = _make_fresh_driver()
        agoda_driver.get("about:blank")
        time.sleep(0.5)
        agoda_driver.get(url)

        # 1차 대기 (초기 렌더링)
        time.sleep(10)

        # 팝업/모달 닫기 시도 (쿠키 동의, 언어 선택 등)
        _agoda_close_popups(agoda_driver)

        # 스크롤 다운 → 레이지 로딩 트리거
        try:
            agoda_driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
            time.sleep(3)
            agoda_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
        except Exception:
            pass

        html = agoda_driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        # 접근 차단 감지
        title_el = soup.find("title")
        title_text = title_el.get_text(strip=True) if title_el else ""
        if any(kw in title_text for kw in ["Access Denied", "Just a moment", "Cloudflare", "Error"]):
            logger.warning(f"[Agoda] {competitor['name']}: 접근 차단 감지")
            records.append(_make_record(competitor, "Agoda", checkin, checkout, url, error="access_denied"))
            return records

        rooms = _parse_agoda_rooms(soup)

        # 1차 파싱 실패 시 추가 대기 후 재시도
        if not rooms:
            time.sleep(8)
            html = agoda_driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            rooms = _parse_agoda_rooms(soup)

        if not rooms:
            logger.warning(f"[Agoda] {competitor['name']} ({checkin}): 객실 데이터 없음")
            records.append(_make_record(competitor, "Agoda", checkin, checkout, url, error="no_room_data"))
            return records

        review_score, review_count = _parse_agoda_review(soup)

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
                review_score=review_score,
                review_count=review_count,
            ))

    except Exception as e:
        logger.error(f"[Agoda] {competitor['name']} 실패: {e}")
        records.append(_make_record(competitor, "Agoda", checkin, checkout, url, error=str(e)[:100]))

    finally:
        if agoda_driver:
            try:
                agoda_driver.quit()
            except Exception:
                pass

    return records


def _agoda_close_popups(driver) -> None:
    """
    Agoda 공통 팝업/오버레이 닫기 시도.
    존재하지 않아도 오류 없이 넘어간다.
    """
    popup_selectors = [
        # 쿠키 동의 버튼
        "[data-element-name='cookie-accept']",
        "[id*='cookie'] button",
        # 언어/지역 선택 닫기
        "[data-selenium='close-btn']",
        "[data-element-name='close-button']",
        # 일반 모달 닫기
        "button[aria-label='Close']",
        ".Modal__closeButton",
        "[class*='closeButton']",
        "[class*='modal-close']",
    ]
    for sel in popup_selectors:
        try:
            els = driver.find_elements("css selector", sel)
            for el in els[:2]:
                if el.is_displayed():
                    el.click()
                    time.sleep(0.5)
        except Exception:
            pass


def _parse_agoda_rooms(soup: BeautifulSoup) -> list:
    """
    Agoda 숙소 페이지에서 객실/가격 추출 (다중 셀렉터 fallback).

    방법 1: data-selenium / data-element-name 속성 기반 (구 레이아웃)
    방법 2: 2024+ 리뉴얼 레이아웃 — MasterRoom, RoomGrid 클래스
    방법 3: __NEXT_DATA__ JSON (Next.js 기반 페이지)
    방법 4: 인라인 JSON 스크립트 (window.__AGODA_DATA__ 등)
    방법 5: 가격 숫자 텍스트 fallback
    """
    rooms = []
    seen  = set()

    # ── 방법 1: data-selenium 속성 기반 ──────────────────────────────────────
    row_selectors = [
        "[data-selenium='room-grid-row']",
        ".RoomCellContainer",
        ".MasterRoom",
        "[data-element-name='room-cell']",
        "[class*='RoomRow']",
        # 2024+ 레이아웃
        "[data-element-name='MasterRoom']",
        "[class*='masterRoom']",
        "[class*='PropertyRoomTypeRow']",
        "[class*='room-type-row']",
    ]
    for sel in row_selectors:
        row_els = soup.select(sel)
        if not row_els:
            continue
        for row in row_els:
            name_el = row.select_one(
                "[data-selenium='room-type-feature-name'], "
                ".RoomCell-info-RoomName, [data-element-name='room-type-name'], "
                ".RoomName, [class*='roomTypeName'], [class*='room-name'], "
                "[class*='RoomTypeFeatureName'], [class*='masterRoomName'], "
                "h3, h4"
            )
            price_el = row.select_one(
                "[data-selenium='display-price'], "
                ".priceValue, .Price__value, .price-exclusive-display, "
                "[data-element-name='price'], .totalPrice, "
                "[class*='pricePerRoom'], [class*='displayPrice'], "
                "[class*='perRoomPerNight'], [class*='price-info'], "
                "[class*='PaymentDetails'] [class*='price']"
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

    # ── 방법 3: 인라인 JSON 스크립트 (window.__AGODA__ 등) ───────────────────
    for script_tag in soup.find_all("script", type="application/json"):
        try:
            data = json.loads(script_tag.string or "")
            rooms_json = _extract_agoda_json_rooms(data)
            if rooms_json:
                return rooms_json
        except Exception:
            pass

    # ── 방법 4: 텍스트 파싱 — data-ppapi-room-type-id 등 ─────────────────────
    room_type_els = soup.select(
        "[data-ppapi-room-type-id], [data-room-type-id], [data-hotel-product-id]"
    )
    for el in room_type_els:
        name = el.get("data-room-type-name") or el.get("data-room-name", "")
        price_text = el.get("data-price") or el.get("data-display-price", "")
        price = _parse_price_str(price_text) if price_text else 0
        if not price:
            # 하위 가격 요소 탐색
            sub = el.select_one("[class*='price'], [class*='Price']")
            if sub:
                price = _parse_price_str(sub.get_text(strip=True))
        if name and name not in seen:
            seen.add(name)
            rooms.append({"name": name, "price": price,
                          "availability": "available" if price > 0 else "sold_out",
                          "is_promo": False})
    if rooms:
        return rooms

    # ── 방법 5: 숫자 가격이 있는 큰 텍스트 요소에서 추출 (최후 수단) ──────────
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


def _parse_agoda_review(soup: BeautifulSoup) -> tuple:
    """
    Agoda 페이지에서 별점과 리뷰 수 추출.
    반환: (review_score: float, review_count: int)
    Agoda는 10점 만점 기준.
    """
    # 방법 1: data-selenium 속성
    score_el = soup.select_one(
        "[data-selenium='hotel-overall-score'], "
        "[data-element-name='review-score'], "
        "[class*='reviewScore'], [class*='review-score'], "
        "[class*='RatingScore']"
    )
    count_el = soup.select_one(
        "[data-selenium='review-count'], "
        "[class*='reviewCount'], [class*='review-count']"
    )
    if score_el:
        try:
            score = float(re.sub(r"[^\d.]", "", score_el.get_text(strip=True)))
            count = int(re.sub(r"[^\d]", "", count_el.get_text(strip=True))) if count_el else 0
            if 0 < score <= 10:
                return score, count
        except Exception:
            pass

    # 방법 2: __NEXT_DATA__ JSON
    next_data_tag = soup.find("script", id="__NEXT_DATA__")
    if next_data_tag:
        try:
            data = json.loads(next_data_tag.string)
            score_val = _deep_get(data, ["reviewScore", "avgRating", "score", "rating"])
            count_val = _deep_get(data, ["reviewCount", "totalReviews", "ratingCount"])
            if score_val:
                s = float(score_val)
                if s > 10:
                    s = round(s / 10, 1)
                return s, int(count_val or 0)
        except Exception:
            pass

    return 0.0, 0


def _deep_get(data, keys: list):
    """딕셔너리/리스트를 재귀 탐색하여 첫 번째로 발견되는 키의 값을 반환."""
    if not isinstance(data, (dict, list)):
        return None
    if isinstance(data, dict):
        for key in keys:
            if key in data and data[key]:
                return data[key]
        for v in data.values():
            result = _deep_get(v, keys)
            if result:
                return result
    elif isinstance(data, list):
        for item in data[:10]:
            result = _deep_get(item, keys)
            if result:
                return result
    return None


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
        review_score, review_count = _parse_booking_review(soup)

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
                review_score=review_score,
                review_count=review_count,
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


def _parse_booking_review(soup: BeautifulSoup) -> tuple:
    """
    Booking.com 페이지에서 별점과 리뷰 수 추출.
    반환: (review_score: float, review_count: int)
    Booking.com은 10점 만점 기준.
    """
    # 방법 1: data-testid 셀렉터
    score_el = soup.select_one(
        "[data-testid='review-score-badge'], "
        "[data-testid='review-score'], "
        ".bui-review-score__badge, "
        "[class*='review-score-badge'], "
        ".b5cd34be2d"  # Booking.com 인라인 클래스 (버전에 따라 다름)
    )
    count_el = soup.select_one(
        "[data-testid='review-count'], "
        ".bui-review-score__text, "
        "[class*='review-count']"
    )

    if score_el:
        try:
            score_text = score_el.get_text(strip=True)
            # "9.5" 또는 "Excellent 9.5" 형식 처리
            score_match = re.search(r"([0-9]+(?:[.,][0-9]+)?)", score_text)
            if score_match:
                score = float(score_match.group(1).replace(",", "."))
                if 0 < score <= 10:
                    count_text = count_el.get_text(strip=True) if count_el else ""
                    count_match = re.search(r"[\d,]+", count_text.replace(",", ""))
                    count = int(count_match.group().replace(",", "")) if count_match else 0
                    return score, count
        except Exception:
            pass

    # 방법 2: JSON-LD schema.org (리뷰 점수가 있는 경우)
    for script_tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script_tag.string or "")
            if isinstance(data, dict):
                agg = data.get("aggregateRating") or {}
                score = agg.get("ratingValue") or agg.get("bestRating")
                count = agg.get("reviewCount") or agg.get("ratingCount")
                if score:
                    s = float(score)
                    if s > 10:
                        s = round(s / 10, 1)
                    return s, int(count or 0)
        except Exception:
            pass

    return 0.0, 0


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

def run_crawl(
    config_path: str = "config.yaml",
    test_mode: bool = False,
    ota_filter: list = None,
) -> pd.DataFrame:
    """
    test_mode=True: 첫 사업장 첫 경쟁사만, 2일치만 수집 (빠른 검증용)
    ota_filter: 크롤링할 OTA 목록. None이면 전체.
                예) ["야놀자"], ["Agoda"], ["여기어때", "Booking.com"]
    각 사업장의 own_urls가 있으면 자사 가격도 함께 수집 (is_own=True)
    """
    cfg = load_config(config_path)
    all_records = []

    date_pairs = generate_date_pairs(2 if test_mode else cfg["crawl"]["days_ahead"])
    delay = cfg["crawl"]["request_delay"]

    all_crawlers = [
        (crawl_yanolja, "야놀자"),
        (crawl_yeogiuh, "여기어때"),
        (crawl_booking, "Booking.com"),
        (crawl_agoda,   "Agoda"),
    ]
    if ota_filter:
        crawlers = [(fn, name) for fn, name in all_crawlers if name in ota_filter]
        logger.info(f"OTA 필터 적용: {ota_filter}")
    else:
        crawlers = all_crawlers

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
