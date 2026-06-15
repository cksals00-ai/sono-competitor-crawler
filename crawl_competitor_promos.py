#!/usr/bin/env python3
"""
경쟁사 프로모션 수집기 (하이브리드 B+A)
============================================================
B(주): 경쟁사 공식 홈페이지의 프로모션/패키지 페이지를 직접 크롤
       - 한화리조트 / 롯데리조트 / 신라호텔
A(보조): 가격 크롤 CSV(exports/sono_competitor_prices_*.csv)의 is_promo에서
       경쟁사별 'OTA 실판매 최저가 · 특가 건수' 신호를 산출해 카드 detail에 합류

출력: data/competitors.json  (+ docs/data/competitors.json 사본)
      → 소노 레포 일일 커밋/푸시에 실려 GitHub Pages/raw 로 발행
      → gs collect_gs_monitor.py 가 이 URL을 매일 fetch (gs 무수정)

스키마: gs collect_gs_monitor.normalize_competitor 가 기대하는 형식
      {brand, region?, title, period, discount_pct, channel, detail, link}

설계 원칙: 가격 크롤러(crawler.py/run_phased.py)는 건드리지 않는다.
          사이트별 파서는 best-effort — 한 곳 실패해도 나머지는 진행,
          전부 실패하면 기존 competitors.json 유지.

사용: python3 crawl_competitor_promos.py
"""
import json
import logging
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("promo")

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DOCS_DATA_DIR = ROOT / "docs" / "data"
EXPORTS = ROOT / "exports"
KST = timezone(timedelta(hours=9))

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}
TIMEOUT = 25

# 날짜/가격/할인율 정규식
RE_PERIOD = re.compile(r"(\d{4}[.\-]\s?\d{1,2}[.\-]\s?\d{1,2}\s*[~\-]\s*\d{4}[.\-]\s?\d{1,2}[.\-]\s?\d{1,2})")
RE_PRICE = re.compile(r"([\d,]{4,})\s*(?:원|KRW)")
RE_DISCOUNT = re.compile(r"(\d{1,3})\s*%")


def fetch(url: str) -> BeautifulSoup | None:
    try:
        r = requests.get(url, headers=UA, timeout=TIMEOUT)
        r.encoding = r.apparent_encoding or "utf-8"
        if r.status_code != 200:
            logger.warning(f"  HTTP {r.status_code}: {url}")
            return None
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        logger.warning(f"  fetch 실패 ({type(e).__name__}): {url}")
        return None


def extract_discount(*texts: str) -> int:
    """텍스트에서 명시된 할인율%를 추출 (최댓값, 0~100)."""
    best = 0
    for t in texts:
        if not t:
            continue
        for m in RE_DISCOUNT.finditer(t):
            v = int(m.group(1))
            if 0 < v <= 100:
                best = max(best, v)
    return best


def clean(s: str) -> str:
    return " ".join((s or "").split())


# ─────────────────────────────────────────────────────────────
# B — 사이트별 파서.  각 파서는 캠페인 dict 리스트를 반환:
#   {title, detail, period, price, discount_pct}
# ─────────────────────────────────────────────────────────────
def parse_hanwha() -> list[dict]:
    url = "https://www.hanwharesort.co.kr/irsweb/resort3/event/package_list.do"
    soup = fetch(url)
    out: list[dict] = []
    if not soup:
        return out
    for it in soup.select("li.pin_item"):
        full = clean(it.get_text(" ", strip=True))
        tit = it.select_one("dt.cont_tit")
        txt = it.select_one("dd.cont_txt")
        title = clean(tit.get_text()) if tit else (full[:40])
        detail = clean(txt.get_text()) if txt else ""
        pm = RE_PERIOD.search(full)
        prm = RE_PRICE.search(full)
        out.append({
            "title": title,
            "detail": detail,
            "period": clean(pm.group(1)) if pm else "",
            "price": prm.group(1) if prm else "",
            "discount_pct": extract_discount(title, detail, full),
        })
    logger.info(f"  한화: {len(out)}개 프로모션")
    return out


def parse_lotte() -> list[dict]:
    url = "https://www.lotteresort.com/main/ko/package-event/package/list"
    soup = fetch(url)
    out: list[dict] = []
    if not soup:
        return out
    for it in soup.select("li.colum.col-4"):
        tit = it.select_one("dt.tit")
        det = it.select_one("dd.text-ellipsis")
        date = it.select_one("dd.date")
        num = it.select_one("p.price .num")
        title = clean(tit.get_text()) if tit else ""
        if not title:
            continue
        detail = clean(det.get_text()) if det else ""
        period = clean(date.get_text()).replace("진행기간 :", "").strip() if date else ""
        out.append({
            "title": title,
            "detail": detail,
            "period": period,
            "price": clean(num.get_text()) if num else "",
            "discount_pct": extract_discount(title, detail),
        })
    logger.info(f"  롯데리조트: {len(out)}개 프로모션")
    return out


def parse_shilla() -> list[dict]:
    out: list[dict] = []
    for prop, label in [("seoul", "서울"), ("jeju", "제주")]:
        url = f"https://www.shilla.net/{prop}/offers/pack/listPack.do"
        soup = fetch(url)
        if not soup:
            continue
        n0 = len(out)
        for it in soup.select("li.packBox"):
            full = clean(it.get_text(" ", strip=True))
            date = it.select_one("p.date")
            # 제목: date 앞쪽 텍스트 (Reserve/가격 제거)
            title = full
            if date:
                title = clean(full.split(clean(date.get_text()))[0])
            title = re.split(r"\d{4}[.\-]\d", title)[0].strip() or title
            if not title:
                continue
            out.append({
                "title": f"[{label}] {title}"[:120],
                "detail": "",
                "period": clean(date.get_text()) if date else "",
                "price": (RE_PRICE.search(full).group(1) if RE_PRICE.search(full) else ""),
                "discount_pct": extract_discount(full),
            })
        logger.info(f"  신라({label}): {len(out) - n0}개 프로모션")
    return out


def parse_high1() -> list[dict]:
    url = "https://high1.com/www/eventList.do?key=596&searchSe=프로모션"
    soup = fetch(url)
    out: list[dict] = []
    if not soup:
        return out
    for it in soup.select("li.p-media"):
        head = it.select_one(".p-event__heading-text")
        sub = it.select_one(".p-event__sub-text")
        info = it.select_one(".p-media__info")
        title = clean(head.get_text()) if head else ""
        if not title:
            continue
        detail = clean(sub.get_text()) if sub else ""
        infotxt = clean(info.get_text(" ")) if info else ""
        pm = re.search(r"이용기간\s*([\d\-]+\s*~\s*[\d\-]+)", infotxt)
        out.append({
            "title": title,
            "detail": detail,
            "period": clean(pm.group(1)) if pm else "",
            "price": "",
            "discount_pct": extract_discount(title, detail),
        })
    logger.info(f"  하이원: {len(out)}개 프로모션")
    return out


def parse_kensington() -> list[dict]:
    url = "https://www.kensington.co.kr/promotion"
    soup = fetch(url)
    out: list[dict] = []
    if not soup:
        return out
    for it in soup.select("li.event_item"):
        tit = it.select_one("div.title")
        title = clean(tit.get_text()) if tit else ""
        if not title:
            continue
        chain = it.select_one("div.chain")
        if chain and clean(chain.get_text()):
            title = f"{clean(chain.get_text())} {title}"
        desc = it.select_one("div.desc")
        detail = clean(desc.get_text()) if desc else ""
        period = ""
        dw = it.select_one("div.date_wrap")
        if dw:
            dwt = clean(dw.get_text(" "))
            m = re.search(r"투숙기간\s*(\d{4}[.\s]+\d{1,2}[.\s]+\d{1,2}\s*~\s*\d{4}[.\s]+\d{1,2}[.\s]+\d{1,2})", dwt)
            period = clean(m.group(1)) if m else ""
        out.append({
            "title": title[:120],
            "detail": detail,
            "period": period,
            "price": "",
            "discount_pct": extract_discount(title, detail),
        })
    logger.info(f"  켄싱턴: {len(out)}개 프로모션")
    return out


# 브랜드 정의: (표시명, 공식 프로모션 페이지 링크, 채널명, 파서, CSV 매칭 키워드, 권역)
BRANDS = [
    ("한화리조트", "https://www.hanwharesort.co.kr/irsweb/resort3/event/package_list.do",
     "한화리조트 공식 홈페이지", parse_hanwha, "한화", "vivaldi"),
    ("롯데리조트", "https://www.lotteresort.com/main/ko/package-event/package/list",
     "롯데리조트 공식 홈페이지", parse_lotte, "롯데", "apac"),
    ("신라호텔", "https://www.shilla.net/seoul/offers/pack/listPack.do",
     "신라호텔 공식 홈페이지", parse_shilla, "신라", "apac"),
    ("하이원리조트", "https://high1.com/www/eventList.do?key=596&searchSe=프로모션",
     "하이원리조트 공식 홈페이지", parse_high1, "하이원", "central"),
    ("켄싱턴리조트", "https://www.kensington.co.kr/promotion",
     "켄싱턴리조트 공식 홈페이지", parse_kensington, "켄싱턴", "south"),
]


# ─────────────────────────────────────────────────────────────
# A — 가격 CSV 신호 (보조).  브랜드 키워드 → {min_price, count, top_ota}
# ─────────────────────────────────────────────────────────────
def latest_csv() -> Path | None:
    cands = sorted(
        [c for c in EXPORTS.glob("sono_competitor_prices_2*.csv")
         if "only" not in c.stem and "broken" not in c.stem],
        key=lambda x: x.stem,
    )
    return cands[-1] if cands else None


def price_signal() -> dict[str, dict]:
    sig: dict[str, dict] = {}
    csv = latest_csv()
    if not csv:
        logger.info("  [A] 가격 CSV 없음 — 신호 생략")
        return sig
    try:
        import pandas as pd
        df = pd.read_csv(csv, dtype=str, usecols=[
            "경쟁사명", "OTA", "판매가(원)", "is_own", "is_promo"])
    except Exception as e:
        logger.warning(f"  [A] CSV 읽기 실패: {e}")
        return sig
    df = df[df["is_own"].str.lower() == "false"].copy()
    df = df[df["is_promo"].fillna("False").str.lower() == "true"]
    df["price"] = pd.to_numeric(df["판매가(원)"], errors="coerce")
    df = df.dropna(subset=["price"])
    df = df[df["price"] > 0]
    for _, kw in [(b[0], b[4]) for b in BRANDS]:
        g = df[df["경쟁사명"].fillna("").str.contains(kw)]
        if len(g):
            row = g.loc[g["price"].idxmin()]
            sig[kw] = {
                "min_price": int(row["price"]),
                "count": int(len(g)),
                "top_ota": clean(str(row["OTA"])),
            }
    logger.info(f"  [A] 가격 신호: {csv.name} → { {k: v['min_price'] for k,v in sig.items()} }")
    return sig


def main() -> None:
    logger.info("=" * 60)
    logger.info("경쟁사 프로모션 수집 (하이브리드 B+A) 시작")
    logger.info("=" * 60)

    sig = price_signal()
    competitors: list[dict] = []

    for brand, link, channel, parser, kw, region in BRANDS:
        try:
            camps = parser()
        except Exception as e:
            logger.warning(f"  {brand} 파서 예외 ({type(e).__name__}) — 스킵")
            camps = []
        if not camps:
            logger.warning(f"  {brand}: 프로모션 0개 — 카드 생략")
            continue

        # 헤드라인 = 명시 할인율 최대 캠페인, 없으면 첫 번째(피처드)
        camps_sorted = sorted(camps, key=lambda c: c["discount_pct"], reverse=True)
        head = camps_sorted[0]

        # detail = 헤드라인 상세 + 외 N개 + A 가격신호
        bits = []
        if head["detail"]:
            bits.append(head["detail"])
        elif head["price"]:
            bits.append(f"{head['price']}원~")
        if len(camps) > 1:
            others = [c["title"] for c in camps_sorted[1:3] if c["title"] != head["title"]]
            if others:
                bits.append("외 " + " · ".join(others[:2]) + (f" 등 {len(camps)}개" if len(camps) > 3 else ""))
        s = sig.get(kw)
        if s:
            bits.append(f"📊 OTA 실판매 최저 ₩{s['min_price']:,}({s['top_ota']}·특가 {s['count']}건)")

        competitors.append({
            "brand": brand,
            "region": region,
            "title": head["title"],
            "period": head["period"] or "상시",
            "discount_pct": head["discount_pct"],
            "channel": channel,
            "detail": " · ".join(bits)[:200],
            "link": link,
        })

    if not competitors:
        logger.warning("⚠ 모든 경쟁사 수집 실패 — 기존 competitors.json 유지")
        if (DATA_DIR / "competitors.json").exists():
            sys.exit(0)
        sys.exit("[error] 기존 파일도 없음")

    now = datetime.now(KST)
    out = {
        "_instruction": "경쟁사 공식 홈페이지 직접 크롤(B) + 가격 CSV 특가 신호(A). crawl_competitor_promos.py 자동 생성.",
        "_updated_at": now.strftime("%Y-%m-%d %H:%M:%S KST"),
        "_source": "경쟁사 공식 홈페이지 (한화/롯데/신라) + sono 가격 CSV",
        "_auto_collected": True,
        "competitors": competitors,
    }

    for d in (DATA_DIR, DOCS_DATA_DIR):
        d.mkdir(parents=True, exist_ok=True)
    primary = DATA_DIR / "competitors.json"
    primary.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    (DOCS_DATA_DIR / "competitors.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("=" * 60)
    logger.info(f"✓ 저장: {primary.relative_to(ROOT)} (+docs 사본)")
    for c in competitors:
        logger.info(f"  · {c['brand']} -{c['discount_pct']}% | {c['title'][:36]}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
