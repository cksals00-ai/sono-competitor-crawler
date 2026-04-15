"""
17개 전체 사업장 통합 테스트
- 사업장별 첫 번째 경쟁사만
- 체크인: 오늘+7일, 1박
- 3개 OTA 모두 테스트
- 결과를 사업장/OTA별로 요약 출력
"""

import sys
import time
import logging
from datetime import datetime, timedelta

import pandas as pd

from crawler import load_config, crawl_yanolja, crawl_yeogiuh, crawl_booking, close_driver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

CHECKIN  = (datetime.today() + timedelta(days=7)).strftime("%Y-%m-%d")
CHECKOUT = (datetime.today() + timedelta(days=8)).strftime("%Y-%m-%d")

CRAWLERS = [
    (crawl_yanolja, "야놀자"),
    (crawl_yeogiuh, "여기어때"),
    (crawl_booking, "Booking.com"),
]


def run_test():
    cfg = load_config("config.yaml")
    delay = cfg["crawl"]["request_delay"]

    results = []  # (property, competitor, ota, ok, records, error_msg)

    try:
        for prop in cfg["properties"]:
            comp = prop["competitors"][0]  # 첫 번째 경쟁사만
            prop_name = prop["name"]
            comp_name = comp["name"]

            logger.info(f"=== [{prop_name}] → {comp_name} ===")

            for crawl_fn, ota_label in CRAWLERS:
                url_key = {"야놀자": "yanolja_url", "여기어때": "yeogiuh_url", "Booking.com": "booking_url"}[ota_label]
                has_url = bool(comp.get(url_key, ""))

                if not has_url:
                    results.append({
                        "사업장": prop_name,
                        "경쟁사": comp_name,
                        "OTA": ota_label,
                        "상태": "URL없음",
                        "객실수": 0,
                        "오류": "",
                    })
                    continue

                try:
                    records = crawl_fn(comp, CHECKIN, CHECKOUT, cfg)
                    errors = [r for r in records if r.error]
                    ok_rooms = [r for r in records if not r.error and r.room_type]
                    sold_out = [r for r in records if not r.error and r.availability == "sold_out"]

                    if errors:
                        err_msg = errors[0].error
                        results.append({
                            "사업장": prop_name,
                            "경쟁사": comp_name,
                            "OTA": ota_label,
                            "상태": "오류",
                            "객실수": 0,
                            "오류": err_msg,
                        })
                    elif ok_rooms:
                        results.append({
                            "사업장": prop_name,
                            "경쟁사": comp_name,
                            "OTA": ota_label,
                            "상태": "성공",
                            "객실수": len(ok_rooms),
                            "오류": "",
                        })
                    elif sold_out:
                        results.append({
                            "사업장": prop_name,
                            "경쟁사": comp_name,
                            "OTA": ota_label,
                            "상태": "매진",
                            "객실수": len(sold_out),
                            "오류": "",
                        })
                    else:
                        results.append({
                            "사업장": prop_name,
                            "경쟁사": comp_name,
                            "OTA": ota_label,
                            "상태": "데이터없음",
                            "객실수": 0,
                            "오류": "",
                        })
                except Exception as e:
                    logger.error(f"  [{ota_label}] 예외: {e}")
                    results.append({
                        "사업장": prop_name,
                        "경쟁사": comp_name,
                        "OTA": ota_label,
                        "상태": "예외",
                        "객실수": 0,
                        "오류": str(e)[:80],
                    })

                time.sleep(delay)

    finally:
        close_driver()

    df = pd.DataFrame(results)

    print("\n" + "=" * 100)
    print(f"통합 테스트 결과 (체크인: {CHECKIN})")
    print("=" * 100)
    print(df.to_string(index=False))

    print("\n--- 요약 ---")
    summary = df.groupby("상태").size().reset_index(name="건수")
    print(summary.to_string(index=False))

    total = len(df)
    success = (df["상태"].isin(["성공", "매진"])).sum()
    no_url  = (df["상태"] == "URL없음").sum()
    errors  = (df["상태"].isin(["오류", "예외", "데이터없음"])).sum()

    print(f"\n총 {total}건: 성공/매진 {success}건 | URL없음(스킵) {no_url}건 | 오류/데이터없음 {errors}건")

    if errors > 0:
        print("\n--- 오류 상세 ---")
        err_df = df[df["상태"].isin(["오류", "예외", "데이터없음"])][["사업장", "경쟁사", "OTA", "상태", "오류"]]
        print(err_df.to_string(index=False))

    return df


if __name__ == "__main__":
    run_test()
