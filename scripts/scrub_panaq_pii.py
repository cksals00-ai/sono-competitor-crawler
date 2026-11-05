#!/usr/bin/env python3
"""
파나크 예약현황 xlsx 개인정보(PII) 제거 — 소스 파일을 덮어써서 이름/연락처를 비운다.
  · 값만 비움(열은 유지) → parse_panaq(명명 컬럼 기반)·다른 소비처 안전.
  · 대상: 투숙객명·예약자명·영업직원·확인자·생성자·수정자·회원권명·연락처·핸드폰번호
  · build_panaq 가 파싱 전 자동 호출 → 매일 재export(26_11 등)에도 이름이 배포본에 안 남음.
  · 이미 비워진 파일은 저장만 생략(재실행 안전).

단독 실행:  python3.12 scripts/scrub_panaq_pii.py [폴더]
   기본 폴더 = gs_daily_trend_news/data/panaq_db
"""
import os, sys, glob
import openpyxl

DEFAULT_DIR = "/Users/chanminpark/Projects/gs_daily_trend_news_public_temp/data/panaq_db"
BLANK_COLUMNS = {"투숙객명", "예약자명", "영업직원", "확인자", "생성자", "수정자",
                 "회원권명", "연락처", "핸드폰번호"}


def _header_row(ws):
    """'상태'·'예약번호'가 있는 헤더 행 번호(1-base)와 헤더값. 없으면 (None, None)."""
    for r in range(1, 11):
        vals = [str(c.value).strip() if c.value is not None else "" for c in ws[r]]
        if "상태" in vals and "예약번호" in vals:
            return r, vals
    return None, None


def scrub_file(path: str) -> int:
    wb = openpyxl.load_workbook(path)        # 편집 위해 full load
    ws = wb.active
    hr, header = _header_row(ws)
    if hr is None:
        print(f"  헤더 못 찾음 — 스킵: {os.path.basename(path)}")
        wb.close(); return 0
    blanked = 0
    for i, h in enumerate(header):
        if h not in BLANK_COLUMNS:
            continue
        hit = False
        for (cell,) in ws.iter_rows(min_row=hr + 1, min_col=i + 1, max_col=i + 1):
            if cell.value not in (None, ""):
                cell.value = None
                hit = True
        if hit:
            blanked += 1
    if blanked:
        wb.save(path)
    wb.close()
    return blanked


def scrub_all(folder: str = DEFAULT_DIR) -> int:
    n = 0
    for path in sorted(glob.glob(os.path.join(folder, "*.xlsx"))):
        if os.path.basename(path).startswith("~$"):
            continue
        c = scrub_file(path)
        if c:
            n += 1
            print(f"  개인정보 {c}개 열 비움: {os.path.basename(path)}")
    return n


if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DIR
    cnt = scrub_all(folder)
    print(f"정리 완료: {cnt}개 파일 수정")
