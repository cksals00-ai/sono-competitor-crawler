#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
예약정보조회 xlsx 에서 불필요 열(비고·핸드폰·예약자전화번호)을 일괄 삭제 — 파일 안 열고 버튼 한 번.

사용: python3 scripts/clean_palatium_raw.py [폴더]
  - 폴더 생략 시 기본 = ~/Downloads
  - 폴더 내 *예약정보조회*.xlsx 전부 처리(제자리 저장). 사업계획·객실계획·임시(~$) 파일은 건너뜀.
  - 헤더행에서 이름으로 찾아 삭제하므로 열 위치가 달라도 안전. 이미 없으면 그 파일은 건너뜀.
"""
import sys, os, glob
import openpyxl

DROP_COLS = {"비고", "핸드폰", "예약자전화번호"}


def _find_header_row(ws):
    """헤더행(번호, 값리스트) 반환 — '상태'가 들어있는 첫 행(앞 타이틀/날짜행 스킵)."""
    for i, row in enumerate(ws.iter_rows(min_row=1, max_row=10, values_only=True), start=1):
        if row and any((c is not None and str(c).strip() == "상태") for c in row):
            return i, [(str(c).strip() if c is not None else "") for c in row]
    return None, None


def clean_file(path) -> int:
    """대상 열 삭제 후 제자리 저장. 삭제한 열 수 반환(0=변경없음)."""
    wb = openpyxl.load_workbook(path)  # 값+서식 유지
    removed = 0
    for ws in wb.worksheets:
        hr, hdr = _find_header_row(ws)
        if not hr:
            continue
        # 1-based 열 인덱스, 오른쪽→왼쪽으로 지워야 인덱스가 안 밀림
        idxs = sorted([j + 1 for j, name in enumerate(hdr) if name in DROP_COLS], reverse=True)
        for idx in idxs:
            ws.delete_cols(idx, 1)
            removed += 1
    if removed:
        wb.save(path)
    wb.close()
    return removed


def main():
    folder = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/Downloads")
    folder = os.path.expanduser(folder)
    if not os.path.isdir(folder):
        print(f"❌ 폴더 없음: {folder}")
        sys.exit(1)
    files = [f for f in glob.glob(os.path.join(folder, "*예약정보조회*.xlsx"))
             if not os.path.basename(f).startswith("~$")
             and "사업계획" not in f and "객실계획" not in f]
    if not files:
        print(f"⚠ 처리할 예약정보조회 파일 없음: {folder}")
        return
    print(f"📂 {folder}  —  {len(files)}개 파일 처리")
    tot = 0
    for f in sorted(files):
        try:
            n = clean_file(f)
        except Exception as e:
            print(f"  ❌ {os.path.basename(f)} 실패: {e}")
            continue
        tot += n
        mark = f"✂ {n}열 삭제" if n else "· 삭제할 열 없음"
        print(f"  {mark:<14} {os.path.basename(f)}")
    print(f"\n✅ 완료 — 총 {tot}개 열 삭제 ({', '.join(sorted(DROP_COLS))})")


if __name__ == "__main__":
    main()
