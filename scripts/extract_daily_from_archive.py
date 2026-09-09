#!/usr/bin/env python3
"""
extract_daily_from_archive.py
누적본에서 '그 파일 자신의 수집일' 행만 뽑아 비누적 일자별 데이터셋을 만든다.

배경:
  sono_competitor_prices_YYYYMMDD.csv 는 누적본이지만, 매일 새로 쓰는 과정에서
  과거 날짜의 is_own(자사) 행과 일부 경쟁사 행이 통째로 유실된다.
  따라서 날짜 D의 온전한 데이터는 파일명이 D인 파일에만 존재한다.
  → 각 파일에서 자기 날짜 행만 추출하면 무손실 일자별 데이터셋이 된다.

최적화:
  누적본은 append 순서라 '자기 날짜' 행은 파일 끝에 몰려 있다.
  파일 전체(64GB)를 읽지 않고 꼬리 구간만 읽되, 경계(이전 날짜 행)를 확인할 때까지
  창을 2배씩 늘려 안전성을 보장한다.

산출: data/daily_extract/sono_daily_YYYYMMDD.csv
실행: python3 scripts/extract_daily_from_archive.py
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ARCHIVE = Path("/Volumes/맥북프로26/crawler_archive/exports")
LOCAL = ROOT / "exports"
OUT = ROOT / "data" / "daily_extract"

FILE_RE = re.compile(r"^sono_competitor_prices_(\d{8})\.csv$")
START_WINDOW = 96 * 1024 * 1024     # 96MB 부터 시작
MAX_WINDOW = 3 * 1024 * 1024 * 1024  # 3GB 넘으면 전체 스캔으로 간주


def log(m):
    print(m, flush=True)


def collect_files() -> dict[str, Path]:
    """수집일(YYYY-MM-DD) → 그 날짜 파일. 로컬 우선."""
    found: dict[str, Path] = {}
    for base in (ARCHIVE, LOCAL):
        if not base.exists():
            log(f"경고: 경로 없음 {base}")
            continue
        for p in sorted(base.iterdir()):
            m = FILE_RE.match(p.name)
            if not m:
                continue
            raw = m.group(1)
            d = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
            # 로컬(뒤에 순회)이 아카이브를 덮어씀
            found[d] = p
    return dict(sorted(found.items()))


def read_header(path: Path) -> bytes:
    with open(path, "rb") as f:
        return f.readline()


def extract_day(path: Path, date: str) -> tuple[list[bytes], bool, int]:
    """파일 끝에서부터 date 로 시작하는 행을 모은다.
    반환: (행 리스트, 경계확인여부, 읽은 바이트)"""
    size = path.stat().st_size
    prefix = date.encode()
    window = START_WINDOW
    read_bytes = 0

    while True:
        full = window >= size
        start = 0 if full else size - window
        with open(path, "rb") as f:
            f.seek(start)
            buf = f.read()
        read_bytes = len(buf)
        lines = buf.split(b"\n")
        if not full:
            lines = lines[1:]          # 잘린 첫 줄 버림
        else:
            lines = lines[1:]          # 헤더 버림
        hits = [ln for ln in lines if ln.startswith(prefix)]

        # 경계 확인: 창 안에 '이 날짜가 아닌' 행이 하나라도 있어야
        # 블록 시작을 놓치지 않았다고 확신할 수 있다.
        boundary_seen = any(ln and not ln.startswith(prefix) for ln in lines)
        if full or boundary_seen or window >= MAX_WINDOW:
            return hits, (full or boundary_seen), read_bytes
        window *= 2


def main():
    if not ARCHIVE.exists():
        sys.exit(f"외장 아카이브가 마운트되어 있지 않습니다: {ARCHIVE}\n"
                 f"외장하드를 연결한 뒤 다시 실행하세요.")

    OUT.mkdir(parents=True, exist_ok=True)
    files = collect_files()
    log(f"대상 파일 {len(files)}개  ({min(files)} ~ {max(files)})")

    total_rows = 0
    total_read = 0
    empty_days = []
    unsure = []
    t0 = time.time()

    for i, (date, path) in enumerate(files.items(), 1):
        outp = OUT / f"sono_daily_{date.replace('-', '')}.csv"
        header = read_header(path)
        hits, ok, nread = extract_day(path, date)
        total_read += nread
        with open(outp, "wb") as f:
            f.write(header if header.endswith(b"\n") else header + b"\n")
            for ln in hits:
                f.write(ln + b"\n")
        total_rows += len(hits)
        if not hits:
            empty_days.append(date)
        if not ok:
            unsure.append(date)
        loc = "local" if path.parent == LOCAL else "arch"
        log(f"  [{i:3d}/{len(files)}] {date} {loc} "
            f"{len(hits):>7,}행  ({nread/1048576:.0f}MB 읽음)  "
            f"누적 {total_read/1073741824:.1f}GB / {time.time()-t0:.0f}s")

    log(f"\n완료: {total_rows:,} 행 / {len(files)}일 / "
        f"{total_read/1073741824:.1f}GB 읽음 / {time.time()-t0:.0f}초")
    if empty_days:
        log(f"수집 0행인 날 ({len(empty_days)}): {empty_days}")
    if unsure:
        log(f"⚠ 경계 미확인(창 부족 가능) ({len(unsure)}): {unsure}")


if __name__ == "__main__":
    main()
