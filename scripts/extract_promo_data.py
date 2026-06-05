#!/usr/bin/env python3
"""
크롤링 CSV에서 경쟁사 프로모션 데이터를 추출해 promo_data.json 생성.

추출 로직:
1. CSV 읽기 (exports/sono_competitor_prices_<날짜>.csv)
2. 경쟁사(is_own=False)만 그룹핑
3. 패키지/프로모션명에서 특가/할인/PKG 키워드로 프로모션 감지
4. 같은 경쟁사 + 같은 객실타입은 최저가만 유지
5. data/promo_data.json 저장
6. docs/data/promo_data.json 에도 복사 (트렌드 리포트용)

사용: python3 scripts/extract_promo_data.py [YYYYMMDD]
"""
import json
import shutil
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
EXPORTS = ROOT / "exports"

# 프로모션/특가 감지 키워드
PROMO_KEYWORDS = [
    "특가", "할인", "PKG", "패키지", "프로모", "이벤트", "단독", "비밀",
    "쉿", "secret", "deal", "sale", "early", "얼리", "선착순", "한정",
    "타임", "핫딜", "스페셜", "혜택", "증정", "무료", "무박", "+",
]


def pick_csv(date_arg: str | None) -> Path:
    """요청 날짜 CSV, 없으면 가장 최근 CSV 선택."""
    if date_arg:
        p = EXPORTS / f"sono_competitor_prices_{date_arg}.csv"
        if p.exists():
            return p
        print(f"[warn] {p.name} 없음 → 최신 파일 사용")
    candidates = sorted(
        EXPORTS.glob("sono_competitor_prices_2*.csv"),
        key=lambda x: x.stem,
    )
    candidates = [c for c in candidates if "only" not in c.stem and "broken" not in c.stem]
    if not candidates:
        sys.exit("[error] CSV 파일을 찾을 수 없습니다.")
    return candidates[-1]


def is_promo_name(name: str) -> bool:
    if not isinstance(name, str):
        return False
    low = name.lower()
    return any(k.lower() in low for k in PROMO_KEYWORDS)


def main() -> None:
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    csv_path = pick_csv(date_arg)
    collected_at = csv_path.stem.split("_")[-1]  # YYYYMMDD
    collected_at_iso = f"{collected_at[:4]}-{collected_at[4:6]}-{collected_at[6:8]}"
    print(f"[info] 입력: {csv_path.name}  (collected_at={collected_at_iso})")

    df = pd.read_csv(
        csv_path,
        usecols=[
            "소노사업장", "경쟁사명", "OTA", "체크인", "객실유형",
            "객실카테고리", "판매가(원)", "판매상태", "URL", "is_own", "is_promo",
        ],
        dtype=str,
    )

    # 경쟁사만 (is_own=False)
    df = df[df["is_own"].str.lower() == "false"].copy()

    # 가격 정제
    df["price"] = pd.to_numeric(df["판매가(원)"], errors="coerce")
    df = df.dropna(subset=["price"])
    df = df[df["price"] > 0]

    # 판매 가능 건만
    if "판매상태" in df.columns:
        df = df[df["판매상태"].fillna("available").str.lower().isin(["available", "open", ""])]

    # is_promo: 플래그(컬럼) OR 패키지명 키워드
    flag = df["is_promo"].fillna("False").str.lower() == "true"
    name_promo = df["객실유형"].apply(is_promo_name)
    df["promo"] = flag | name_promo

    properties: dict = {}
    grouped = df.groupby(["소노사업장", "경쟁사명", "객실카테고리"], dropna=False)

    for (prop, comp, room_type), g in grouped:
        # 같은 경쟁사+같은 객실타입 → 최저가 1건만
        row = g.loc[g["price"].idxmin()]
        promo = {
            "name": (row["객실유형"] or "").strip(),
            "channel": (row["OTA"] or "").strip(),
            "price": int(round(row["price"])),
            "room_type": (room_type if isinstance(room_type, str) and room_type.strip() else "기타"),
            "check_in": (row["체크인"] or "").strip(),
            "is_promo": bool(row["promo"]),
            "url": (row["URL"] or "").strip(),
        }
        prop_node = properties.setdefault(prop, {"competitors": {}})
        comp_node = prop_node["competitors"].setdefault(comp, {"promotions": []})
        comp_node["promotions"].append(promo)

    # 각 경쟁사 프로모션을 가격 오름차순 정렬
    for prop_node in properties.values():
        for comp_node in prop_node["competitors"].values():
            comp_node["promotions"].sort(key=lambda x: x["price"])

    out = {
        "collected_at": collected_at_iso,
        "source_file": csv_path.name,
        "properties": properties,
    }

    targets = [ROOT / "data" / "promo_data.json", ROOT / "docs" / "data" / "promo_data.json"]
    for t in targets:
        t.parent.mkdir(parents=True, exist_ok=True)
    primary = targets[0]
    primary.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    shutil.copyfile(primary, targets[1])

    n_props = len(properties)
    n_comps = sum(len(p["competitors"]) for p in properties.values())
    n_promos = sum(
        len(c["promotions"]) for p in properties.values() for c in p["competitors"].values()
    )
    n_active = sum(
        1
        for p in properties.values()
        for c in p["competitors"].values()
        for x in c["promotions"]
        if x["is_promo"]
    )
    print(f"[done] 사업장 {n_props} · 경쟁사 {n_comps} · 프로모션 항목 {n_promos} (특가 {n_active})")
    print(f"[saved] {targets[0].relative_to(ROOT)}")
    print(f"[saved] {targets[1].relative_to(ROOT)}")


if __name__ == "__main__":
    main()
