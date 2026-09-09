#!/usr/bin/env python3
"""
build_competitor_dashboard.py
경쟁사 가격 대시보드용 집계 산출물 생성.

소스 (로컬만 사용 — 외장 아카이브 미사용):
  analytics/hotel_trends.csv   호텔 일자별 정규화 집계 (preprocess_trends.py 산출)
  analytics/golf_trends.csv    골프 일자별 정규화 집계
  config.yaml                  권역/사업장/경쟁세트 정의

산출 (docs/data/cd/):
  meta.json              축 트리 + 데이터 품질 플래그
  overview.json          일자 x 권역 롤업
  gap.json               일자 x 사업장 소노-경쟁사 격차
  alerts.json            급등락 / 격차 전환 / rate parity
  golf.json              골프 전체
  prop/{property_id}.json  사업장별 상세 (lazy load 대상)

원칙: 더미 데이터 없음. 값이 없으면 null.
실행:  python3 scripts/build_competitor_dashboard.py
"""
from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parent.parent
# 아카이브 복원본(hotel_trends_full.csv)이 있으면 그걸 쓴다.
# 누적본 재기록 과정에서 유실된 자사/경쟁사 행이 복원되어 있어 비교 가능 구간이 훨씬 넓다.
_FULL = ROOT / "analytics" / "hotel_trends_full.csv"
HOTEL_SRC = _FULL if _FULL.exists() else ROOT / "analytics" / "hotel_trends.csv"
GOLF_SRC = ROOT / "analytics" / "golf_trends.csv"
CONFIG = ROOT / "config.yaml"
OUT = ROOT / "docs" / "data" / "cd"
PROP_OUT = OUT / "prop"

# ── 튜닝 파라미터 ────────────────────────────────────────────────────────────
TIER_SAME_LO, TIER_SAME_HI = 0.75, 1.35   # 자사 대비 중앙가 배수 → '동급' 구간
TIER_MIN_N = 1000                          # 체급 판정에 필요한 최소 표본
LOWDATA_DAY_THRESHOLD = 60                 # 수집일수 이 미만이면 '데이터 부족'
EARLY_UNSTABLE_UNTIL = "2026-04-19"        # 이 날짜까지는 초기 불안정 구간
SPIKE_DOD = 5.0                            # 전일 대비 ±% 급등락
SPIKE_VS_MED = 10.0                        # 7일 이동중앙값 대비 ±%
SPIKE_MIN_N = 10                           # 급등락 판정 최소 표본
PARITY_THRESHOLD = 5.0                     # 채널 간 자사가 격차 ±%
GAP_SHIFT_PP = 10.0                        # 격차 추세 전환 판정 (%p)


def log(msg: str) -> None:
    print(msg, flush=True)


def r0(x):
    """반올림 정수 or None (NaN 금지 — JSON에 null로)."""
    if x is None:
        return None
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return int(round(f))


def r1(x):
    if x is None:
        return None
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return round(f, 1)


def dump(obj, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    log(f"  → {path.relative_to(ROOT)}  ({path.stat().st_size/1024:.0f}KB)")


# ── 1. config 로드 ───────────────────────────────────────────────────────────

def load_config():
    cfg = yaml.safe_load(open(CONFIG, encoding="utf-8"))
    props = {}
    for p in cfg.get("properties") or []:
        pid = p.get("id")
        if not pid:
            continue
        comps = []
        for c in p.get("competitors") or []:
            comps.append(c.get("name") if isinstance(c, dict) else str(c))
        props[pid] = {
            "property_id": pid,
            "property_name": p.get("name"),
            "region": p.get("display_region"),
            "address_region": p.get("region"),
            "config_competitors": comps,
        }
    golf = {}
    for p in cfg.get("golf_properties") or []:
        pid = p.get("id")
        if not pid:
            continue
        comps = []
        for c in p.get("competitors") or []:
            comps.append(c.get("name") if isinstance(c, dict) else str(c))
        golf[pid] = {
            "property_id": pid,
            "property_name": p.get("name"),
            "region": p.get("region"),
            "config_competitors": comps,
        }
    return props, golf


# ── 2. 호텔 데이터 로드 + 정규화 ─────────────────────────────────────────────

def load_hotel(cfg_props: dict):
    log(f"호텔 로드: {HOTEL_SRC.name} ({HOTEL_SRC.stat().st_size/1048576:.0f}MB)")
    df = pd.read_csv(HOTEL_SRC, low_memory=False)
    log(f"  {len(df):,} 행")

    df["is_own"] = df["is_own"].astype(str).str.lower().isin(["true", "1"])
    for c in ["price_avg", "price_min", "price_max", "availability_rate",
              "available_cnt", "sold_out_cnt", "total_cnt"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    notes = {}

    # 2-1. config에 없는 legacy property_id 제거 (초기 1~2일만 존재)
    known = set(cfg_props)
    legacy = sorted(set(df["property_id"].dropna().unique()) - known)
    if legacy:
        lg = (df[df.property_id.isin(legacy)]
              .groupby("property_id")
              .agg(rows=("property_id", "size"),
                   name=("property_name", "first"),
                   d_min=("crawl_date", "min"), d_max=("crawl_date", "max"),
                   n_days=("crawl_date", "nunique")))
        notes["excluded_legacy_properties"] = [
            {"property_id": pid, "property_name": r["name"], "rows": int(r["rows"]),
             "days": int(r["n_days"]), "range": f"{r['d_min']}~{r['d_max']}"}
            for pid, r in lg.iterrows()
        ]
        log(f"  config 미등록 property_id {len(legacy)}개 제외: {legacy}")
        df = df[~df.property_id.isin(legacy)].copy()

    # 2-2. 사업장 명칭 분열 → config 명칭으로 통일 (property_id 기준)
    split = (df.groupby("property_id")["property_name"].nunique())
    split_ids = split[split > 1].index.tolist()
    if split_ids:
        detail = []
        for pid in split_ids:
            names = sorted(df.loc[df.property_id == pid, "property_name"].unique())
            detail.append({"property_id": pid, "names_found": names,
                           "unified_to": cfg_props[pid]["property_name"]})
        notes["merged_property_names"] = detail
        log(f"  명칭 분열 통일: {split_ids}")
    df["property_name"] = df["property_id"].map(
        lambda p: cfg_props[p]["property_name"]).astype(str)

    # 2-3. 자기 자신이 경쟁사로 잡힌 행 제거
    own_names = {pid: cfg_props[pid]["property_name"] for pid in cfg_props}
    self_mask = (~df["is_own"]) & (
        df["competitor_name"].astype(str).str.replace(" ", "") ==
        df["property_id"].map(own_names).astype(str).str.replace(" ", "")
    )
    # 명칭 분열 흔적(팔리티움 등)까지 잡기 위해 원본 이름과도 대조
    if self_mask.any():
        notes["removed_self_as_competitor"] = int(self_mask.sum())
        log(f"  자기 자신=경쟁사 행 제거: {int(self_mask.sum()):,}")
        df = df[~self_mask].copy()

    df["region"] = df["property_id"].map(lambda p: cfg_props[p]["region"])
    return df, notes


# ── 3. 경쟁사 체급(가격대) 태그 ──────────────────────────────────────────────

def build_tiers(df: pd.DataFrame, cfg_props: dict):
    """자사 중앙가 대비 경쟁사 중앙가 배수로 체급 태그.
    브랜드 등급이 아니라 '관측된 가격대' 기준임을 라벨에 명시한다."""
    tiers = {}
    warnings = []
    for pid, sub in df.groupby("property_id"):
        own_med = sub.loc[sub.is_own, "price_avg"].median()
        if pd.isna(own_med) or own_med <= 0:
            tiers[pid] = {"own_median": None, "competitors": [], "no_same_tier": True}
            warnings.append({"property_id": pid, "issue": "자사 가격 표본 없음"})
            continue
        rows = []
        cs = sub[~sub.is_own]
        for cname, csub in cs.groupby("competitor_name"):
            med = csub["price_avg"].median()
            n = int(csub["price_avg"].notna().sum())
            if pd.isna(med) or med <= 0 or n == 0:
                rows.append({"name": cname, "median": None, "ratio": None,
                             "n": n, "tier": "표본없음", "default": False})
                continue
            ratio = med / own_med
            if n < TIER_MIN_N:
                tier = "표본부족"
            elif ratio > TIER_SAME_HI:
                tier = "상위"
            elif ratio < TIER_SAME_LO:
                tier = "하위"
            else:
                tier = "동급"
            rows.append({"name": cname, "median": r0(med), "ratio": round(ratio, 2),
                         "n": n, "tier": tier, "default": tier == "동급"})
        same = [r for r in rows if r["tier"] == "동급"]
        no_same = len(same) == 0
        if no_same:
            # 동급이 없으면 표본 충분한 경쟁사 중 배수가 1에 가장 가까운 1곳을 기본값으로.
            cand = [r for r in rows if r["ratio"] is not None and r["n"] >= TIER_MIN_N]
            if cand:
                near = min(cand, key=lambda r: abs(math.log(r["ratio"])))
                near["default"] = True
                warnings.append({
                    "property_id": pid,
                    "property_name": cfg_props[pid]["property_name"],
                    "issue": "동급(가격대 0.75~1.35배) 경쟁사 없음",
                    "detail": f"전 경쟁사가 자사 중앙가({r0(own_med):,}원)의 "
                              f"{min(r['ratio'] for r in cand):.2f}~{max(r['ratio'] for r in cand):.2f}배. "
                              f"기본 비교 대상은 최근접인 '{near['name']}'({near['ratio']:.2f}배) 1곳으로 대체.",
                })
        tiers[pid] = {
            "own_median": r0(own_med),
            "competitors": sorted(rows, key=lambda r: (r["ratio"] is None, r["ratio"] or 0)),
            "no_same_tier": no_same,
        }
    return tiers, warnings


# ── 4. 사업장별 상세 산출물 ─────────────────────────────────────────────────

def _series_block(g: pd.DataFrame, dates: list[str]) -> dict:
    """(crawl_date 인덱스) → 날짜축에 정렬된 컬럼형 배열."""
    idx = {d: i for i, d in enumerate(dates)}
    n = len(dates)
    avg = [None] * n
    lo = [None] * n
    hi = [None] * n
    cnt = [None] * n
    av = [None] * n
    for d, row in g.iterrows():
        i = idx.get(d)
        if i is None:
            continue
        avg[i] = r0(row.get("price_avg"))
        lo[i] = r0(row.get("price_min"))
        hi[i] = r0(row.get("price_max"))
        c = row.get("n")
        cnt[i] = int(c) if pd.notna(c) else None
        av[i] = r1(row.get("availability_rate") * 100
                   if pd.notna(row.get("availability_rate")) else None)
    return {"avg": avg, "min": lo, "max": hi, "n": cnt, "avail": av}


def _agg(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    return df.groupby(keys).agg(
        price_avg=("price_avg", "mean"),
        price_min=("price_min", "min"),
        price_max=("price_max", "max"),
        availability_rate=("availability_rate", "mean"),
        n=("price_avg", "count"),
    )


def build_property(pid: str, sub: pd.DataFrame, meta: dict, tiers: dict,
                   all_dates: list[str]) -> dict:
    dates = sorted(sub["crawl_date"].unique())
    priced = sub[sub["price_avg"].notna()]

    channels = sorted(priced["ota"].dropna().unique())
    comps = sorted(priced.loc[~priced.is_own, "competitor_name"].dropna().unique())
    cats = sorted(priced["room_category"].dropna().unique())

    own = priced[priced.is_own]
    comp = priced[~priced.is_own]

    out = {
        "property_id": pid,
        "property_name": meta["property_name"],
        "region": meta["region"],
        "address_region": meta["address_region"],
        "dates": dates,
        "channels": channels,
        "competitors": tiers[pid]["competitors"],
        "own_median": tiers[pid]["own_median"],
        "no_same_tier": tiers[pid]["no_same_tier"],
        "room_categories": cats,
        "rows": int(len(sub)),
    }

    # 4-1. 전 채널 통합 시계열 (자사 / 경쟁사 전체)
    out["daily"] = {
        "own": _series_block(_agg(own, ["crawl_date"]), dates),
        "comp_all": _series_block(_agg(comp, ["crawl_date"]), dates),
    }

    # 4-2. 채널별 시계열
    ch_own, ch_comp = {}, {}
    for ota, g in own.groupby("ota"):
        ch_own[ota] = _series_block(_agg(g, ["crawl_date"]), dates)
    for ota, g in comp.groupby("ota"):
        ch_comp[ota] = _series_block(_agg(g, ["crawl_date"]), dates)
    out["by_channel"] = {"own": ch_own, "comp": ch_comp}

    # 4-3. 경쟁사별 시계열
    out["by_competitor"] = {
        cname: _series_block(_agg(g, ["crawl_date"]), dates)
        for cname, g in comp.groupby("competitor_name")
    }

    # 4-4. 객실카테고리별 시계열
    out["by_category"] = {
        "own": {c: _series_block(_agg(g, ["crawl_date"]), dates)
                for c, g in own.groupby("room_category")},
        "comp": {c: _series_block(_agg(g, ["crawl_date"]), dates)
                 for c, g in comp.groupby("room_category")},
    }

    # 4-5. 투숙일(체크인) 축 — 최신 수집일 기준 스냅샷
    if len(dates):
        last = dates[-1]
        snap = priced[priced.crawl_date == last]
        ci = sorted(snap["checkin_date"].dropna().unique())
        idx = {d: i for i, d in enumerate(ci)}

        def ci_block(g):
            arr = [None] * len(ci)
            for d, row in g.iterrows():
                i = idx.get(d)
                if i is not None:
                    arr[i] = r0(row["price_avg"])
            return arr

        out["by_checkin"] = {
            "as_of": last,
            "checkin_dates": ci,
            "own": ci_block(_agg(snap[snap.is_own], ["checkin_date"])),
            "comp_all": ci_block(_agg(snap[~snap.is_own], ["checkin_date"])),
            "comp": {c: ci_block(_agg(g, ["checkin_date"]))
                     for c, g in snap[~snap.is_own].groupby("competitor_name")},
        }
    else:
        out["by_checkin"] = None

    return out


# ── 5. 격차 / 급등락 ────────────────────────────────────────────────────────

def pct_change(new, old):
    if new is None or old is None or old == 0:
        return None
    return (new - old) / old * 100


def build_gap_and_alerts(df: pd.DataFrame, cfg_props: dict, tiers: dict):
    """소노 vs 경쟁사 격차 추이 + 급등락/전환 알림."""
    gap_out = {}
    alerts = []

    for pid, sub in df.groupby("property_id"):
        priced = sub[sub["price_avg"].notna()]
        if not len(priced):
            continue
        name = cfg_props[pid]["property_name"]
        dates = sorted(priced["crawl_date"].unique())

        # 기본 비교 대상 = 동급(없으면 최근접 1곳)
        default_comps = [c["name"] for c in tiers[pid]["competitors"] if c["default"]]
        own_s = priced[priced.is_own].groupby("crawl_date")["price_avg"].mean()
        own_n = priced[priced.is_own].groupby("crawl_date")["price_avg"].count()
        comp_all = priced[~priced.is_own].groupby("crawl_date")["price_avg"].mean()
        comp_def = (priced[(~priced.is_own) &
                           (priced.competitor_name.isin(default_comps))]
                    .groupby("crawl_date")["price_avg"].mean())

        own_a = [r0(own_s.get(d)) for d in dates]
        call_a = [r0(comp_all.get(d)) for d in dates]
        cdef_a = [r0(comp_def.get(d)) for d in dates]
        gap_all = [r1(pct_change(o, c)) for o, c in zip(own_a, call_a)]
        gap_def = [r1(pct_change(o, c)) for o, c in zip(own_a, cdef_a)]

        gap_out[pid] = {
            "property_name": name,
            "region": cfg_props[pid]["region"],
            "dates": dates,
            "own": own_a,
            "comp_all": call_a,
            "comp_default": cdef_a,
            "gap_all_pct": gap_all,
            "gap_default_pct": gap_def,
            "default_competitors": default_comps,
            "no_same_tier": tiers[pid]["no_same_tier"],
        }

        # ── 알림 6: 일 단위 급등락 (자사/경쟁사 각각) ──────────────────────
        for side, arr, ncnt in (("자사", own_a, own_n),
                                ("경쟁사", call_a, None)):
            med7 = pd.Series(arr, dtype="float64").rolling(7, min_periods=4).median()
            for i in range(1, len(dates)):
                cur, prev = arr[i], arr[i - 1]
                if cur is None or prev is None:
                    continue
                if ncnt is not None:
                    c = ncnt.get(dates[i])
                    if c is None or c < SPIKE_MIN_N:
                        continue
                dod = pct_change(cur, prev)
                mv = med7.iloc[i - 1] if i - 1 < len(med7) else None
                vsmed = pct_change(cur, mv) if pd.notna(mv) else None
                hit_dod = dod is not None and abs(dod) >= SPIKE_DOD
                hit_med = vsmed is not None and abs(vsmed) >= SPIKE_VS_MED
                if hit_dod or hit_med:
                    alerts.append({
                        "type": "급등락",
                        "property_id": pid, "property_name": name,
                        "region": cfg_props[pid]["region"],
                        "date": dates[i], "side": side,
                        "price": cur, "prev": prev,
                        "dod_pct": r1(dod), "vs_med7_pct": r1(vsmed),
                        "severity": r1(max(abs(dod or 0), abs(vsmed or 0))),
                    })

        # ── 알림 7: 격차 추세 전환 ────────────────────────────────────────
        gs = pd.Series(gap_def, dtype="float64")
        ma = gs.rolling(7, min_periods=4).mean()
        for i in range(1, len(dates)):
            a, b = ma.iloc[i - 1], ma.iloc[i]
            if pd.isna(a) or pd.isna(b):
                continue
            if (a < 0 <= b) or (a >= 0 > b):
                alerts.append({
                    "type": "격차전환", "property_id": pid, "property_name": name,
                    "region": cfg_props[pid]["region"], "date": dates[i],
                    "from_pct": r1(a), "to_pct": r1(b),
                    "detail": "소노가 경쟁사보다 " + ("비싸짐" if b >= 0 else "싸짐"),
                    "severity": r1(abs(b - a)),
                })
            elif abs(b - a) >= GAP_SHIFT_PP:
                alerts.append({
                    "type": "격차급변", "property_id": pid, "property_name": name,
                    "region": cfg_props[pid]["region"], "date": dates[i],
                    "from_pct": r1(a), "to_pct": r1(b),
                    "detail": f"격차 {r1(b-a)}%p 이동",
                    "severity": r1(abs(b - a)),
                })

    return gap_out, alerts


def build_parity(df: pd.DataFrame, cfg_props: dict, last_date: str):
    """채널 간 자사가 불일치 — 최신 수집일 스냅샷."""
    own = df[(df.is_own) & (df.price_avg.notna()) & (df.crawl_date == last_date)]
    if not len(own):
        return []
    key = ["property_id", "checkin_date", "room_category"]
    p = own.groupby(key + ["ota"])["price_avg"].mean().reset_index()
    g = p.groupby(key)["price_avg"].agg(["min", "max", "count"])
    g = g[g["count"] >= 2]
    if not len(g):
        return []
    g["gap_pct"] = (g["max"] - g["min"]) / g["min"] * 100
    bad = g[g["gap_pct"] >= PARITY_THRESHOLD].sort_values("gap_pct", ascending=False)
    rows = []
    for (pid, ci, cat), r in bad.head(300).iterrows():
        # 최저/최고 채널 이름
        blk = p[(p.property_id == pid) & (p.checkin_date == ci) & (p.room_category == cat)]
        lo = blk.loc[blk.price_avg.idxmin()]
        hi = blk.loc[blk.price_avg.idxmax()]
        rows.append({
            "type": "채널가격불일치",
            "property_id": pid,
            "property_name": cfg_props[pid]["property_name"],
            "region": cfg_props[pid]["region"],
            "date": last_date, "checkin_date": ci, "room_category": cat,
            "min": r0(r["min"]), "max": r0(r["max"]),
            "min_channel": lo["ota"], "max_channel": hi["ota"],
            "n_channels": int(r["count"]),
            "gap_pct": r1(r["gap_pct"]), "severity": r1(r["gap_pct"]),
        })
    return rows


# ── 6. 권역 롤업 ────────────────────────────────────────────────────────────

def build_overview(df: pd.DataFrame):
    priced = df[df["price_avg"].notna()]
    dates = sorted(priced["crawl_date"].unique())
    idx = {d: i for i, d in enumerate(dates)}
    n = len(dates)

    def block(g):
        own = [None] * n
        comp = [None] * n
        gap = [None] * n
        cnt = [None] * n
        o = g[g.is_own].groupby("crawl_date")["price_avg"].mean()
        c = g[~g.is_own].groupby("crawl_date")["price_avg"].mean()
        k = g.groupby("crawl_date")["price_avg"].count()
        for d in g["crawl_date"].unique():
            i = idx.get(d)
            if i is None:
                continue
            own[i] = r0(o.get(d))
            comp[i] = r0(c.get(d))
            gap[i] = r1(pct_change(own[i], comp[i]))
            cnt[i] = int(k.get(d, 0))
        return {"own": own, "comp": comp, "gap_pct": gap, "n": cnt}

    regions = {}
    for reg, g in priced.groupby("region"):
        regions[reg] = block(g)
    return {
        "dates": dates,
        "total": block(priced),
        "by_region": regions,
    }


# ── 7. 골프 ─────────────────────────────────────────────────────────────────

def build_golf(cfg_golf: dict):
    if not GOLF_SRC.exists():
        return None
    g = pd.read_csv(GOLF_SRC, low_memory=False)
    log(f"골프 로드: {len(g):,} 행")
    g["is_own"] = g["is_own"].astype(str).str.lower().isin(["true", "1"])
    for c in ["fee_min_krw", "fee_avg_krw", "fee_max_krw"]:
        g[c] = pd.to_numeric(g[c], errors="coerce")
    g = g[g["fee_avg_krw"].notna()]
    dates = sorted(g["crawl_date"].unique())
    idx = {d: i for i, d in enumerate(dates)}

    def blk(sub):
        arr = [None] * len(dates)
        cnt = [None] * len(dates)
        s = sub.groupby("crawl_date")["fee_avg_krw"].mean()
        k = sub.groupby("crawl_date")["fee_avg_krw"].count()
        for d, v in s.items():
            i = idx.get(d)
            if i is not None:
                arr[i] = r0(v)
                cnt[i] = int(k.get(d, 0))
        return {"avg": arr, "n": cnt}

    props = {}
    for pid, sub in g.groupby("property_id"):
        meta = cfg_golf.get(pid, {})
        own = sub[sub.is_own]
        comp = sub[~sub.is_own]
        props[pid] = {
            "property_id": pid,
            "property_name": meta.get("property_name") or sub["property_name"].iloc[0],
            "region": meta.get("region"),
            "channels": sorted(sub["channel"].dropna().unique()),
            "competitors": sorted(comp["competitor_name"].dropna().unique()),
            "own": blk(own),
            "comp_all": blk(comp),
            "by_competitor": {c: blk(s) for c, s in comp.groupby("competitor_name")},
            "by_channel": {c: blk(s) for c, s in sub.groupby("channel")},
            "by_daytype": {c: blk(s) for c, s in sub.groupby("day_of_week")},
            "rows": int(len(sub)),
        }
    return {"dates": dates, "properties": props}


# ── main ────────────────────────────────────────────────────────────────────

def main():
    if not HOTEL_SRC.exists():
        sys.exit(f"없음: {HOTEL_SRC} — preprocess_trends.py 를 먼저 실행하세요")

    OUT.mkdir(parents=True, exist_ok=True)
    PROP_OUT.mkdir(parents=True, exist_ok=True)

    cfg_props, cfg_golf = load_config()
    df, notes = load_hotel(cfg_props)

    dates_all = sorted(df["crawl_date"].dropna().unique())
    d_min, d_max = dates_all[0], dates_all[-1]
    # 결측일
    full = pd.date_range(d_min, d_max, freq="D").strftime("%Y-%m-%d").tolist()
    missing = [d for d in full if d not in set(dates_all)]

    log("\n체급 태그 산출 ...")
    tiers, tier_warn = build_tiers(df, cfg_props)
    for w in tier_warn:
        log(f"  ⚠ {w.get('property_name', w['property_id'])}: {w['issue']}")

    log("\n권역 롤업 ...")
    overview = build_overview(df)
    dump(overview, OUT / "overview.json")

    log("\n격차 / 급등락 ...")
    gap, alerts = build_gap_and_alerts(df, cfg_props, tiers)
    parity = build_parity(df, cfg_props, d_max)
    alerts_all = alerts + parity
    alerts_all.sort(key=lambda a: (a.get("date") or "", -(a.get("severity") or 0)),
                    reverse=True)
    dump(gap, OUT / "gap.json")
    dump({"generated_from": d_max, "count": len(alerts_all),
          "rules": {
              "급등락": f"전일 대비 ±{SPIKE_DOD}% 또는 7일 이동중앙값 대비 ±{SPIKE_VS_MED}% "
                       f"(자사 표본 {SPIKE_MIN_N}건 이상)",
              "격차전환": "동급 경쟁사 대비 격차의 7일 이동평균이 부호를 바꾼 시점",
              "격차급변": f"동급 경쟁사 대비 격차 7일 이동평균이 {GAP_SHIFT_PP}%p 이상 이동",
              "채널가격불일치": f"최신 수집일 기준 같은 자사 상품의 채널 간 가격차 {PARITY_THRESHOLD}% 이상",
          },
          "items": alerts_all}, OUT / "alerts.json")

    log("\n사업장별 상세 ...")
    prop_index = []
    for pid, sub in df.groupby("property_id"):
        meta = cfg_props[pid]
        obj = build_property(pid, sub, meta, tiers, dates_all)
        dump(obj, PROP_OUT / f"{pid}.json")
        n_days = sub["crawl_date"].nunique()
        prop_index.append({
            "property_id": pid,
            "property_name": meta["property_name"],
            "region": meta["region"],
            "address_region": meta["address_region"],
            "days": int(n_days),
            "rows": int(len(sub)),
            "channels": int(sub["ota"].nunique()),
            "competitors": int(sub.loc[~sub.is_own, "competitor_name"].nunique()),
            "own_median": tiers[pid]["own_median"],
            "no_same_tier": tiers[pid]["no_same_tier"],
            "low_data": bool(n_days < LOWDATA_DAY_THRESHOLD),
        })

    log("\n골프 ...")
    golf = build_golf(cfg_golf)
    if golf:
        dump(golf, OUT / "golf.json")

    # 권역 트리
    regions = defaultdict(list)
    for p in prop_index:
        regions[p["region"]].append(p["property_id"])

    channels = sorted(df["ota"].dropna().unique())
    meta_obj = {
        "generated_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": {
            "hotel": str(HOTEL_SRC.relative_to(ROOT)),
            "golf": str(GOLF_SRC.relative_to(ROOT)) if GOLF_SRC.exists() else None,
            "config": "config.yaml",
            "external_archive_used": False,
        },
        "period": {"start": d_min, "end": d_max,
                   "days_present": len(dates_all),
                   "days_expected": len(full),
                   "missing_dates": missing,
                   "early_unstable_until": EARLY_UNSTABLE_UNTIL},
        "regions": [{"region": r, "properties": p} for r, p in regions.items()],
        "properties": sorted(prop_index, key=lambda x: (x["region"] or "", x["property_name"])),
        "channels": channels,
        "channel_note": ("OTA 15종 중 대부분이 '네이버호텔/*' 접두어입니다. "
                         "이는 해당 OTA의 직판가가 아니라 네이버호텔 메타서치에 노출된 "
                         "해당 OTA의 판매가입니다."),
        "tier_rule": {
            "basis": "관측된 price_avg 중앙값의 자사 대비 배수 (브랜드 등급 아님)",
            "동급": f"{TIER_SAME_LO}~{TIER_SAME_HI}배",
            "상위": f">{TIER_SAME_HI}배",
            "하위": f"<{TIER_SAME_LO}배",
            "표본부족": f"표본 {TIER_MIN_N}건 미만",
            "fallback": "동급이 없으면 표본 충분한 경쟁사 중 배수가 1에 가장 가까운 1곳을 기본 비교 대상으로 사용",
        },
        "tiers": tiers,
        "quality_warnings": tier_warn,
        "normalization_notes": notes,
        "golf": ({"properties": [{"property_id": k,
                                  "property_name": v["property_name"],
                                  "region": v["region"],
                                  "channels": v["channels"],
                                  "competitors": v["competitors"],
                                  "rows": v["rows"]}
                                 for k, v in golf["properties"].items()],
                  "period": {"start": golf["dates"][0], "end": golf["dates"][-1],
                             "days": len(golf["dates"])}}
                 if golf else None),
    }
    dump(meta_obj, OUT / "meta.json")

    log(f"\n완료. 사업장 {len(prop_index)}개 / 기간 {d_min}~{d_max} "
        f"({len(dates_all)}일, 결측 {len(missing)}일) / 알림 {len(alerts_all)}건")


if __name__ == "__main__":
    main()
