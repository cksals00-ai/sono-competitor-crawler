"""
Power BI 공개 대시보드 - 채널별 판매객실수(RNS) 수집기

대상: GS OTB Status Dashboard v1.1
URL: https://app.powerbi.com/view?r=eyJrIjoiZWMwYmUyOTUtODg4MC00MmRkLWIyYWMtMzIxN2U5YzQyYjI0IiwidCI6IjJmOGNjOGE4LWE5YjAtNGY4Zi04ZjlmLWZiN2E3ZmQxM2ZmNCJ9

수집 방법:
  1. HTML에서 clusterUri 추출
  2. X-PowerBI-ResourceKey 헤더로 Analysis Services API 직접 호출
  3. DSR(Data Shape Result) 형식 파싱

출력:
  - data/powerbi_rns_YYYYMMDD.json  (날짜별 백업)
  - data/powerbi_rns_latest.json     (항상 최신 덮어쓰기)

실행:
  python powerbi_collector.py                                # 당월 투숙기준 (기본)
  python powerbi_collector.py --stay-month 202604            # 4월 투숙기준
  python powerbi_collector.py --cumulative                   # 연간 누적
  python powerbi_collector.py --discover                     # 스키마/페이지 탐색
  python powerbi_collector.py --output-dir ./data --pretty
"""

import json
import logging
import re
import uuid
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Power BI 공개 보고서 설정
# ─────────────────────────────────────────────
EMBED_URL = (
    "https://app.powerbi.com/view?"
    "r=eyJrIjoiZWMwYmUyOTUtODg4MC00MmRkLWIyYWMtMzIxN2U5YzQyYjI0Iiwidci"
    "6IjJmOGNjOGE4LWE5YjAtNGY4Zi04ZjlmLWZiN2E3ZmQxM2ZmNCJ9"
)
RESOURCE_KEY   = "ec0be295-8880-42dd-b2ac-3217e9c42b24"
TENANT_ID      = "2f8cc8a8-a9b0-4f8f-8f9f-fb7a7fd13ff4"
MODEL_ID       = 902554
DATASET_ID     = "8ee000d9-5efb-403f-83ad-9a8e3d3b80eb"
REPORT_ID      = "846569"

# 클러스터 URL — 페이지 로드 없이 라우팅 API로 확인하거나 하드코딩 사용
_CLUSTER_FALLBACK = "https://wabi-korea-central-a-primary-redirect.analysis.windows.net"

# ─────────────────────────────────────────────
# 유틸리티
# ─────────────────────────────────────────────

_BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": "https://app.powerbi.com",
    "Referer": "https://app.powerbi.com/",
    "X-PowerBI-ResourceKey": RESOURCE_KEY,
}


def _make_headers() -> dict:
    return {
        **_BASE_HEADERS,
        "ActivityId": str(uuid.uuid4()),
        "RequestId": str(uuid.uuid4()),
    }


def _apim_url(cluster_uri: str) -> str:
    """
    cluster URI → APIM URL 변환 (Power BI JS getAPIMUrl 로직 재현)
    예: https://wabi-korea-central-a-primary-redirect.analysis.windows.net/
      → https://wabi-korea-central-a-primary-api.analysis.windows.net
    """
    hostname = cluster_uri.rstrip("/").split("//")[-1]
    parts = hostname.split(".")
    parts[0] = parts[0].replace("-redirect", "").replace("global-", "") + "-api"
    return "https://" + ".".join(parts)


# ─────────────────────────────────────────────
# Step 1: 클러스터 URI 취득
# ─────────────────────────────────────────────

def get_cluster_uri() -> str:
    """
    라우팅 API로 테넌트의 클러스터 URI를 확인.
    실패하면 페이지 HTML에서 추출, 그것도 실패하면 하드코딩된 값 사용.
    """
    # 먼저 fallback URI로 APIM URL 계산 후 routing 호출
    apim = _apim_url(_CLUSTER_FALLBACK)
    url = f"{apim}/public/routing/cluster/{TENANT_ID}"
    try:
        r = requests.get(url, headers=_make_headers(), timeout=15)
        if r.status_code == 200:
            cluster = r.json().get("FixedClusterUri", "").rstrip("/")
            if cluster:
                logger.info(f"클러스터 URI: {cluster}")
                return cluster
    except Exception as e:
        logger.warning(f"라우팅 API 실패: {e}")

    # HTML에서 추출 시도
    try:
        r = requests.get(EMBED_URL, headers={
            "User-Agent": _BASE_HEADERS["User-Agent"],
            "Accept": "text/html",
        }, timeout=20)
        m = re.search(
            r'"FixedClusterUri"\s*:\s*"(https?://[^"]+)"', r.text
        )
        if m:
            cluster = m.group(1).rstrip("/")
            logger.info(f"HTML에서 클러스터 URI 추출: {cluster}")
            return cluster
    except Exception as e:
        logger.warning(f"HTML 클러스터 추출 실패: {e}")

    logger.info(f"하드코딩된 클러스터 URI 사용: {_CLUSTER_FALLBACK}")
    return _CLUSTER_FALLBACK


# ─────────────────────────────────────────────
# Step 2: querydata 호출
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
# 스키마 / 페이지 탐색
# ─────────────────────────────────────────────

def discover_schema(apim_cluster: str) -> None:
    """
    Power BI Analysis Services conceptualschema API를 호출해서
    data_raw 테이블의 엔티티와 컬럼 목록을 출력한다.

    실행: python powerbi_collector.py --discover
    """
    # 1. conceptualschema — 테이블/컬럼 목록 (POST + modelId/datasetId)
    schema_url = f"{apim_cluster}/public/reports/conceptualschema"
    schema_body = {
        "ModelIds":   [MODEL_ID],
        "DatasetIds": [DATASET_ID],
    }
    logger.info(f"conceptualschema 호출: {schema_url}")
    try:
        headers = {**_make_headers(), "Content-Type": "application/json"}
        r = requests.post(schema_url, headers=headers, json=schema_body, timeout=30)
        if r.status_code != 200:
            logger.warning(f"conceptualschema POST 응답 {r.status_code}, GET 재시도")
            r = requests.get(schema_url, headers=_make_headers(), timeout=30)
        r.raise_for_status()
        schema = r.json()
        # 응답 형식: { "schemas": [{ "modelId": ..., "schema": { "Entities": [...] } }] }
        # 또는 구버전: { "schema": { "Entities": [...] } }
        schemas_list = schema.get("schemas", [])
        if schemas_list:
            all_entities: list = []
            for s in schemas_list:
                ents = s.get("schema", {}).get("Entities", [])
                all_entities.extend(ents)
        else:
            raw_schema = schema.get("schema", schema)
            all_entities = raw_schema.get("Entities", raw_schema.get("entities", []))

        print(f"\n{'='*60}")
        print(f"[스키마 엔티티 목록]  (총 {len(all_entities)}개)")
        print(f"{'='*60}")
        if all_entities:
            for ent in all_entities:
                name = ent.get("Name", ent.get("name", ""))
                props = ent.get("Properties", ent.get("properties", []))
                print(f"\n  ▶ {name}  (컬럼 {len(props)}개)")
                for p in props:
                    pname = p.get("Name", p.get("name", ""))
                    ptype = p.get("DataType", p.get("dataType", ""))
                    print(f"      - {pname}  [{ptype}]")
        else:
            print("  (엔티티 없음 — 원본 응답 출력)")
            print(json.dumps(schema, ensure_ascii=False, indent=2)[:2000])
    except Exception as e:
        logger.error(f"conceptualschema 호출 실패: {e}")

    # 2. 리포트 페이지 목록
    pages_url = f"{apim_cluster}/public/reports/{REPORT_ID}/pages"
    logger.info(f"pages 목록 호출: {pages_url}")
    try:
        r = requests.get(pages_url, headers=_make_headers(), timeout=30)
        if r.status_code == 200:
            pages_data = r.json()
            pages = pages_data if isinstance(pages_data, list) else pages_data.get("value", [])
            print(f"\n{'='*60}")
            print(f"[리포트 페이지 목록]  (총 {len(pages)}개)")
            print(f"{'='*60}")
            for pg in pages:
                pg_name    = pg.get("displayName", pg.get("name", ""))
                pg_order   = pg.get("order", "")
                pg_id      = pg.get("name", "")
                visuals    = pg.get("visuals", [])
                print(f"  [{pg_order:>2}] {pg_name}  (id={pg_id}, visuals={len(visuals)})")
                for v in visuals:
                    v_id   = v.get("visualId", v.get("name", ""))
                    v_type = v.get("visualType", "")
                    v_title= v.get("title", "")
                    print(f"        visual_id={v_id}  type={v_type}  title={v_title}")
        else:
            logger.warning(f"pages API 응답 {r.status_code}: {r.text[:200]}")
    except Exception as e:
        logger.error(f"pages 목록 호출 실패: {e}")


def _wrap_query(sem_query: dict, n_projections: int) -> dict:
    """SemanticQuery를 querydata API 요청 바디로 감싼다."""
    return {
        "version": "1.0.0",
        "queries": [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": sem_query,
                                "Binding": {
                                    "Primary": {
                                        "Groupings": [
                                            {"Projections": list(range(n_projections))}
                                        ]
                                    },
                                    "DataReduction": {
                                        "DataVolume": 4,
                                        "Primary": {"Window": {"Count": 1000}},
                                    },
                                    "Version": 1,
                                },
                            }
                        }
                    ]
                },
                "QueryId": "",
                "ApplicationContext": {
                    "DatasetId": DATASET_ID,
                    "Sources": [{"ReportId": REPORT_ID, "VisualId": ""}],
                },
            }
        ],
        "cancelQueries": [],
        "modelId": MODEL_ID,
    }


def _build_ty_channel_query(stay_month: str | None = None) -> dict:
    """
    TY 전용 쿼리: data_raw + DimAgent만 사용.
    data_lastraw를 포함하지 않으므로 cross-filter 간섭 없음.

    Where: d.월=M AND d.투숙년도=YYYY
    Select: G0(AGENT명), G1(영업장변경), M0(RNS), M1(REV)
    """
    sem_query: dict = {
        "Version": 2,
        "From": [
            {"Name": "d",  "Entity": "data_raw", "Type": 0},
            {"Name": "d2", "Entity": "DimAgent",  "Type": 0},
        ],
        "Select": [
            {
                "Column": {
                    "Expression": {"SourceRef": {"Source": "d2"}},
                    "Property": "AGENT명",
                },
                "Name": "DimAgent.AGENT명",
            },
            {
                "Column": {
                    "Expression": {"SourceRef": {"Source": "d"}},
                    "Property": "영업장변경",
                },
                "Name": "data_raw.영업장변경",
            },
            {
                "Aggregation": {
                    "Expression": {
                        "Column": {
                            "Expression": {"SourceRef": {"Source": "d"}},
                            "Property": "RNS",
                        }
                    },
                    "Function": 0,
                },
                "Name": "Sum(data_raw.RNS)",
            },
            {
                "Aggregation": {
                    "Expression": {
                        "Column": {
                            "Expression": {"SourceRef": {"Source": "d"}},
                            "Property": "REV",
                        }
                    },
                    "Function": 0,
                },
                "Name": "Sum(data_raw.REV)",
            },
        ],
        "OrderBy": [
            {
                "Direction": 2,
                "Expression": {
                    "Aggregation": {
                        "Expression": {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "d"}},
                                "Property": "RNS",
                            }
                        },
                        "Function": 0,
                    }
                },
            }
        ],
    }

    if stay_month and len(stay_month) == 6 and stay_month.isdigit():
        month_int = int(stay_month[4:6])
        year_int  = int(stay_month[:4])
        sem_query["Where"] = [
            {
                "Condition": {
                    "Comparison": {
                        "ComparisonKind": 0,
                        "Left": {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "d"}},
                                "Property": "월",
                            }
                        },
                        "Right": {"Literal": {"Value": f"{month_int}L"}},
                    }
                }
            },
            {
                "Condition": {
                    "Comparison": {
                        "ComparisonKind": 0,
                        "Left": {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "d"}},
                                "Property": "투숙년도",
                            }
                        },
                        "Right": {"Literal": {"Value": f"{year_int}L"}},
                    }
                }
            },
        ]
        logger.info(f"TY 필터: 월={month_int}, 투숙년도={year_int}")

    return _wrap_query(sem_query, n_projections=4)


def _build_ly_channel_query(stay_month: str | None = None) -> dict:
    """
    LY 전용 쿼리: data_lastraw + DimAgent만 사용.
    data_raw를 포함하지 않으므로 cross-filter 간섭 없음.

    Where: d1.월=M AND d1.투숙년도_last=YYYY-1
    Select: G0(AGENT명), G1(영업장변경), M0(RNS_last)

    주의: data_lastraw에 '영업장변경' 컬럼이 없으면 ValueError 발생 →
          --discover로 컬럼명 확인 후 수정 필요.
    """
    sem_query: dict = {
        "Version": 2,
        "From": [
            {"Name": "d1", "Entity": "data_lastraw", "Type": 0},
            {"Name": "d2", "Entity": "DimAgent",      "Type": 0},
        ],
        "Select": [
            {
                "Column": {
                    "Expression": {"SourceRef": {"Source": "d2"}},
                    "Property": "AGENT명",
                },
                "Name": "DimAgent.AGENT명",
            },
            {
                "Column": {
                    "Expression": {"SourceRef": {"Source": "d1"}},
                    "Property": "영업장변경",
                },
                "Name": "data_lastraw.영업장변경",
            },
            {
                "Aggregation": {
                    "Expression": {
                        "Column": {
                            "Expression": {"SourceRef": {"Source": "d1"}},
                            "Property": "RNS_last",
                        }
                    },
                    "Function": 0,
                },
                "Name": "Sum(data_lastraw.RNS_last)",
            },
        ],
        "OrderBy": [
            {
                "Direction": 2,
                "Expression": {
                    "Aggregation": {
                        "Expression": {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "d1"}},
                                "Property": "RNS_last",
                            }
                        },
                        "Function": 0,
                    }
                },
            }
        ],
    }

    if stay_month and len(stay_month) == 6 and stay_month.isdigit():
        month_int = int(stay_month[4:6])
        ly_year   = int(stay_month[:4]) - 1
        sem_query["Where"] = [
            {
                "Condition": {
                    "Comparison": {
                        "ComparisonKind": 0,
                        "Left": {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "d1"}},
                                "Property": "월",
                            }
                        },
                        "Right": {"Literal": {"Value": f"{month_int}L"}},
                    }
                }
            },
            {
                "Condition": {
                    "Comparison": {
                        "ComparisonKind": 0,
                        "Left": {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "d1"}},
                                "Property": "투숙년도_last",
                            }
                        },
                        "Right": {"Literal": {"Value": f"{ly_year}L"}},
                    }
                }
            },
        ]
        logger.info(f"LY 필터: 월={month_int}, 투숙년도_last={ly_year}")

    return _wrap_query(sem_query, n_projections=3)


def _check_powerbi_error(result: dict) -> None:
    """Power BI 쿼리 오류 응답 감지 후 ValueError 발생."""
    if "error" in result or (
        result.get("results") and
        result["results"][0].get("result", {}).get("error")
    ):
        err_msg = (
            result.get("error")
            or result["results"][0]["result"].get("error", {})
        )
        raise ValueError(
            f"Power BI 쿼리 오류 (컬럼명이나 필터 값을 확인하세요): {err_msg}"
        )


def fetch_raw_data(
    apim_cluster: str,
    stay_month: str | None = None,
    date_column: str = "월",  # 하위 호환 유지 (현재 미사용)
) -> dict:
    """
    TY/LY 쿼리를 각각 별도 API로 호출한 뒤 {"ty": ..., "ly": ...} 반환.

    분리 이유: 단일 쿼리에서 data_raw+data_lastraw를 From에 함께 넣으면
    Power BI cross-filter로 LY에 연도 필터가 걸리지 않아 모든 연도 합산됨.
    """
    url = f"{apim_cluster}/public/reports/querydata?synchronous=true"
    headers = {**_make_headers(), "Content-Type": "application/json"}

    # ── TY 쿼리 ──
    ty_body = _build_ty_channel_query(stay_month=stay_month)
    logger.info(f"TY querydata API 호출: {url}")
    r = requests.post(url, headers=headers, json=ty_body, timeout=30)
    r.raise_for_status()
    ty_result = r.json()
    _check_powerbi_error(ty_result)
    logger.info("TY 데이터 수신 완료")

    # ── LY 쿼리 ──
    ly_body = _build_ly_channel_query(stay_month=stay_month)
    logger.info(f"LY querydata API 호출: {url}")
    r = requests.post(url, headers=headers, json=ly_body, timeout=30)
    r.raise_for_status()
    ly_result = r.json()
    _check_powerbi_error(ly_result)
    logger.info("LY 데이터 수신 완료")

    return {"ty": ty_result, "ly": ly_result}


# ─────────────────────────────────────────────
# Step 3: DSR 파싱
# ─────────────────────────────────────────────

def _parse_dsr(result: dict) -> list[dict]:
    """
    Power BI DSR(Data Shape Result) 형식 파싱.

    컬럼 순서: G0(채널), G1(사업장), M0(RNS), M1(REV), M2(RNS_last)
    R 비트마스크: 비트 i=1 → i번째 컬럼을 이전 행에서 반복

    반환: [{"channel", "property", "rns", "rev_만원", "rns_ly"}, ...]
    """
    data_section = result["results"][0]["result"]["data"]
    dsr = data_section["dsr"]
    ds  = dsr["DS"][0]

    value_dicts = ds.get("ValueDicts", {})
    d0 = value_dicts.get("D0", [])  # 채널 사전
    d1 = value_dicts.get("D1", [])  # 사업장 사전

    dm0    = ds["PH"][0]["DM0"]
    n_cols = 5  # G0, G1, M0, M1, M2

    rows      = []
    prev_vals = [None] * n_cols

    for entry in dm0:
        r_flag = entry.get("R", 0)
        c_vals = entry.get("C", [])

        new_vals = list(prev_vals)
        c_idx = 0
        for i in range(n_cols):
            if not ((r_flag >> i) & 1):
                new_vals[i] = c_vals[c_idx] if c_idx < len(c_vals) else None
                c_idx += 1

        g0_idx, g1_idx = new_vals[0], new_vals[1]
        channel  = d0[g0_idx] if (g0_idx is not None and 0 <= g0_idx < len(d0)) else ""
        property_= d1[g1_idx] if (g1_idx is not None and 0 <= g1_idx < len(d1)) else ""
        rns      = new_vals[2]
        rev_raw  = new_vals[3]
        rns_ly   = new_vals[4]

        rev_만원 = None
        if rev_raw is not None:
            try:
                rev_만원 = round(float(rev_raw))
            except (ValueError, TypeError):
                rev_만원 = None

        rows.append({
            "channel":   channel,
            "property":  property_,
            "rns":       rns,
            "rev_만원":  rev_만원,
            "rns_ly":    rns_ly,
        })

        prev_vals = new_vals

    return rows


def _parse_ty_dsr(result: dict) -> list[dict]:
    """
    TY 쿼리 DSR 파싱 (4 컬럼: G0=채널, G1=사업장, M0=RNS, M1=REV).
    반환: [{"channel", "property", "rns", "rev_만원"}, ...]
    """
    data_section = result["results"][0]["result"]["data"]
    ds = data_section["dsr"]["DS"][0]

    value_dicts = ds.get("ValueDicts", {})
    d0 = value_dicts.get("D0", [])
    d1 = value_dicts.get("D1", [])

    dm0    = ds["PH"][0]["DM0"]
    n_cols = 4
    rows      = []
    prev_vals = [None] * n_cols

    for entry in dm0:
        r_flag = entry.get("R", 0)
        c_vals = entry.get("C", [])
        new_vals = list(prev_vals)
        c_idx = 0
        for i in range(n_cols):
            if not ((r_flag >> i) & 1):
                new_vals[i] = c_vals[c_idx] if c_idx < len(c_vals) else None
                c_idx += 1

        g0_idx, g1_idx = new_vals[0], new_vals[1]
        channel   = d0[g0_idx] if (g0_idx is not None and 0 <= g0_idx < len(d0)) else ""
        property_ = d1[g1_idx] if (g1_idx is not None and 0 <= g1_idx < len(d1)) else ""
        rns       = new_vals[2]
        rev_raw   = new_vals[3]

        rev_만원 = None
        if rev_raw is not None:
            try:
                rev_만원 = round(float(rev_raw))
            except (ValueError, TypeError):
                rev_만원 = None

        rows.append({
            "channel":  channel,
            "property": property_,
            "rns":      rns,
            "rev_만원": rev_만원,
        })
        prev_vals = new_vals

    return rows


def _parse_ly_dsr(result: dict) -> list[dict]:
    """
    LY 쿼리 DSR 파싱 (3 컬럼: G0=채널, G1=사업장, M0=RNS_last).
    반환: [{"channel", "property", "rns_ly"}, ...]
    """
    data_section = result["results"][0]["result"]["data"]
    ds = data_section["dsr"]["DS"][0]

    value_dicts = ds.get("ValueDicts", {})
    d0 = value_dicts.get("D0", [])
    d1 = value_dicts.get("D1", [])

    dm0    = ds["PH"][0]["DM0"]
    n_cols = 3
    rows      = []
    prev_vals = [None] * n_cols

    for entry in dm0:
        r_flag = entry.get("R", 0)
        c_vals = entry.get("C", [])
        new_vals = list(prev_vals)
        c_idx = 0
        for i in range(n_cols):
            if not ((r_flag >> i) & 1):
                new_vals[i] = c_vals[c_idx] if c_idx < len(c_vals) else None
                c_idx += 1

        g0_idx, g1_idx = new_vals[0], new_vals[1]
        channel   = d0[g0_idx] if (g0_idx is not None and 0 <= g0_idx < len(d0)) else ""
        property_ = d1[g1_idx] if (g1_idx is not None and 0 <= g1_idx < len(d1)) else ""
        rns_ly    = new_vals[2]

        rows.append({
            "channel":  channel,
            "property": property_,
            "rns_ly":   rns_ly,
        })
        prev_vals = new_vals

    return rows


def parse_channel_rns(result: dict) -> dict:
    """
    DSR 파싱 후 사업장 × 채널 구조로 변환.

    반환 형식:
    {
      "collected_at": "2026-04-17T12:00:00",
      "properties": {
        "01.벨비발디": {
          "total_rns": 108959,
          "total_rns_ly": 250578,
          "channels": {
            "OTA_놀유니버스(야놀자)": {"rns": 21682, "rev_만원": 4286, "rns_ly": 66140},
            ...
          }
        },
        ...
      },
      "channels_summary": {
        "OTA_놀유니버스(야놀자)": {"total_rns": 99999, "total_rns_ly": 88888},
        ...
      }
    }
    """
    # 새 형식: {"ty": ..., "ly": ...}
    if isinstance(result, dict) and "ty" in result and "ly" in result:
        ty_rows = _parse_ty_dsr(result["ty"])
        ly_rows = _parse_ly_dsr(result["ly"])
    else:
        # 레거시: 단일 쿼리 결과 (discover 등에서 직접 호출 시)
        ty_rows = _parse_dsr(result)
        ly_rows = []

    # (channel, property) 키로 LY 맵 구성
    ly_map: dict[tuple[str, str], int] = {}
    for row in ly_rows:
        key = (row["channel"], row["property"])
        ly_map[key] = (ly_map.get(key) or 0) + (row.get("rns_ly") or 0)

    properties: dict[str, dict] = {}
    channels_total: dict[str, dict] = {}

    for row in ty_rows:
        channel  = row["channel"]
        property_= row["property"]
        rns      = row.get("rns") or 0
        rev      = row.get("rev_만원")
        # 빈 channel = 사업장 소계 행 → 스킵 (totals는 직접 계산)
        if not channel:
            continue

        rns_ly = ly_map.get((channel, property_), 0)

        prop_entry = properties.setdefault(property_, {
            "total_rns": 0,
            "total_rns_ly": 0,
            "channels": {},
        })
        prop_entry["channels"][channel] = {
            "rns":      rns,
            "rev_만원": rev,
            "rns_ly":   rns_ly,
        }
        prop_entry["total_rns"]    += rns
        prop_entry["total_rns_ly"] += rns_ly

        ch_entry = channels_total.setdefault(channel, {"total_rns": 0, "total_rns_ly": 0})
        ch_entry["total_rns"]    += rns
        ch_entry["total_rns_ly"] += rns_ly

    return {
        "collected_at":     datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "properties":       properties,
        "channels_summary": channels_total,
    }


# ─────────────────────────────────────────────
# Step 3b: 기존 channel_sales_data.json 포맷 변환
# ─────────────────────────────────────────────

# Power BI 채널명 → 기존 대시보드 채널 표시명 매핑
CHANNEL_NAME_MAP: dict[str, str] = {
    "OTA_놀유니버스(야놀자)": "야놀자",
    "OTA_여기어때컴퍼니":    "여기어때",
    "GOTA_아고다":          "아고다",
    "GOTA_트립닷컴":        "트립닷컴",
    "OTA_스마트인피니(객실)": "스마트인피니",
    "OTA_타이드스퀘어":      "타이드스퀘어",
    "GOTA_익스피디아":       "익스피디아",
    "OTA_쿠팡":            "쿠팡",
    "OTA_네이버":           "네이버",
    "GOTA_부킹닷컴":        "부킹닷컴",
    "GOTA_트립비토즈":       "트립비토즈",
    "OTA_맥스모바일":        "맥스모바일",
    "OTA_올마이투어":        "올마이투어",
    "OTA_웹투어":           "웹투어",
    "OTA_마이리얼트립":      "마이리얼트립",
    "OTA_프리즘":           "프리즘",
    "OTA_종이비행기":        "종이비행기",
    "OTA_코이스토리":        "코이스토리",
    "OTA_트립토파즈":        "트립토파즈",
    "GOTA_컴바인":          "컴바인",
    "GOTA_디다트래블":       "디다트래블",
    "GOTA_기타":            "GOTA기타",
}

# Power BI 사업장명(번호.이름) → 기존 대시보드 사업장명 매핑
PROPERTY_NAME_MAP: dict[str, str] = {
    "01.벨비발디":       "소노벨 비발디파크",
    "03.펫비발디":       "소노펫 비발디파크",
    "04.캄비발디":       "소노캄 비발디파크",
    "04.펠리체 비발디":  "소노펠리체 비발디파크",
    "05.빌리지 비발디":  "소노빌리지 비발디파크",
    "06.양평":          "소노벨 양평",
    "07.델피노":        "소노벨 델피노",
    "08.양양":          "소노벨 양양",
    "09.삼척":          "소노벨 삼척",
    "10.단양":          "소노벨 단양",
    "11.경주":          "소노벨 경주",
    "12.청송":          "소노벨 청송",
    "13.천안":          "소노벨 천안",
    "14.변산":          "소노벨 변산",
    "15.여수":          "소노벨 여수",
    "16.거제":          "소노벨 거제",
    "17.진도":          "소노벨 진도",
    "18.벨제주":        "소노벨 제주",
    "19.캄제주":        "소노캄 제주",
    "20.고양":          "소노호텔 고양",
    "21.해운대":        "소노호텔 해운대",
    "22.남해":          "소노벨 남해",
    "23.르네블루":       "르네블루",
}


def to_channel_sales_format(data: dict, stay_month: str | None = None) -> dict:
    """
    powerbi_rns_latest.json 형식 → channel_sales_data.json 호환 형식 변환.

    기존 channel_sales_data.json 구조:
    {
      "date": "YYYY-MM-DD",
      "label": "...",
      "channels": [...],
      "properties": [
        {
          "key": "벨비발디",
          "property_names": ["소노벨 비발디파크"],
          "channels": {"야놀자": {"rns": N, "prev": N}, ...},
          "total": {"rns": N, "prev": N}
        }
      ]
    }

    Args:
        data:        parse_channel_rns() 반환값
        stay_month:  "YYYYMM" (예: "202604") 또는 None (누적)
    """
    collected_at = data.get("collected_at", "")
    date_str = collected_at[:10] if collected_at else datetime.now().strftime("%Y-%m-%d")
    year = date_str[:4]

    # 레이블 결정
    if stay_month and len(stay_month) == 6:
        month_num = int(stay_month[4:6])
        label = f"{month_num}월 투숙기준 (Power BI)"
    else:
        label = f"{year}년 누적 (Power BI)"

    # 전체 채널 목록 (정규화된 이름, OTA/GOTA + RNS>0 필터)
    all_channels_set: set[str] = set()
    for prop_data in data["properties"].values():
        for raw_ch, ch_data in prop_data["channels"].items():
            if not (raw_ch.startswith("OTA") or raw_ch.startswith("GOTA")):
                continue
            if (ch_data.get("rns") or 0) <= 0:
                continue
            normalized = CHANNEL_NAME_MAP.get(raw_ch, raw_ch)
            all_channels_set.add(normalized)

    # 채널 목록을 전체 RNS 합계 기준 내림차순 정렬
    channel_total_rns: dict[str, int] = {}
    for raw_ch, ch_data in data["channels_summary"].items():
        if not (raw_ch.startswith("OTA") or raw_ch.startswith("GOTA")):
            continue
        normalized = CHANNEL_NAME_MAP.get(raw_ch, raw_ch)
        channel_total_rns[normalized] = (
            channel_total_rns.get(normalized, 0) + (ch_data.get("total_rns") or 0)
        )
    all_channels = sorted(
        all_channels_set,
        key=lambda ch: channel_total_rns.get(ch, 0),
        reverse=True,
    )

    properties_out = []
    for raw_prop, prop_data in data["properties"].items():
        display_name = PROPERTY_NAME_MAP.get(raw_prop, raw_prop)

        # 채널별 RNS (OTA/GOTA + RNS>0 필터, RNS 내림차순 정렬)
        channels_out: dict[str, dict] = {}
        for raw_ch, ch_data in prop_data["channels"].items():
            if not (raw_ch.startswith("OTA") or raw_ch.startswith("GOTA")):
                continue
            rns = ch_data.get("rns") or 0
            if rns <= 0:
                continue
            normalized = CHANNEL_NAME_MAP.get(raw_ch, raw_ch)
            channels_out[normalized] = {
                "rns":  rns,
                "prev": ch_data.get("rns_ly") or 0,
            }
        channels_out = dict(
            sorted(channels_out.items(), key=lambda x: x[1]["rns"], reverse=True)
        )

        properties_out.append({
            "key":            raw_prop,
            "property_names": [display_name],
            "channels":       channels_out,
            "total": {
                "rns":  prop_data.get("total_rns") or 0,
                "prev": prop_data.get("total_rns_ly") or 0,
            },
        })

    # RNS 내림차순 정렬
    properties_out.sort(key=lambda x: x["total"]["rns"], reverse=True)

    return {
        "date":       date_str,
        "label":      label,
        "source":     "powerbi",
        "channels":   all_channels,
        "properties": properties_out,
    }


# ─────────────────────────────────────────────
# Step 4: 저장
# ─────────────────────────────────────────────

def save_result(data: dict, output_dir: str = "./data", pretty: bool = True) -> tuple[Path, Path]:
    """
    날짜별 파일 + latest 파일 저장.
    반환: (날짜별 경로, latest 경로)
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y%m%d")
    dated_path  = out / f"powerbi_rns_{today}.json"
    latest_path = out / "powerbi_rns_latest.json"

    indent = 2 if pretty else None
    payload = json.dumps(data, ensure_ascii=False, indent=indent)

    dated_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")

    logger.info(f"저장 완료: {dated_path}")
    logger.info(f"최신 파일 갱신: {latest_path}")
    return dated_path, latest_path


# ─────────────────────────────────────────────
# 메인 실행
# ─────────────────────────────────────────────

def collect(
    output_dir: str = "./data",
    pretty: bool = True,
    update_channel_sales: bool = False,
    stay_month: str | None = None,
    date_column: str = "월",
) -> dict:
    """
    전체 수집 파이프라인 실행. 구조화된 데이터 반환.

    Args:
        output_dir:           저장 디렉토리
        pretty:               JSON 들여쓰기 여부
        update_channel_sales: True이면 channel_sales_data.json도 갱신
        stay_month:           "YYYYMM" 형식 투숙월 필터.
                              None이면 당월 자동 적용. "0"이면 누적(필터 없음).
        date_column:          data_raw 테이블의 날짜 컬럼명 (기본: "월")
    """
    # 기본값: 당월 자동 적용
    if stay_month is None:
        now = datetime.now()
        stay_month = f"{now.year}{now.month:02d}"
        logger.info(f"투숙월 미지정 → 당월({stay_month}) 자동 적용")
    elif stay_month == "0":
        stay_month = None  # 누적 모드
        logger.info("누적 모드 (월 필터 없음)")

    cluster_uri = get_cluster_uri()
    apim        = _apim_url(cluster_uri)
    logger.info(f"APIM 엔드포인트: {apim}")

    raw    = fetch_raw_data(apim, stay_month=stay_month, date_column=date_column)
    parsed = parse_channel_rns(raw)

    n_props    = len(parsed["properties"])
    n_channels = len(parsed["channels_summary"])
    logger.info(f"파싱 완료 — 사업장 {n_props}개, 채널 {n_channels}개")

    save_result(parsed, output_dir=output_dir, pretty=pretty)

    if update_channel_sales:
        compat = to_channel_sales_format(parsed, stay_month=stay_month)
        compat_path = Path("channel_sales_data.json")
        compat_path.write_text(
            json.dumps(compat, ensure_ascii=False, indent=2 if pretty else None),
            encoding="utf-8",
        )
        logger.info(f"channel_sales_data.json 갱신: {compat_path.resolve()}")

    return parsed


def print_summary(data: dict) -> None:
    """수집 결과 콘솔 요약 출력"""
    print(f"\n{'='*60}")
    print(f"수집 시각: {data['collected_at']}")
    print(f"{'='*60}")

    print("\n[채널별 전체 RNS 합계]")
    channels = sorted(
        data["channels_summary"].items(),
        key=lambda x: x[1]["total_rns"],
        reverse=True,
    )
    for ch, v in channels[:20]:
        print(f"  {ch:<35} RNS={v['total_rns']:>8,}  LY={v['total_rns_ly']:>8,}")

    print("\n[사업장별 전체 RNS 합계]")
    props = sorted(
        data["properties"].items(),
        key=lambda x: x[1]["total_rns"],
        reverse=True,
    )
    for prop, v in props:
        ch_count = len(v["channels"])
        print(f"  {prop:<25} RNS={v['total_rns']:>8,}  LY={v['total_rns_ly']:>8,}  채널수={ch_count}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Power BI GS OTB RNS 수집기")
    parser.add_argument("--output-dir", default="./data",  help="저장 디렉토리 (기본: ./data)")
    parser.add_argument("--no-pretty",            action="store_true", help="JSON 압축 저장")
    parser.add_argument("--summary",              action="store_true", help="콘솔 요약 출력")
    parser.add_argument("--update-channel-sales", action="store_true",
                        help="channel_sales_data.json 도 함께 갱신 (대시보드 연동)")
    parser.add_argument("--discover",             action="store_true",
                        help="스키마/페이지 탐색 후 종료 (데이터 수집 없음)")
    parser.add_argument("--stay-month",           default=None, metavar="YYYYMM",
                        help="투숙월 필터 (예: 202604). 미지정시 당월 자동 적용")
    parser.add_argument("--cumulative",           action="store_true",
                        help="연간 누적 데이터 수집 (월 필터 없음)")
    parser.add_argument("--date-column",          default="월",
                        help="data_raw 테이블의 월 컬럼명 (기본: 월, --discover로 확인 가능)")
    args = parser.parse_args()

    cluster_uri = get_cluster_uri()
    apim_base   = _apim_url(cluster_uri)

    if args.discover:
        discover_schema(apim_base)
    else:
        stay_month = "0" if args.cumulative else args.stay_month
        result = collect(
            output_dir=args.output_dir,
            pretty=not args.no_pretty,
            update_channel_sales=args.update_channel_sales,
            stay_month=stay_month,
            date_column=args.date_column,
        )

        if args.summary:
            print_summary(result)
