# 소노 경쟁사 크롤러 — 제작 과정 리포트

> 작성 기준: 2026-04-17 | GS Team

---

## 1. 프로젝트 시작 배경

소노호텔앤리조트(이하 소노)의 GS Team은 국내 주요 OTA(온라인 여행사)에서 경쟁 숙박시설과 자사 가격 차이를 매일 수작업으로 확인하고 있었다. 야놀자·여기어때·Agoda·Booking.com 등 4~6개 채널을 22개 사업장 × 경쟁사 2~5곳씩 30일치 가격을 매일 점검하는 것은 현실적으로 불가능했다.

**목적:**
1. 주요 OTA에서 경쟁사 대비 소노 가격을 매일 자동으로 수집
2. 결과를 HTML 대시보드(GitHub Pages)와 Excel(Power BI)로 배포
3. 요일/연휴 기준 가격 분포 분석 지원

---

## 2. 주요 마일스톤

### 2026-04-15 — 프로젝트 시작

| 커밋 | 내용 |
|------|------|
| `6c22f05` | Initial commit: 야놀자·여기어때·Booking.com·Agoda 크롤러 + 초기 대시보드 |
| `752c98f` | GitHub Pages 배포 설정 (`docs/` 폴더 연동) |

- 야놀자(requests), 여기어때(Selenium), Booking.com(Selenium), Agoda(Selenium) 4채널 초기 구현
- 7개 사업장, 각 30일치 크롤링
- 단순 HTML 테이블 대시보드 첫 버전

---

### 2026-04-16 — 대규모 기능 확장 (하루에 ~20개 커밋)

이날 하루 동안 거의 모든 핵심 기능이 추가되었다.

#### 사업장 확대
| 커밋 | 내용 |
|------|------|
| `818d485` | 팔라티움 해운대 추가, 소노문 해운대 경쟁사 수정 |
| `69f37b3` | 소노벨 델피노 추가 |
| `bc099ab` | 소노캄 델피노 추가 (강원 동해시) |
| `53cdc83` | 소노펠리체 비발디/델피노 카드 + 단지별 정렬 + 벨비발디 패밀리 FIT 요금 |

#### 크롤러 기능 개선
| 커밋 | 내용 |
|------|------|
| `5fc2b85` | 단계별 크롤링(run_phased.py) + Agoda 수정 + 별점 기능 |
| `9100ad3` | 소노 자사홈 크롤러 추가 + 대시보드 자사홈 컬럼 반영 |
| `183d2fc` | 자사 FIT 요금 Excel 파싱(parse_fit_rates.py) + 대시보드 통합 |
| `ca8c4cd` | 자사홈 크롤러 개선 + phase 0(자사홈) 단계 추가 |

#### 대시보드 UI 개선
| 커밋 | 내용 |
|------|------|
| `d28f074` | Noto Sans KR 폰트, 2열 그리드, 인라인 별점, 저작권 푸터 |
| `3b0ddc8` | 3열 그리드 (1200px+ 화면) |
| `ff955c8` | **버그 수정**: 한글 컬럼명 CSV 대시보드 렌더링 오류 |
| `a9c46ca` | 제목 '소노 경쟁사 모니터링 \| GS Team'으로 변경 |
| `2ff6559` | 신규 배지: 전일 데이터 없을 때(첫날) 숨김 처리 |

---

### 2026-04-17 — 3채널 통합 + 공휴일 연동

| 커밋 | 내용 |
|------|------|
| `63958aa` | 네이버호텔 크롤러 추가 (GraphQL API) |
| `131ddb7` | Trip.com 크롤러 추가 (SSR HTML 파싱) |
| `68a819e` | 두 크롤러 main 브랜치에 통합 |
| `0e6bd28` | 공휴일 API(data.go.kr) 연동 + 연휴 크롤링 날짜 자동 결정 |
| `3e8dda6` | 대실(day use) 필터링 + run_phased 병렬 실행 지원 |
| `48bf090` | 야놀자 RSC 페이로드에서 대실 가격 필터링 추가 |
| `8b5d97c` | 대시보드에 네이버호텔·Trip.com 채널 컬럼 추가 |
| `6956a31` | **첫 3채널 자동 수집**: 야놀자+Trip.com+네이버 2026-04-17 08:05 |
| `b4b1e1e` | 대시보드 3개 수정 + GitHub Pages 동기화 |

---

## 3. 각 단계에서 만난 문제점과 해결

### 3-1. 야놀자: HTML 파싱 → RSC JSON 파싱

**문제:** 야놀자(nol.yanolja.com)는 Next.js 기반 앱으로, 브라우저에서 직접 렌더링한 HTML에는 가격이 포함되지 않는다. Selenium으로 렌더링을 기다려도 가격 DOM이 불안정했다.

**해결:** Next.js의 RSC(React Server Components) 방식을 분석하여, `requests`로 일반 HTTP 요청 시 응답 body에 포함되는 RSC 페이로드(JSON 스트림)를 직접 파싱하는 방식으로 전환했다.
- `_extract_rooms_from_decoded_chunk()`: Base64 디코딩된 RSC 청크에서 방 정보 추출
- `_parse_yanolja_rooms()`: HTML 내 RSC 데이터 파싱

**추가 문제 (2026-04-17):** RSC 데이터에 대실(DayUse) 상품이 섞여서 숙박 가격이 오염됨.
**해결:** `_is_dayuse()` 함수로 상품명에 "대실", "DayUse", "day use" 등 포함 여부 체크, 해당 상품 필터링.

---

### 3-2. 여기어때 / Booking.com / Agoda: Selenium 불안정

**문제:** 세 채널 모두 JS 렌더링이 필요해 Selenium headless Chrome을 사용한다. 문제:
- Agoda는 국가 선택 팝업, 로그인 유도 팝업, 쿠키 배너가 자주 뜸
- Selenium 드라이버를 매 크롤링마다 생성하면 메모리 낭비 및 속도 저하

**해결:**
- `_driver` 싱글톤: 세션 전체에서 드라이버 1개만 유지 (`_get_driver()` / `close_driver()`)
- `_agoda_close_popups()`: Agoda 진입 시 팝업 자동 닫기
- Agoda `no_room_data` 재시도: `_retry_agoda_errors()` 함수로 실패 건만 재크롤링

---

### 3-3. 자사홈 크롤러: 로그인 세션 관리

**문제:** 소노 공식 홈페이지 가격은 회원 로그인 후에만 FIT 요금이 노출된다. 매 요청마다 로그인하면 비효율적이고 계정 잠금 위험도 있다.

**해결:** `_sono_session` 싱글톤 + `_sono_login()` 함수로 최초 1회만 로그인, 이후 세션 재사용. `_sono_mem_no` / `_sono_user_ind_cd` 상태값을 모듈 레벨에서 유지.

**추가 문제:** 자사홈 API 응답 구조가 변경될 경우 파싱이 깨짐.  
**현재 상태:** `_SHOW_HOMEPAGE_SECTION = False`로 대시보드 섹션 비활성화 (API 수정 완료 전까지 보류).

---

### 3-4. 네이버호텔: GraphQL API 역공학

**문제:** 네이버호텔(hotels.naver.com)은 내부 GraphQL API를 통해 여러 OTA의 가격을 집계한다. 이 API를 직접 호출하면 여러 채널 가격을 한 번에 얻을 수 있지만, API 엔드포인트와 OTA 코드 체계를 파악해야 했다.

**해결:**
- 엔드포인트: `https://hermes-hotel-svc-api.naver.com/graphql`
- JS 번들 분석으로 OTA 코드 14개 추출 (`NAVER_OTA_NAMES` 딕셔너리: `"NYNJ": "야놀자"`, `"NCTE": "Trip.com"` 등)
- `_naver_find_hotel_id()`: 호텔 이름으로 ID 자동 검색
- `crawl_naver()`: GraphQL 쿼리로 날짜별 가격 수집

---

### 3-5. Trip.com: SSR HTML 파싱

**문제:** Trip.com의 개별 호텔 페이지는 JS 렌더링이 필요하지만, 도시 목록 페이지는 SSR(서버사이드 렌더링)로 가격이 HTML에 이미 포함된다.

**해결:** 도시 페이지(`_fetch_tripcom_city_prices()`)에서 한 번에 여러 호텔 가격을 수집하고, `_tripcom_city_cache`로 캐싱하여 중복 요청 방지. hotel_id가 0인 경우(도시 페이지 없음)는 `_fetch_tripcom_pricerange()`로 개별 조회.

---

### 3-6. 공휴일 연동: API vs 하드코딩

**문제:** 연휴 크롤링 날짜를 config에서 수작업으로 관리하면 매년 업데이트가 필요하고 누락 가능성이 있다.

**해결:**
- `_fetch_korean_holidays_year()`: 공공데이터포털(data.go.kr) 공휴일 API를 연도별로 호출
- `_add_korean_substitute_holidays()`: 대체공휴일 자동 계산 로직
- `_holiday_cache`: 연도별 캐싱으로 중복 API 호출 방지
- `_get_holiday_blocks()`: 연속된 공휴일을 블록 단위로 묶어 크롤링 대상 날짜 생성
- 대시보드(`dashboard_generator.py`)에는 `HOLIDAYS` frozenset으로 2025~2026 하드코딩도 병행 유지

---

### 3-7. 대시보드: 한글 컬럼명 버그

**문제 (커밋 `ff955c8`):** export_powerbi.py가 내부 영어 컬럼명(property_name, ota 등)을 한글로 변환한 CSV를 저장하는데, 대시보드 생성기가 영어 컬럼명을 기대하고 있어 렌더링이 깨졌다.

**해결:** 대시보드 생성기에서 영어/한글 컬럼명을 모두 처리하도록 컬럼 탐지 로직 추가. `run_phased.py`의 `_merge_and_save()`에서도 `"ota" if "ota" in df.columns else "OTA"` 패턴으로 대응.

---

### 3-8. 소노문 해운대 매핑 오류

**문제 (커밋 `9db886f`):** 채널 실적 매핑에서 소노문 해운대를 소노벨로 잘못 연결하여 판매객실수 데이터가 틀린 사업장에 표시됨.

**해결:** config.yaml에서 property id `해운대`를 별도로 정의, 대시보드 매핑 수정.

---

## 4. 기술적 의사결정 근거

### 왜 야놀자를 requests + RSC로 처리하나?

야놀자는 Next.js App Router 기반으로, 서버에서 RSC 페이로드를 HTTP 응답에 포함시킨다. Selenium 없이 `requests`만으로 가격 데이터를 추출할 수 있어:
- 속도가 Selenium 대비 10배 이상 빠름
- 봇 탐지 가능성이 낮음 (일반 HTTP 요청처럼 보임)
- 메모리 사용량이 낮음

### 왜 네이버호텔을 GraphQL로 처리하나?

네이버호텔 페이지는 JS 렌더링이 필요하지만, GraphQL API를 직접 호출하면:
- 여러 OTA(야놀자·Trip.com·Agoda·여기어때 등) 가격을 한 번의 요청으로 수집
- Selenium 없이 requests만 사용 가능
- 구조화된 JSON 응답으로 파싱 안정성이 높음

### 왜 Trip.com을 SSR HTML로 처리하나?

Trip.com 도시 목록 페이지는 검색엔진 최적화를 위해 서버에서 미리 렌더링된 HTML을 제공한다. JS 실행 없이 BeautifulSoup으로 파싱 가능하고, 도시 내 여러 호텔을 한 번에 수집할 수 있어 효율적이다.

### 왜 여기어때·Booking.com·Agoda는 Selenium을 쓰나?

세 채널은 모두 JS로 가격을 동적으로 렌더링하며, SSR 데이터나 내부 API를 찾기 어렵다. Selenium headless Chrome이 현실적인 유일한 방법이다.

### 왜 단계별 크롤링(run_phased.py)인가?

전체 22개 사업장 × 6개 채널 × 30일치를 한 번에 실행하면 2~3시간 이상 소요된다. 단계별로 실행하면:
- 야놀자(빠름) 완료 즉시 부분 대시보드 배포 가능
- 특정 채널만 재수집 가능 (`--phase 1`)
- 오류가 나도 완료된 단계 데이터는 보존

---

## 5. 현재 상태 요약 (2026-04-17)

| 항목 | 내용 |
|------|------|
| 활성 채널 | 야놀자, 네이버호텔, Trip.com (3채널 매일 자동 수집) |
| 비활성 채널 | 여기어때, Agoda, Booking.com, 자사홈 (크롤러 구현 완료, 대시보드 표시 보류) |
| 사업장 수 | 22개 (국내 21개 + 베트남 1개) |
| 크롤링 범위 | 오늘부터 30일 앞 (연휴 포함 자동 결정) |
| 배포 방식 | GitHub Pages (docs/index.html) + iCloud Drive HTML |
| 데이터 저장 | exports/ 날짜별 CSV + XLSX + latest.xlsx (Power BI 연동) |
| 마지막 수집 | 2026-04-17 08:05 (3채널, exports/ 약 90MB) |
| 자동화 | run_sequential_3ch.py 수동 실행 or launchd (설정 진행 중) |
