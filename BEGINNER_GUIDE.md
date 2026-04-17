# 소노 경쟁사 크롤러 — 초보자 가이드

> 이 문서는 파이썬을 처음 접하는 분도 따라할 수 있게 step-by-step으로 작성되었습니다.

---

## 목차

1. [개발 환경 설정](#1-개발-환경-설정)
2. [프로젝트 구조 설명](#2-프로젝트-구조-설명)
3. [config.yaml 설정 방법](#3-configyaml-설정-방법)
4. [첫 실행 전 준비](#4-첫-실행-전-준비)
5. [크롤링 실행 방법](#5-크롤링-실행-방법)
6. [대시보드 생성 및 배포](#6-대시보드-생성-및-배포)
7. [자동화 설정](#7-자동화-설정)
8. [문제 해결 FAQ](#8-문제-해결-faq)

---

## 1. 개발 환경 설정

### 1-1. Python 설치 확인

```bash
python3 --version
# Python 3.11.x 이상 권장
```

Python이 없으면 https://python.org 에서 설치하세요.

### 1-2. 프로젝트 폴더로 이동

```bash
cd ~/Projects/sono-competitor-crawler
```

### 1-3. 가상환경 생성 (권장)

```bash
# 가상환경 생성
python3 -m venv venv

# 활성화 (macOS/Linux)
source venv/bin/activate

# 활성화 (Windows)
venv\Scripts\activate
```

터미널 프롬프트 앞에 `(venv)` 가 붙으면 성공입니다.

### 1-4. 필수 패키지 설치

```bash
pip install requests
pip install beautifulsoup4
pip install pandas
pip install pyyaml
pip install selenium
pip install openpyxl
pip install schedule
```

한 번에 설치하는 방법:

```bash
pip install requests beautifulsoup4 pandas pyyaml selenium openpyxl schedule
```

### 1-5. Chrome + ChromeDriver 설치

여기어때·Agoda·Booking.com 크롤러는 Chrome 브라우저가 필요합니다.

1. Chrome 설치: https://www.google.com/chrome/
2. ChromeDriver는 Selenium이 자동으로 관리합니다 (selenium >= 4.6.0 기준)

설치 확인:
```bash
python3 -c "from selenium import webdriver; print('OK')"
```

---

## 2. 프로젝트 구조 설명

```
sono-competitor-crawler/
│
├── config.yaml              # 핵심 설정 파일 (사업장·경쟁사·OTA 정보)
│
├── crawler.py               # 메인 크롤러 (야놀자·네이버·Trip.com 등 7개 채널)
├── dashboard_generator.py   # HTML 대시보드 생성기
├── export_powerbi.py        # Excel/CSV 내보내기 (Power BI 연동)
├── parse_fit_rates.py       # 자사 FIT 요금 Excel → JSON 변환
│
├── run_sequential_3ch.py    # ★ 주요 실행 파일: 야놀자+Trip.com+네이버 3채널
├── run_phased.py            # 채널별 단계 실행 (--phase 1~5)
├── run_once.py              # launchd 자동화용 1회 실행
├── run_parallel.py          # 병렬 실행 (고급)
├── scheduler.py             # schedule 라이브러리 기반 반복 실행
├── test_all.py              # 테스트 스크립트
│
├── data/
│   └── fit_rates_source.xlsx  # 자사 FIT 요금 원본 Excel
│
├── exports/                 # 크롤링 결과 저장 폴더
│   ├── sono_competitor_prices_20260417.csv
│   ├── sono_competitor_prices_20260417.xlsx
│   └── sono_competitor_prices_latest.xlsx   # Power BI가 읽는 파일
│
├── dashboard/
│   └── index.html           # 생성된 대시보드 (브라우저에서 바로 열기 가능)
│
├── docs/
│   └── index.html           # GitHub Pages 배포용 (dashboard/와 동일)
│
├── logs/                    # 로그 파일 저장 폴더
│   ├── crawler.log
│   └── phased.log
│
└── fit_rates.json           # parse_fit_rates.py 실행 결과 (자사 FIT 요금)
```

**핵심 파일 역할 한줄 요약:**

| 파일 | 역할 |
|------|------|
| `config.yaml` | 어느 사업장의 어느 경쟁사를 어떤 OTA에서 수집할지 정의 |
| `crawler.py` | 실제로 웹사이트에서 가격을 가져오는 모든 함수 |
| `dashboard_generator.py` | 수집된 가격 데이터 → HTML 보고서로 변환 |
| `run_sequential_3ch.py` | 매일 실행하는 메인 스크립트 |

---

## 3. config.yaml 설정 방법

`config.yaml`은 프로젝트의 핵심 설정 파일입니다. 크게 세 섹션으로 나뉩니다.

### 3-1. 크롤링 설정

```yaml
crawl:
  days_ahead: 30          # 오늘부터 몇 일 후까지 수집할지 (기본: 30일)
  request_delay: 2        # 각 요청 사이 대기 시간 (초) — 너무 줄이면 차단됨
  retry_count: 3          # 실패 시 재시도 횟수
  timeout: 30             # 요청 타임아웃 (초)
  holiday_api_key: "..."  # 공공데이터포털 API 키 (공휴일 자동 연동용)
```

> **공휴일 API 키 발급:** https://www.data.go.kr 에서 무료 가입 후 "한국천문연구원 특일 정보" API 신청

### 3-2. 출력 설정

```yaml
output:
  export_dir: ./exports                                   # 저장 폴더
  excel_filename: sono_competitor_prices_{date}.xlsx      # Excel 파일명 (날짜 자동 삽입)
  csv_filename: sono_competitor_prices_{date}.csv         # CSV 파일명
  powerbi_filename: sono_competitor_prices_latest.xlsx    # Power BI 연동 파일 (덮어쓰기)
```

### 3-3. 사업장 및 경쟁사 설정

```yaml
properties:
- name: 소노벨 비발디파크       # 소노 사업장 이름
  id: vivaldi                   # 내부 식별자 (영문 소문자, 파일명 등에 사용)
  region: 강원도 홍천군          # 지역 (표시용)
  
  competitors:                  # 이 사업장의 경쟁 숙소 목록
  - name: 웰리힐리파크           # 경쟁사 이름 (대시보드에 표시됨)
    yanolja_url: https://nol.yanolja.com/stay/domestic/3016118   # 야놀자 URL
    yeogiuh_url: https://www.yeogi.com/domestic-accommodations/7083
    booking_url: https://www.booking.com/hotel/kr/...
    agoda_url: https://www.agoda.com/ko-kr/...
    naver_id: '11583166'        # 네이버호텔 ID (숫자, 따옴표 필수)
    tripcom_hotel_id: 3462424   # Trip.com 호텔 ID (0이면 수집 안 함)
    tripcom_city_id: 78905      # Trip.com 도시 ID (0이면 도시 페이지 없음)
  
  own_urls:                     # 소노 자사 OTA 등록 URL (자사 가격 수집용)
    yanolja_url: https://nol.yanolja.com/stay/domestic/3001803
    yeogiuh_url: https://www.yeogi.com/domestic-accommodations/6576
    booking_url: ''             # 빈 문자열 = 미등록
    agoda_url: ''
    naver_id: '12345678'
    tripcom_hotel_id: 0
    tripcom_city_id: 0
```

**새 사업장 추가하는 방법:**

1. OTA 사이트에서 해당 호텔을 검색
2. URL에서 ID를 복사 (야놀자: `/domestic/숫자`, 네이버: 검색 후 URL의 ID)
3. config.yaml에 새 항목 추가
4. YAML 문법 주의: 들여쓰기는 반드시 스페이스 2칸

**naver_id 찾는 방법:**
1. https://hotels.naver.com 에서 호텔 검색
2. 호텔 페이지 URL에서 숫자 확인 (예: `/hotels/hotelDetail?hotelId=11583166`)

**tripcom_hotel_id 찾는 방법:**
1. https://kr.trip.com 에서 호텔 검색
2. 호텔 상세 페이지 URL에서 숫자 확인 (예: `/hotels/detail/?hotelId=3462424`)

---

## 4. 첫 실행 전 준비

### 4-1. 자사 FIT 요금 업데이트 (선택사항)

자사 FIT 요금을 대시보드에 표시하려면:

```bash
# data/fit_rates_source.xlsx 파일을 최신 버전으로 교체 후
python3 parse_fit_rates.py
# → fit_rates.json 생성
```

### 4-2. exports/ 폴더 확인

```bash
ls -la exports/
```

처음 실행하면 자동 생성됩니다.

### 4-3. 테스트 실행 (빠른 검증)

본격 실행 전에 설정이 올바른지 확인:

```bash
python3 -c "
from crawler import load_config, run_crawl
cfg = load_config()
print('사업장 수:', len(cfg['properties']))
print('크롤 설정:', cfg['crawl'])
print('설정 파일 OK')
"
```

---

## 5. 크롤링 실행 방법

### 방법 A: 3채널 순차 실행 (권장 — 매일 사용)

야놀자 → Trip.com → 네이버호텔 순서로 실행하고, 완료 후 대시보드 자동 생성:

```bash
python3 run_sequential_3ch.py
```

- 소요 시간: 약 1~2시간
- 로그: `/tmp/sono_3ch_crawl.log` 에서 실시간 확인 가능
- 완료 후: `exports/` 에 날짜별 CSV·XLSX 생성 + `dashboard/index.html` + `docs/index.html` 업데이트 + git push

### 방법 B: 단일 채널만 실행

```bash
# 야놀자만
python3 run_phased.py --phase 1

# 네이버호텔만
python3 run_phased.py --phase 4

# Trip.com만
python3 run_phased.py --phase 5

# Agoda만
python3 run_phased.py --phase 2

# 여기어때만
python3 run_phased.py --phase 3

# 자사홈만
python3 run_phased.py --phase 0
```

각 단계 완료 시 자동으로 대시보드 재생성 + git push.

### 방법 C: 전체 6채널 순차 실행

```bash
python3 run_phased.py
# 0→1→2→3→4→5 순서로 전체 실행 (3~5시간 소요)
```

### 실행 중 로그 확인

별도 터미널에서:

```bash
# 3채널 실행 시
tail -f /tmp/sono_3ch_crawl.log

# run_phased 실행 시
tail -f logs/phased.log
```

---

## 6. 대시보드 생성 및 배포

### 6-1. 대시보드 수동 생성

크롤링 데이터가 이미 있을 때 대시보드만 다시 생성하려면:

```bash
python3 -c "
import pandas as pd
from dashboard_generator import generate_dashboard, load_previous_df

# 최신 CSV 로드
df = pd.read_csv('exports/sono_competitor_prices_latest.xlsx',
                 engine='openpyxl')  # 또는 CSV
prev_df = load_previous_df('./exports')
generate_dashboard(df, 'dashboard/index.html', prev_df=prev_df)
print('대시보드 생성 완료: dashboard/index.html')
"
```

### 6-2. 브라우저에서 대시보드 열기

```bash
open dashboard/index.html   # macOS
# 또는 파인더에서 dashboard/index.html 더블클릭
```

### 6-3. GitHub Pages 배포

```bash
# 1. docs/ 폴더에 복사
cp dashboard/index.html docs/index.html

# 2. git 커밋 & 푸시
git add docs/index.html
git commit -m "dashboard: 대시보드 업데이트 $(date '+%Y-%m-%d')"
git push
```

GitHub 저장소 Settings → Pages에서 Source를 `main` 브랜치 `/docs` 폴더로 설정하면 자동으로 웹에서 확인 가능합니다.

### 6-4. iCloud Drive 동기화

`run_phased.py`와 `run_sequential_3ch.py`는 완료 시 자동으로 `~/Library/Mobile Documents/com~apple~CloudDocs/소노_경쟁사_대시보드.html` 에 복사합니다.

---

## 7. 자동화 설정

### macOS launchd로 매일 자동 실행

**방법 1: run_once.py 사용**

```bash
# plist 파일 생성
cat > ~/Library/LaunchAgents/com.sono.crawler.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" ...>
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.sono.crawler</string>
    <key>ProgramArguments</key>
    <array>
        <string>/path/to/venv/bin/python3</string>
        <string>/Users/chanminpark/Projects/sono-competitor-crawler/run_sequential_3ch.py</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>7</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>WorkingDirectory</key>
    <string>/Users/chanminpark/Projects/sono-competitor-crawler</string>
    <key>StandardOutPath</key>
    <string>/tmp/sono_crawler.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/sono_crawler_err.log</string>
</dict>
</plist>
EOF

# 등록
launchctl load ~/Library/LaunchAgents/com.sono.crawler.plist
```

**방법 2: scheduler.py 백그라운드 실행**

```bash
nohup python3 scheduler.py &
echo $! > crawler.pid   # PID 저장
```

중지하려면:
```bash
kill $(cat crawler.pid)
```

---

## 8. 문제 해결 FAQ

### Q. "ModuleNotFoundError: No module named 'selenium'" 오류

**A.** 패키지가 설치되지 않았습니다.

```bash
pip install selenium
```

가상환경 사용 중이라면 `source venv/bin/activate` 먼저 실행 후 설치하세요.

---

### Q. Chrome 관련 오류 (WebDriverException)

**A.** Chrome이 설치되어 있는지 확인:

```bash
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --version
```

Chrome 버전과 ChromeDriver 버전이 맞지 않을 때는:

```bash
pip install --upgrade selenium
```

---

### Q. 야놀자 크롤링이 빈 결과 반환

**A.** 야놀자 URL 형식이 `https://nol.yanolja.com/stay/domestic/숫자` 인지 확인하세요. `https://www.yanolja.com/` 형식은 지원하지 않습니다.

```python
# 테스트 방법
from crawler import load_config, crawl_yanolja
cfg = load_config()
result = crawl_yanolja(
    {"name": "테스트", "yanolja_url": "https://nol.yanolja.com/stay/domestic/3016118"},
    "2026-05-01", "2026-05-02", cfg
)
print(result)
```

---

### Q. 네이버 GraphQL 오류 (401/403)

**A.** 네이버 API는 세션이 필요합니다. `_get_naver_session()` 함수가 올바른 헤더를 설정하는지 확인하세요. 일반적으로 `User-Agent`를 실제 브라우저 문자열로 변경하면 해결됩니다.

---

### Q. "no_room_data" 오류가 많이 발생

**A.** Agoda에서 자주 발생합니다. run_phased.py의 Agoda 단계(--phase 2)에는 자동 재시도 로직이 내장되어 있습니다. 반복적으로 발생한다면:
1. Agoda URL이 유효한지 직접 브라우저에서 확인
2. 해당 날짜에 실제로 판매 가능한 방이 없을 수 있음 (정상 상황)

---

### Q. 대시보드가 빈 화면으로 표시

**A.** exports/ 폴더에 오늘 날짜 CSV가 있는지 확인:

```bash
ls -la exports/
```

CSV가 있다면 대시보드 수동 생성을 시도:

```bash
python3 -c "
import pandas as pd
from dashboard_generator import generate_dashboard
df = pd.read_csv('exports/sono_competitor_prices_$(date +%Y%m%d).csv', encoding='utf-8-sig')
generate_dashboard(df, 'dashboard/index.html')
"
```

---

### Q. git push 시 "nothing to commit" 메시지

**A.** 정상입니다. 크롤링 결과가 이전과 동일하거나 변경사항이 없을 때 발생합니다.

---

### Q. config.yaml에 새 사업장을 추가했는데 대시보드에 안 보임

**A.** 새 사업장 추가 후 크롤링을 새로 실행해야 합니다. 기존 exports/ 파일을 재사용하면 새 사업장 데이터가 없습니다.

```bash
python3 run_phased.py --phase 1  # 야놀자 재수집
```

---

### Q. 대실(DayUse) 가격이 섞여서 이상하게 높은 가격이 표시됨

**A.** 이미 필터링 로직이 적용되어 있습니다. `crawler.py`의 `_is_dayuse()` 함수가 상품명에 "대실", "DayUse" 등이 포함된 항목을 자동 제외합니다. 새로운 대실 상품명 패턴이 생긴 경우 해당 함수에 패턴을 추가하세요.

---

### Q. 실행 중 터미널을 닫으면 크롤링이 중단됨

**A.** 백그라운드 실행:

```bash
nohup python3 run_sequential_3ch.py > /tmp/crawl_$(date +%Y%m%d).log 2>&1 &
echo "PID: $!"
```

로그 확인:
```bash
tail -f /tmp/crawl_$(date +%Y%m%d).log
```

---

## 빠른 참고 명령어

```bash
# 3채널 크롤링 (매일 실행)
python3 run_sequential_3ch.py

# 야놀자만 빠르게 수집
python3 run_phased.py --phase 1

# 대시보드만 새로고침 (크롤링 없이)
# → dashboard/index.html 을 브라우저에서 열기

# 설정 확인
python3 -c "import yaml; cfg=yaml.safe_load(open('config.yaml')); print(len(cfg['properties']), '개 사업장')"

# 로그 실시간 확인
tail -f /tmp/sono_3ch_crawl.log
tail -f logs/phased.log
```
