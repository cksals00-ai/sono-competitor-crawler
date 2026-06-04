# 경쟁사 OTA 가격 크롤링 — 스케줄 실행 리포트

- **작업명:** competitor-crawling (매일 04:00)
- **실행일시:** 2026-05-25 (자동 스케줄 실행, 무인)
- **프로젝트:** ~/Projects/sono-competitor-crawler
- **전체 결과:** ⚠️ **핵심 작업 미완료 — 실행 환경(샌드박스)에서 외부 네트워크 접근이 차단됨**

---

## 한 줄 요약

이번 스케줄 실행 환경은 **모든 외부 HTTP/HTTPS가 allowlist 프록시(`localhost:3128`)에서
403으로 차단**되고 **DNS도 도달 불가**하여, 브랜드몰·야놀자·트립닷컴·네이버호텔·골프장
어느 사이트도 크롤링할 수 없었고 GitHub 배포도 불가능했습니다. 크롤러를 실제로
실행해 확인한 결과 수집 건수는 **0건**이며, 작업 지침("크롤링 결과가 빈 값이면 이전
데이터 보존")에 따라 `exports/`·`dashboard/`·`docs/` 등 **기존 산출물은 일절
변경하지 않았습니다.** 상황은 직전 2회 실행(05-23, 05-24)과 동일합니다.

참고: 호스트(사용자 PC)의 별도 파이프라인은 정상 동작 중입니다 — `exports/`에
**05-24자 실데이터가 존재**합니다(아래 "데이터 보존 상태" 참고). 즉 문제는 크롤러
코드가 아니라 **이 스케줄 작업이 실행되는 환경의 네트워크 제약**입니다.

---

## 단계별 결과

| 단계 | 내용 | 결과 |
|---|---|---|
| 0 | repo 준비 / `.git` 락 삭제 | ❌ 삭제 불가 (마운트 EPERM) |
| 1 | 브랜드몰(자사몰) 크롤링 | ❌ 0건 (네트워크 차단 + 로그인 정보 없음) |
| 2 | 야놀자 크롤링 | ❌ 0건 (네트워크 차단) |
| 3 | 트립닷컴 크롤링 | ❌ 0건 (네트워크 차단) |
| 4 | 네이버호텔 크롤링 | ❌ 0건 (네트워크 차단) |
| 5 | 골프장 크롤링 | ❌ 0건 (네트워크 차단) |
| 6 | 대시보드 생성 + git push 배포 | ❌ 미실행 (신규 데이터 0건 → 보존, 배포 불가) |

크롤러(`crawler.py`, `golf_crawler.py`)는 **실제로 구동했고**, 모든 요청이 동일한
프록시 차단 오류로 실패하는 것을 확인했습니다(아래 로그).

---

## 단계별 에러 로그 (구체)

### 0단계 — `.git` 락 파일 삭제 불가
```
rm -f .git/index.lock .git/HEAD.lock
→ rm: cannot remove '.git/index.lock':  Operation not permitted
→ rm: cannot remove '.git/HEAD.lock':   Operation not permitted
```
프로젝트 폴더가 unlink를 허용하지 않는 FUSE 마운트로 연결되어 있어 삭제 실패.
stale 락 잔존: `HEAD.lock`(05-20 23:39 생성), `index.lock`(05-22 19:02 생성).
이 락이 남아 있는 한 정상 환경에서도 `git add/commit/push`가 전부 막힙니다.

### 1단계 — 브랜드몰(자사몰) 크롤링
```
[자사홈] 로그인 정보 없음 — 환경변수 SONO_USER_ID / SONO_PASSWORD
        또는 config.yaml sono_homepage 설정 필요
```
자사몰은 외부 네트워크 차단에 더해 **로그인 자격증명이 설정돼 있지 않아** 어차피
수집 불가 상태입니다. (`config.yaml`의 `sono_homepage`에 `user_id`만 있고 비밀번호
미설정, 환경변수도 없음.)

### 2단계 — 야놀자 크롤링 (`crawler.py --platform yanolja`)
```
[야놀자] 소노캄 비발디파크 요청 실패: HTTPSConnectionPool(host='nol.yanolja.com', port=443):
  Max retries exceeded ... (ProxyError('Unable to connect to proxy',
  OSError('Tunnel connection failed: 403 Forbidden')))
[여기어때] 소노캄 비발디파크 실패: No module named 'selenium'
[Agoda]   소노캄 비발디파크 실패: No module named 'selenium'
```
- `nol.yanolja.com` 도메인이 allowlist 프록시에서 403 차단.
- Selenium 기반 채널(여기어때·Agoda)은 이 환경에 **selenium 패키지 미설치**로도 실패
  (`pip install` 역시 PyPI 차단으로 불가).

### 3단계 — 트립닷컴 크롤링
```
[Trip.com] city 72360 fetch 실패 (시도 1~3): HTTPSConnectionPool(host='kr.trip.com', port=443):
  ... 403 Forbidden
[Trip.com] hotel 114782107 상세 fetch 실패: kr.trip.com ... 403 Forbidden
```
재시도 3회 모두 동일하게 프록시 403.

### 4단계 — 네이버호텔 크롤링
```
[네이버호텔] 세션 초기화 경고: hotels.naver.com ... 403 Forbidden
[네이버호텔] 소노캄 비발디파크 요청 실패: hermes-hotel-svc-api.naver.com /graphql ... 403 Forbidden
```

### 5단계 — 골프장 크롤링 (`golf_crawler.py`)
```
[환율] open.er-api.com 실패: ... 403 Forbidden
[환율] fawazahmed0 폴백도 실패: cdn.jsdelivr.net ... 403 Forbidden
[몽키트래블] 1076765242 / 1076866782 ... 요청 실패:
  www.monkeytravel.com /api/search/golfTeeoffPrice.php ... 403 Forbidden
```
환율 API·골프 가격 API 모두 차단되어 0건.

### 공휴일 API 조회 (투숙일 자동 판단)
```
[공휴일] 2026년 공휴일 조회 실패: date.nager.at /api/v3/PublicHolidays/2026/KR ... 403 Forbidden
        — 금토 기준만 적용
```
한국 공휴일 API(`date.nager.at`)도 차단되어, 연휴 자동 판단이 불가하고 **금·토 기준만**
적용되었습니다(코드의 폴백 동작). 차단이 없었다면 크롤링 투숙일 기준은
금/토 + 연휴(전날~마지막날 전날)로 산정됩니다.

### 6단계 — 배포
git remote는 `ssh://git@ssh.github.com:443/cksals00-ai/sono-competitor-crawler.git`.
`github.com` 도메인 차단 + DNS 미해석 + 잔존 `.git` 락으로 인해 `git add/commit/push`
전 과정 불가. 더하여 이번 실행은 신규 수집 0건이므로 지침상 배포 대상도 없습니다.

**네트워크 차단 근거 (직접 확인):**
```
CONNECT www.google.com:443  → HTTP/1.1 403 Forbidden
curl https://github.com     → (56) Received HTTP code 403 from proxy after CONNECT
getent hosts github.com     → (실패)   getent hosts nol.yanolja.com → (실패)
```

---

## 데이터 보존 상태

신규 수집이 **0건**이므로 작업 지침에 따라 **데이터 파일을 일절 생성·수정·삭제하지
않았습니다.** 실행 후 `exports/` 신규 파일 0개, `git status` 변동 없음을 확인했습니다.

호스트(사용자 PC)의 별도 파이프라인이 만든 가장 최근 정상 데이터:

- 경쟁사 가격 CSV: `exports/sono_competitor_prices_20260524.csv` (05-24 13:02, 약 340MB) ✅ **05-24 실데이터 존재**
- 골프 가격: `exports/golf_prices_20260524.csv` / `.xlsx` (05-24 13:08) ✅ **골프 정체 해소됨**
- 대시보드: `dashboard/index.html`, `docs/index.html` (둘 다 05-24 13:09 갱신)

이 리포트 파일(`SCHEDULED_TASK_REPORT_competitor-crawling_20260525.md`)만 새로 추가했습니다.

---

## 추가로 발견된 이슈 (점검 권장)

1. **실행 환경 ≠ 호스트 파이프라인 (가장 중요)**
   이 스케줄 작업이 도는 샌드박스는 외부 네트워크가 차단되어 크롤링/배포를 못 합니다.
   반면 호스트에는 05-24자 실데이터가 정상 생성돼 있어, 실제 크롤링은 별도 경로
   (`run_phased.py`/launchd 등, 05-24 13시대 실행 흔적)에서 이뤄지는 것으로 보입니다.
   → 이 스케줄 작업을 **네트워크가 허용되는 환경**에서 돌리거나, 호스트의 기존
   파이프라인으로 일원화할지 결정이 필요합니다. (05-23·05-24도 동일 증상)

2. **경쟁사 XLSX export 손상 — 미해결, 우선 점검 권장**
   `exports/sono_competitor_prices_*.xlsx`가 05-18까지는 정상(50~72MB)이었으나
   **05-19~05-23분은 3,275 bytes 빈 파일**이고, **05-24분은 dated xlsx 자체가
   미생성**입니다(CSV는 340MB로 정상). xlsx 변환/저장 로직 점검이 필요합니다.

3. **`.git` stale 락 — 미해결**
   `HEAD.lock`(05-20)·`index.lock`(05-22)이 계속 남아 있습니다. 정상 환경 터미널에서
   1회 수동 제거 전까지 git 쓰기가 전부 막힙니다:
   ```
   cd ~/Projects/sono-competitor-crawler
   rm -f .git/index.lock .git/HEAD.lock
   ```

4. **`crawler.py --platform` 인자 미작동 — 미해결**
   지침 2·3·4단계는 `--platform yanolja|tripcom|naver`를 명시하지만 `crawler.py`는
   `--test`만 인식하며 `--platform`을 파싱하지 않습니다(`if __name__` 블록 확인). 해당
   인자는 무시되고 매 실행마다 전 플랫폼을 크롤링합니다. 지침 명령어를 실제 동작에
   맞게 갱신하거나 argparse를 추가하는 것을 권장합니다.

5. **`exports/` CSV 비정상 누적 — 미해결**
   일별 경쟁사 CSV가 계속 커지는 중입니다(05-22 ~316MB → 05-24 ~340MB). 일별 스냅샷이
   아니라 과거 데이터가 누적되는 구조로 보이며, GitHub 100MB 제한도 초과합니다
   (현재 `.gitignore`로 제외돼 커밋되진 않음). 당일분만 남기도록 점검 권장.

6. **(해소) 골프 크롤링 정체** — 직전 리포트에서 "5일째 정체"로 지적했으나,
   `exports/golf_prices_20260524.csv`가 정상 생성되어 **해소된 것으로 확인**됩니다.

---

## 권장 조치

1. **실행 환경 점검 (최우선)** — 이 스케줄 작업에는 야놀자·네이버·트립닷컴·몽키트래블·
   GitHub·공휴일 API(`date.nager.at`)에 대한 아웃바운드 접근이 필수입니다. 해당
   도메인을 allowlist에 추가하거나 네트워크가 허용되는 환경에서 실행되도록 스케줄
   설정을 조정해 주세요.
2. **`.git` 락 수동 제거** — 위 4번 명령을 정상 터미널에서 1회 실행.
3. **XLSX export 손상 수정** — 05-19부터 깨진 경쟁사 xlsx 저장 로직 점검.
4. **자사몰 로그인 자격증명 설정** — `SONO_USER_ID`/`SONO_PASSWORD` 환경변수 또는
   `config.yaml`의 `sono_homepage`에 설정(현재 미설정).
5. **지침 명령어 갱신** — `--platform` 인자 미작동 반영.
6. **`exports/` CSV 누적 버그 수정** — 일별 스냅샷만 유지하도록 export 로직 점검.

---

*이번 실행은 무인 자동 스케줄 실행이며, 환경 제약(네트워크 전면 차단·마운트 권한)으로
크롤링·배포를 수행할 수 없어 작업 지침에 따라 발견 사항 리포트로 갈음했습니다.
신규 수집 0건이므로 기존 산출물은 일절 변경하지 않았습니다.*
