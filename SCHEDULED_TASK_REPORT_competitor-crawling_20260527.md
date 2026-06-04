# 경쟁사 OTA 가격 크롤링 — 스케줄 실행 리포트

- **작업명:** competitor-crawling (매일 04:00)
- **실행일시:** 2026-05-27 04:00 KST (자동 스케줄 실행, 무인)
- **프로젝트:** ~/Projects/sono-competitor-crawler
- **전체 결과:** ⚠️ **핵심 작업 미완료 — 실행 환경(샌드박스)에서 외부 네트워크 접근이 차단됨 (5일 연속 동일)**

---

## 한 줄 요약

이번 스케줄 실행 환경 역시 **모든 외부 HTTP/HTTPS가 allowlist 프록시(`localhost:3128`)에서 403 차단**, **DNS도 도달 불가**이며, 브랜드몰·야놀자·트립닷컴·네이버호텔·골프장 어느 사이트도 크롤링할 수 없었고 GitHub 배포도 불가했습니다. 신규 수집 **0건**이며, 지침("크롤링 결과가 빈 값이면 이전 데이터 보존")에 따라 `exports/`·`dashboard/`·`docs/` 기존 산출물을 **일절 변경하지 않았습니다.** 상황은 직전 3회(05-23, 05-24, 05-25) 실행과 완전히 동일합니다.

참고: 호스트(사용자 PC)의 별도 파이프라인은 **05-26 13:14~13:22에 정상 동작**한 흔적이 있어 `exports/`에 **05-26자 실데이터가 존재**합니다(아래 "데이터 보존 상태" 참고). 즉 문제는 크롤러 코드가 아니라 **이 스케줄 작업이 실행되는 환경의 네트워크 제약**입니다.

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
| 6 | 대시보드 생성 + git push 배포 | ❌ 미실행 (신규 수집 0건 → 보존, 배포 불가) |

크롤러(`crawler.py`, `golf_crawler.py`)는 import·구동을 확인했고, 모든 요청이 동일한 프록시 차단 오류로 실패함을 확인했습니다(아래 로그).

---

## 단계별 에러 로그 (구체)

### 0단계 — `.git` 락 파일 삭제 불가
```
rm -f .git/index.lock .git/HEAD.lock
→ rm: cannot remove '.git/index.lock':  Operation not permitted
→ rm: cannot remove '.git/HEAD.lock':   Operation not permitted
```
프로젝트 폴더가 unlink를 허용하지 않는 FUSE 마운트로 연결되어 있어 삭제 실패. stale 락 잔존:
- `HEAD.lock`(05-20 23:39 생성), `index.lock`(05-22 19:02 생성).

오늘 호스트 파이프라인(2026-05-26 22:22) 로그에서도 동일한 메시지 확인:
```
[ERROR] git commit 실패:
fatal: Unable to create '.../.git/index.lock': File exists.
Another git process seems to be running in this repository, ...
```
이 락이 남아 있는 한 정상 환경에서도 `git add/commit/push`가 전부 막힙니다.

### 1단계 — 브랜드몰(자사몰) 크롤링
- 외부 네트워크 차단으로 `https://www.sonohotelsresorts.com/...` 도달 불가.
- 또한 **로그인 자격증명이 설정돼 있지 않아** 차단이 해제되어도 수집 불가:
  - `config.yaml` `sono_homepage`에 `user_id: cksals00`만 있고 비밀번호 미설정,
    환경변수 `SONO_USER_ID`/`SONO_PASSWORD`도 미설정.

### 2단계 — 야놀자 크롤링 (`crawler.py --platform yanolja`)
```
yanolja-direct ERR: ProxyError(MaxRetryError("HTTPSConnectionPool(host='nol.yanolja.com', port=443):
  Max retries exceeded with url: /places/3000000223
  (Caused by ProxyError('Unable to connect to proxy',
   OSError('Tunnel connection failed: 403 Forbidden')))"))
```
참고: `crawler.py`는 `--platform` 인자를 파싱하지 않습니다(`--test`만 인식). 지침 명령은 그대로 통과되지만 사실상 모든 플랫폼이 일괄 실행되는 구조입니다. (미해결 이슈, 아래 "추가로 발견된 이슈" 참고)

### 3단계 — 트립닷컴 크롤링
```
urlopen('https://kr.trip.com') → <urlopen error Tunnel connection failed: 403 Forbidden>
```

### 4단계 — 네이버호텔 크롤링
```
urlopen('https://hotels.naver.com') → <urlopen error Tunnel connection failed: 403 Forbidden>
```

### 5단계 — 골프장 크롤링 (`golf_crawler.py`)
- 환율 API(`open.er-api.com`, `cdn.jsdelivr.net`) 차단.
- 몽키트래블·KKday·골프존카운티 등 골프 가격 API 도메인 일괄 차단.

### 공휴일 API 조회 (투숙일 자동 판단)
```
urlopen('https://date.nager.at/api/v3/PublicHolidays/2026/KR')
  → <urlopen error Tunnel connection failed: 403 Forbidden>
```
한국 공휴일 API도 차단되어, 연휴 자동 판단이 불가하고 코드 폴백상 **금·토 기준만** 적용되었습니다. 차단이 없었다면 크롤링 투숙일 기준은 금/토 + 연휴(전날~마지막날 전날)로 산정됩니다.

### 6단계 — 배포
- git remote: `ssh://git@ssh.github.com:443/cksals00-ai/sono-competitor-crawler.git`
- `github.com` 도메인 차단(`curl https://github.com → 403 from proxy`) + DNS 미해석 + 잔존 `.git` 락.
- 추가로 신규 수집 0건이므로 지침상 배포 대상도 없음.

**네트워크 차단 근거 (직접 확인):**
```
HTTPS_PROXY=http://localhost:3128 curl -v https://www.google.com
  → CONNECT www.google.com:443 HTTP/1.1
  ← HTTP/1.1 403 Forbidden
  ← X-Proxy-Error: blocked-by-allowlist
```
DNS 직조회(`getent hosts github.com`, `getent hosts nol.yanolja.com`)도 모두 실패.

---

## 데이터 보존 상태

신규 수집이 **0건**이므로 지침에 따라 **데이터 파일을 일절 생성·수정·삭제하지 않았습니다.** 실행 후 `exports/` 신규 파일 0개, `git status`상 추적 파일 변동은 직전 호스트 파이프라인이 만든 것이며 본 스케줄 실행으로 추가/수정한 파일 없음을 확인했습니다.

호스트(사용자 PC)의 별도 파이프라인이 만든 가장 최근 정상 데이터 (오늘 기준):

- 경쟁사 가격 CSV: `exports/sono_competitor_prices_20260526.csv` (05-26 13:14, 약 391MB) ✅ **05-26 실데이터 존재**
- 골프 가격: `exports/golf_prices_20260526.csv` / `.xlsx` (05-26 13:21) ✅
- 대시보드: `dashboard/index.html`, `docs/index.html` (둘 다 05-26 13:22 갱신)

이 리포트 파일(`SCHEDULED_TASK_REPORT_competitor-crawling_20260527.md`)만 새로 추가했습니다.

---

## 추가로 발견된 이슈 (점검 권장)

1. **실행 환경 ≠ 호스트 파이프라인 (가장 중요, 미해결 5일째)**
   이 스케줄 작업이 도는 샌드박스는 외부 네트워크가 전면 차단되어 크롤링/배포를 못 합니다. 반면 호스트에는 05-26자 실데이터가 정상 생성돼 있어, 실제 크롤링은 별도 경로(`run_phased.py`/launchd 등, 05-26 13시대 실행 흔적)에서 이뤄지는 것으로 보입니다.
   → 이 스케줄 작업을 **네트워크가 허용되는 환경**에서 돌리거나, 호스트의 기존 파이프라인으로 일원화할지 결정이 필요합니다. (05-23·05-24·05-25·05-27 모두 동일 증상)

2. **경쟁사 XLSX export 손상 — 부분 해소 / 일부 잔존**
   `exports/sono_competitor_prices_*.xlsx`가 05-18까지는 정상(50~72MB)이었으나 **05-19~05-23분은 3,275 bytes 빈 파일**입니다. 다만 **05-24·05-25·05-26분은 xlsx 자체가 미생성**(CSV만 존재). xlsx 변환/저장 로직 점검이 여전히 필요합니다.

3. **`.git` stale 락 — 미해결 (5일째)**
   `HEAD.lock`(05-20)·`index.lock`(05-22)이 계속 남아 있고, 어제(05-26) 호스트 파이프라인도 이 락 때문에 `git commit` 실패 메시지를 남겼습니다. 정상 환경 터미널에서 1회 수동 제거 전까지 git 쓰기가 전부 막힙니다:
   ```
   cd ~/Projects/sono-competitor-crawler
   rm -f .git/index.lock .git/HEAD.lock
   ```

4. **`crawler.py --platform` 인자 미작동 — 미해결**
   지침 2·3·4단계는 `--platform yanolja|tripcom|naver`를 명시하지만 `crawler.py`는 `--test`만 인식하며 `--platform`을 파싱하지 않습니다(L2265 확인). 해당 인자는 무시되고 매 실행마다 전 플랫폼을 크롤링합니다. 지침 명령어를 실제 동작에 맞게 갱신하거나 argparse를 추가하는 것을 권장합니다.

5. **`exports/` CSV 누적 — 미해결**
   일별 경쟁사 CSV가 계속 커지는 중입니다(05-22 ~316MB → 05-24 ~340MB → 05-26 **~391MB**). 일별 스냅샷이 아니라 과거 데이터가 누적되는 구조로 보이며, GitHub 100MB 제한도 초과합니다(현재 `.gitignore`로 제외돼 커밋되진 않음). 당일분만 남기도록 점검 권장.

6. **iCloud 복사 충돌 — 신규 관측**
   05-26 호스트 파이프라인 로그에서:
   ```
   [ERROR] iCloud 복사 실패: [Errno 11] Resource deadlock avoided:
     '.../com~apple~CloudDocs/소노_경쟁사_대시보드.html'
   ```
   iCloud Drive 동기화 충돌로 대시보드 백업 복사가 실패한 흔적이 있습니다. 잠금 충돌이라 일시적일 수 있으나 반복되면 점검 필요.

---

## 권장 조치

1. **실행 환경 점검 (최우선, 5일째)** — 이 스케줄 작업에는 야놀자(`nol.yanolja.com`)·네이버(`hotels.naver.com`, `hermes-hotel-svc-api.naver.com`)·트립닷컴(`kr.trip.com`)·몽키트래블(`www.monkeytravel.com`)·GitHub(`github.com`, `ssh.github.com`)·공휴일 API(`date.nager.at`)·환율 API(`open.er-api.com`, `cdn.jsdelivr.net`) 등에 대한 아웃바운드 접근이 필수입니다. 해당 도메인을 allowlist에 추가하거나 네트워크가 허용되는 환경에서 실행되도록 스케줄 설정을 조정해 주세요.
2. **`.git` 락 수동 제거** — 위 3번 명령을 정상 터미널에서 1회 실행. (이 락이 풀려야 호스트 파이프라인의 `git push`까지 정상화됩니다)
3. **XLSX export 손상 수정** — 05-19부터 깨진/미생성 경쟁사 xlsx 저장 로직 점검.
4. **자사몰 로그인 자격증명 설정** — `SONO_USER_ID`/`SONO_PASSWORD` 환경변수 또는 `config.yaml`의 `sono_homepage`에 비밀번호 설정(현재 미설정).
5. **지침 명령어 갱신** — `--platform` 인자 미작동 반영.
6. **`exports/` CSV 누적 버그 수정** — 일별 스냅샷만 유지하도록 export 로직 점검(391MB 증가 중).

---

*이번 실행은 무인 자동 스케줄 실행이며, 환경 제약(네트워크 전면 차단·마운트 권한)으로 크롤링·배포를 수행할 수 없어 작업 지침에 따라 발견 사항 리포트로 갈음했습니다. 신규 수집 0건이므로 기존 산출물은 일절 변경하지 않았습니다.*
