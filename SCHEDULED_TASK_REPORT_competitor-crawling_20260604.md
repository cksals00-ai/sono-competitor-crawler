# 경쟁사 OTA 가격 크롤링 — 스케줄 실행 리포트

- **작업명:** competitor-crawling (매일 04:00)
- **실행일시:** 2026-06-04 04:0x KST (자동 스케줄 실행, 무인)
- **프로젝트:** ~/Projects/sono-competitor-crawler
- **전체 결과:** ⚠️ **핵심 작업 미완료 — 실행 환경(샌드박스) 외부 네트워크 차단 (연속 지속)**

---

## 한 줄 요약

이번 실행도 **모든 외부 HTTP/HTTPS가 allowlist 프록시(`localhost:3128`)에서 403(`X-Proxy-Error: blocked-by-allowlist`) 차단**되어 브랜드몰·야놀자·트립닷컴·네이버호텔·골프장 어느 사이트도 크롤링할 수 없었고, GitHub 배포(SSH `ssh.github.com:443`)도 `Forbidden`으로 차단되었습니다. 신규 수집 **0건**이며, 지침("크롤링 결과가 빈 값이면 이전 데이터 보존")에 따라 `exports/`·`dashboard/`·`docs/`·`analytics/` 기존 산출물을 **일절 변경하지 않았습니다.** 증상은 직전 다회(05-23~06-03) 실행과 완전히 동일합니다.

참고(긍정 신호): 호스트(사용자 PC)의 별도 파이프라인은 **06-03 13:24~13:35 KST에 정상 동작**해 `exports/sono_competitor_prices_20260603.csv`(약 **588MB**), `exports/golf_prices_20260603.csv/.xlsx`, `analytics/*.csv`(06-03 13:35), `docs/index.html`(06-03 13:33)을 새로 생성했습니다. 즉 문제는 크롤러 코드가 아니라 **이 스케줄 작업이 실행되는 환경의 네트워크 제약**입니다.

---

## 단계별 결과

| 단계 | 내용 | 결과 |
|---|---|---|
| 0 | repo 준비 / `.git` 락 삭제 | ⚠️ `HEAD.lock`·`index.lock` 잔존 (EPERM, sandbox에서 삭제 불가) |
| 1 | 브랜드몰(자사몰) 크롤링 | ❌ 0건 (네트워크 차단 + 로그인 자격증명 미설정) |
| 2 | 야놀자 크롤링 | ❌ 0건 (프록시 403 차단) |
| 3 | 트립닷컴 크롤링 | ❌ 0건 (프록시 403 차단) |
| 4 | 네이버호텔 크롤링 | ❌ 0건 (프록시 403 차단) |
| 5 | 골프장 크롤링 | ❌ 0건 (프록시 403 차단 + 환율 API 차단) |
| 6 | 대시보드 생성 + git push 배포 | ❌ 미실행 (신규 0건 → 보존, SSH 차단 + 락 잔존) |

탭 순서(브랜드몰 → 야놀자 → 트립닷컴 → 네이버호텔)는 유지했으나 모든 단계가 동일 원인으로 0건이었습니다.

---

## 이번 실행 실측 로그

### 네트워크 차단 근거 (직접 확인)
```
curl -v https://www.google.com
  → Establish HTTP proxy tunnel to www.google.com:443
  → HTTP/1.1 403 Forbidden   (proxy localhost:3128, blocked-by-allowlist)

대상 도메인 연결 테스트 (curl, --max-time 8):
  https://www.google.com         -> HTTP 000
  https://nol.yanolja.com        -> HTTP 000
  https://kr.trip.com            -> HTTP 000
  https://hotels.naver.com       -> HTTP 000
  https://date.nager.at          -> HTTP 000   (공휴일 API)
  https://ssh.github.com         -> HTTP 000   (배포)
```
모든 아웃바운드가 프록시에서 차단(HTTP 000 = 터널 미수립). 공휴일 API(`date.nager.at`)도 차단되어 "연휴 자동 판단" 불가, 금/토 기준만 적용 가능한 상태였습니다.

### 0단계 — `.git` 락 파일
```
rm -f .git/index.lock .git/HEAD.lock .git/objects/maintenance.lock
  → rm: cannot remove '.git/index.lock': Operation not permitted
  → rm: cannot remove '.git/HEAD.lock': Operation not permitted
잔존:
  .git/HEAD.lock   (05-27 00:52, 0 byte)
  .git/index.lock  (05-28 19:05, 0 byte)
```
프로젝트 폴더가 unlink를 허용하지 않는 마운트라 sandbox에서 락 삭제 불가.
**호스트(macOS) 터미널에서 1회 수동 제거 권장:**
```
cd ~/Projects/sono-competitor-crawler
rm -f .git/HEAD.lock .git/index.lock .git/objects/maintenance.lock
```

### 1~5단계 — 크롤러
- `selenium` **미설치**(`ModuleNotFoundError: No module named 'selenium'`) → 야놀자/여기어때/Agoda 폴백 경로 추가 차단. 외부 pip 설치도 프록시 차단으로 불가.
- 차단이 해제되어도 자사몰은 **로그인 자격증명 미설정**(config.yaml `sono_homepage`에 `login_url`만 있고 `SONO_USER_ID`/`SONO_PASSWORD` 미설정)으로 수집 불가.

### 6단계 — 배포
```
git ls-remote origin
  → socat E CONNECT ssh.github.com:443: Forbidden
  → kex_exchange_identification: Connection closed by remote host
  → fatal: Could not read from remote repository.
```
`ssh.github.com` 차단 + `.git` 락 잔존 + 신규 수집 0건 → 배포 대상 없음. `pull --rebase` 재시도도 remote 도달 불가로 무의미.

---

## 데이터 보존 상태

신규 수집 **0건**이므로 지침에 따라 **데이터 파일을 일절 생성·수정·삭제하지 않았습니다.** 이 리포트 파일만 새로 추가했습니다. `dashboard_generator.py`는 신규 데이터가 없어 기존 대시보드 보존을 위해 실행하지 않았습니다.

호스트(사용자 PC) 별도 파이프라인이 만든 가장 최근 정상 데이터:
- `exports/sono_competitor_prices_20260603.csv` (≈588MB, 06-03 13:24)
- `exports/golf_prices_20260603.csv` / `.xlsx` (06-03 13:31)
- `analytics/daily_summary.csv`, `hotel_price_pivot.csv`, `golf_price_pivot.csv`, `hotel_trends.csv`, `golf_trends.csv` (06-03 13:35)
- `docs/index.html` (06-03 13:33)

> 참고: 위 호스트 산출물 중 일부(`data/palatium_data.json`, `docs/index.html` 등)는 이미 git staged 상태이나, 락 잔존 + remote 차단으로 commit/push 불가합니다.

---

## 권장 조치 (사용자/호스트 측 1회 작업)

1. **이 스케줄 작업의 실행 환경에 아웃바운드 네트워크 허용** — 핵심 원인. allowlist에 다음 도메인 추가 필요: `nol.yanolja.com`, `kr.trip.com`, `hotels.naver.com`, `date.nager.at`, `ssh.github.com`(또는 `github.com:443`).
2. **`.git` 락 수동 제거** (위 0단계 명령).
3. **`selenium` 설치** 및 자사몰 **로그인 자격증명 환경변수 설정**(`SONO_USER_ID`/`SONO_PASSWORD`).
4. 위 조치 전까지는 **호스트 PC의 정상 파이프라인(매일 13시대 동작)** 이 실데이터·대시보드를 계속 생성하므로 운영 공백은 없음. 이 스케줄 작업은 환경 제약이 풀릴 때까지 리포트만 생성합니다.
