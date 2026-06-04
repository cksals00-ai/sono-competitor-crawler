# 경쟁사 OTA 가격 크롤링 — 스케줄 실행 리포트

- **작업명:** competitor-crawling (매일 04:00)
- **실행일시:** 2026-05-30 KST (자동 스케줄 실행, 무인)
- **프로젝트:** ~/Projects/sono-competitor-crawler
- **전체 결과:** ⚠️ **핵심 작업 미완료 — 실행 환경(샌드박스)의 외부 네트워크 차단 8일 연속**

---

## 한 줄 요약

이번 스케줄 실행도 **모든 외부 HTTP/HTTPS가 allowlist 프록시(`localhost:3128`)에서 403(`X-Proxy-Error: blocked-by-allowlist`) 차단**되어 브랜드몰·야놀자·트립닷컴·네이버호텔·골프장 어느 사이트도 크롤링할 수 없었고 GitHub 배포(SSH)도 불가했습니다. 신규 수집 **0건**이며, 지침("크롤링 결과가 빈 값이면 이전 데이터 보존")에 따라 `exports/`·`dashboard/`·`docs/` 기존 산출물을 **일절 변경하지 않았습니다.** 상황은 직전 6회(05-23·05-24·05-25·05-27·05-28·05-29) 실행과 동일합니다.

참고(긍정 신호): 호스트(사용자 PC)의 별도 파이프라인은 **05-29 13:19에 정상 동작**해 `exports/sono_competitor_prices_20260529.csv`(약 **466MB**)를 새로 생성했습니다. 즉 문제는 크롤러 코드가 아니라 **이 스케줄 작업이 실행되는 환경의 네트워크 제약**입니다.

---

## 단계별 결과

| 단계 | 내용 | 결과 |
|---|---|---|
| 0 | repo 준비 / `.git` 락 삭제 | ⚠️ `HEAD.lock`·`index.lock`·`maintenance.lock` 모두 잔존(EPERM, 삭제 불가) |
| 1 | 브랜드몰(자사몰) 크롤링 | ❌ 0건 (네트워크 차단 + 로그인 정보 없음) |
| 2 | 야놀자 크롤링 | ❌ 0건 (네트워크 차단) |
| 3 | 트립닷컴 크롤링 | ❌ 0건 (네트워크 차단) |
| 4 | 네이버호텔 크롤링 | ❌ 0건 (네트워크 차단) |
| 5 | 골프장 크롤링 | ❌ 0건 (네트워크 차단 + 환율 API 차단) |
| 6 | 대시보드 생성 + git push 배포 | ❌ 미실행 (신규 0건 → 보존, SSH 차단 + 락 잔존) |

크롤러(`crawler.py`, `golf_crawler.py`)는 import·구동 자체는 정상이며, 이번 실행에서도 실제로 기동시켜 모든 요청이 동일한 프록시 403으로 실패함을 직접 확인했습니다(아래 실제 로그).

---

## 단계별 에러 로그 (이번 실행 실측)

### 네트워크 차단 근거 (직접 확인)
```
curl -v https://www.google.com
  → CONNECT www.google.com:443 HTTP/1.1
  ← HTTP/1.1 403 Forbidden
  ← X-Proxy-Error: blocked-by-allowlist
  (proxy: localhost:3128)
```
야놀자·트립닷컴·네이버 API·GitHub·공휴일 API·몽키트래블 모두 `curl` 시 HTTP 코드 `000`(터널 미수립)으로 동일 차단.

### 0단계 — `.git` 락 파일
```
rm -f .git/index.lock .git/HEAD.lock .git/objects/maintenance.lock
  → rm: cannot remove '.git/index.lock': Operation not permitted
  → rm: cannot remove '.git/HEAD.lock': Operation not permitted
  → rm: cannot remove '.git/objects/maintenance.lock': Operation not permitted
```
프로젝트 폴더가 unlink를 허용하지 않는 마운트라 sandbox에서 락 삭제 불가. 잔존:
- `.git/HEAD.lock` (05-27 00:52, 0 byte) — 8일째 잔존
- `.git/index.lock` (05-28 19:05, 0 byte)
- `.git/objects/maintenance.lock` (05-12 04:05, 0 byte)

호스트(macOS) 터미널에서 1회 수동 제거 권장:
```
cd ~/Projects/sono-competitor-crawler
rm -f .git/HEAD.lock .git/index.lock .git/objects/maintenance.lock
```

### 1~4단계 — `crawler.py` (브랜드몰/야놀자/트립닷컴/네이버호텔)
실제 기동 로그(발췌):
```
[공휴일] 2026년 공휴일 조회 실패: ... date.nager.at ... 403 Forbidden — 금토 기준만 적용
[자사] 소노캄 비발디파크
[야놀자] 소노캄 비발디파크 요청 실패: ... nol.yanolja.com ... 403 Forbidden
[여기어때] 소노캄 비발디파크 실패: No module named 'selenium'
[Agoda]   소노캄 비발디파크 실패: No module named 'selenium'
[네이버호텔] 세션 초기화 경고: ... hotels.naver.com ... 403 Forbidden
[네이버호텔] 소노캄 비발디파크 요청 실패: ... hermes-hotel-svc-api.naver.com/graphql ... 403 Forbidden
[Trip.com] hotel 114782107 상세 fetch 실패: ... kr.trip.com ... 403 Forbidden (3회 재시도 모두 실패)
[자사홈] 로그인 정보 없음 — 환경변수 SONO_USER_ID / SONO_PASSWORD 또는 config.yaml sono_homepage 설정 필요
```
- 차단이 해제되어도 자사몰은 **로그인 자격증명 미설정**으로 수집 불가(`config.yaml` `sono_homepage`에 `user_id`만, 비밀번호 없음).
- 야놀자/여기어때/Agoda 폴백 경로는 `selenium` 미설치로 추가 차단(이 환경은 외부 pip 설치도 불가).

### 5단계 — 골프장 크롤링 (`golf_crawler.py`)
```
[환율] open.er-api.com 실패: ... 403 Forbidden
[환율] fawazahmed0 폴백(cdn.jsdelivr.net)도 실패: ... 403 Forbidden
[몽키트래블] 1076765242 2026-05-30 요청 실패: ... www.monkeytravel.com/api/search/golfTeeoffPrice.php ... 403 Forbidden
(이하 6/1~6/8 투숙일 전부 동일 403)
```

### 6단계 — 배포
```
git ls-remote origin
  → socat[..] E CONNECT ssh.github.com:443: Forbidden
  → kex_exchange_identification: Connection closed by remote host
  → fatal: Could not read from remote repository.
```
- remote: `ssh://git@ssh.github.com:443/cksals00-ai/sono-competitor-crawler.git`
- `ssh.github.com` 차단 + `.git` 락 잔존 + 신규 수집 0건 → 배포 대상 없음.

---

## 데이터 보존 상태

신규 수집이 **0건**이므로 지침에 따라 **데이터 파일을 일절 생성·수정·삭제하지 않았습니다.** 이 리포트 파일(`SCHEDULED_TASK_REPORT_competitor-crawling_20260530.md`)만 새로 추가했습니다.

호스트(사용자 PC)의 별도 파이프라인이 만든 가장 최근 정상 데이터:

- 경쟁사 가격 CSV: `exports/sono_competitor_prices_20260529.csv` (05-29 13:19, 약 **466MB**) ✅ **05-29 실데이터 존재(신규)**
- 직전: `exports/sono_competitor_prices_20260528.csv` (443MB)
- `git status` 기준 호스트 파이프라인의 미커밋 변경(아래)이 존재하며, 본 스케줄 작업은 손대지 않았습니다.

```
Changes to be committed:
  modified:   data/palatium_data.json
  modified:   docs/external-report.html
  modified:   docs/index.html
  modified:   docs/palatium-client.html
  modified:   docs/palatium.html
Changes not staged for commit:
  modified:   analytics/daily_summary.csv
  modified:   analytics/golf_price_pivot.csv
  modified:   analytics/golf_trends.csv
  modified:   analytics/hotel_price_pivot.csv
  modified:   analytics/hotel_trends.csv
  modified:   channel_sales_data.json
```
→ 위 변경은 호스트 파이프라인 산출물입니다. 호스트 측에서 commit/push를 마쳐야 GitHub에 반영됩니다(`.git` 락 잔존이 git 쓰기를 막고 있을 가능성 큼).

---

## 추가로 발견된 이슈 (점검 권장)

1. **실행 환경 ≠ 호스트 파이프라인 (가장 중요, 미해결 8일째)**
   이 스케줄 작업이 도는 샌드박스는 외부 네트워크가 전면 차단(allowlist 프록시 403)되어 크롤링/배포 불가. 반면 호스트에는 05-29자 실데이터(466MB)가 정상 생성돼 있어, 실제 크롤링은 별도 경로(launchd 등)에서 이뤄지는 것으로 보입니다.
   → 이 스케줄 작업을 **네트워크가 허용되는 환경**에서 돌리거나 호스트 파이프라인으로 일원화할지 결정이 필요합니다. (05-23~05-30 동일 증상)

2. **`exports/` CSV 누적 — 계속 증가 중**
   일별 경쟁사 CSV가 누적되어 계속 커집니다: 05-22 ~316MB → 05-24 ~340MB → 05-26 ~391MB → 05-27 ~416MB → 05-28 ~443MB → **05-29 ~466MB**. 일별 스냅샷이 아니라 과거 데이터가 누적되는 구조로 보이며 GitHub 100MB 제한도 초과합니다(현재 `.gitignore`로 제외돼 커밋되진 않음). 당일분만 남기도록 점검 권장.

3. **경쟁사 XLSX export 손상 — 일부 잔존**
   `exports/sono_competitor_prices_*.xlsx`가 05-19~05-23분은 **3,275 bytes 빈 파일**, 05-24 이후로는 **xlsx 미생성**(CSV만 존재). xlsx 변환/저장 로직 점검 필요.

4. **`.git` stale 락**
   `.git/HEAD.lock`·`.git/index.lock`·`.git/objects/maintenance.lock` 잔존. sandbox에서 EPERM으로 삭제 불가. 호스트에서 1회 수동 제거 필요(위 0단계 명령). 이 락이 풀려야 호스트 파이프라인의 commit/push도 정상화됩니다.

5. **`crawler.py --platform` 인자 미작동 — 미해결**
   지침 2·3·4단계는 `--platform yanolja|tripcom|naver`를 명시하지만 `crawler.py`는 해당 인자를 파싱하지 않아 매 실행 전 플랫폼을 일괄 크롤링합니다. 지침 명령어를 실제 동작에 맞게 갱신하거나 argparse 추가 권장.

6. **자사몰 로그인 자격증명 — 미해결**
   `SONO_USER_ID`/`SONO_PASSWORD` 환경변수 또는 `config.yaml` `sono_homepage.password` 미설정 상태 지속.

7. **`selenium` 미설치**
   야놀자/여기어때/Agoda 폴백 경로에 필요하나 미설치이며, 이 환경에서는 외부 pip 설치도 차단되어 보강 불가.

---

## 권장 조치

1. **실행 환경 점검 (최우선, 8일째)** — 야놀자(`nol.yanolja.com`)·네이버(`hotels.naver.com`, `hermes-hotel-svc-api.naver.com`)·트립닷컴(`kr.trip.com`)·몽키트래블(`www.monkeytravel.com`)·GitHub(`ssh.github.com`)·공휴일 API(`date.nager.at`)·환율 API(`open.er-api.com`, `cdn.jsdelivr.net`) 아웃바운드 허용, 또는 네트워크 허용 환경에서 실행되도록 스케줄 조정.
2. **`.git` 락 일괄 수동 제거** — 위 0단계 명령을 정상 터미널에서 1회 실행.
3. **`exports/` CSV 누적 버그 수정** — 일별 스냅샷만 유지하도록 export 로직 점검(466MB까지 증가).
4. **XLSX export 손상 수정** — 05-19부터 깨진/미생성 경쟁사 xlsx 저장 로직 점검.
5. **자사몰 로그인 자격증명 설정** — `SONO_USER_ID`/`SONO_PASSWORD` 또는 `config.yaml` `sono_homepage.password`.
6. **지침 명령어 갱신** — `crawler.py --platform` 인자 미작동 반영.

---

*이번 실행은 무인 자동 스케줄 실행이며, 환경 제약(네트워크 전면 차단·마운트 권한)으로 크롤링·배포를 수행할 수 없어 작업 지침에 따라 발견 사항 리포트로 갈음했습니다. 신규 수집 0건이므로 기존 산출물은 일절 변경하지 않았습니다.*
