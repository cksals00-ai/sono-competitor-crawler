# 경쟁사 OTA 가격 크롤링 — 스케줄 실행 리포트

- **작업명:** competitor-crawling (매일 04:00)
- **실행일시:** 2026-05-24 (자동 스케줄 실행, 무인)
- **프로젝트:** ~/Projects/sono-competitor-crawler
- **전체 결과:** ⚠️ **핵심 작업 미완료 — 실행 환경에 외부 네트워크 접근이 차단됨**

---

## 한 줄 요약

이번 실행 환경(샌드박스)은 **모든 외부 HTTP/HTTPS가 allowlist 프록시에서 차단**되고
**DNS 자체가 도달 불가**하여 OTA 사이트 크롤링과 GitHub 배포를 수행할 수 없었습니다.
크롤링 데이터는 전혀 수집되지 않았으며, 작업 지침("크롤링 결과가 빈 값이면 이전
데이터 보존")에 따라 **기존 데이터(`exports/`, `dashboard/`, `docs/`)는 일절 변경하지
않고 그대로 보존**했습니다. 상황은 직전 실행(2026-05-23)과 동일합니다.

---

## 단계별 결과

| 단계 | 내용 | 결과 |
|---|---|---|
| 0 | repo 준비 / .git 락 삭제 | ❌ 삭제 불가 (마운트 EPERM) |
| 1 | 브랜드몰(자사몰) 크롤링 | ❌ 미실행 (네트워크 차단) |
| 2 | 야놀자 크롤링 | ❌ 미실행 (네트워크 차단) |
| 3 | 트립닷컴 크롤링 | ❌ 미실행 (네트워크 차단) |
| 4 | 네이버호텔 크롤링 | ❌ 미실행 (네트워크 차단) |
| 5 | 골프장 크롤링 | ❌ 미실행 (네트워크 차단) |
| 6 | 대시보드 생성 + git push 배포 | ❌ 미실행 (배포 불가 + 신규 데이터 없음) |

---

## 핵심 차단 원인

### 1. 외부 네트워크 전면 차단 (allowlist 프록시)
모든 외부 HTTPS 요청이 `localhost:3128` 프록시에서 `403 Forbidden`으로 거부됩니다.
응답 헤더에 `X-Proxy-Error: blocked-by-allowlist`가 명시되어, 크롤링 대상 도메인이
allowlist에 등록되어 있지 않음이 확인됩니다.

```
CONNECT www.google.com:443  → HTTP/1.1 403 Forbidden  (X-Proxy-Error: blocked-by-allowlist)
nol.yanolja.com   → curl (56) 403 from proxy  /  python requests: ProxyError (Tunnel connection failed)
hotels.naver.com  → 403 blocked-by-allowlist
kr.trip.com       → 403 blocked-by-allowlist
github.com        → 403 blocked-by-allowlist
```

추가로 DNS 해석도 불가능합니다(`172.16.10.1#53 ... network unreachable`).

→ `crawler.py`(야놀자·트립닷컴·네이버호텔)와 `golf_crawler.py`는 Selenium/`requests`
   기반으로 대상 사이트에 직접 접속해야 하므로 단 한 건도 수집할 수 없습니다.
   브랜드몰 크롤링도 동일하게 외부 접속이 필요해 실행 불가입니다.

### 2. GitHub 배포 불가
git remote는 `ssh://git@ssh.github.com:443/cksals00-ai/sono-competitor-crawler.git`
입니다. github.com 도메인 자체가 allowlist에서 차단되고 DNS도 해석되지 않아,
신규 데이터가 있더라도 `git push origin main`은 이 환경에서 성공할 수 없습니다.

### 3. .git 락 파일 삭제 불가 + git 쓰기 작업 전면 불가
지침 1단계(`.git/index.lock`, `.git/HEAD.lock` 삭제)를 시도했으나, 프로젝트 폴더가
unlink를 허용하지 않는 FUSE 마운트로 연결되어 있어 실패했습니다.

```
rm -f .git/index.lock .git/HEAD.lock
→ rm: cannot remove '.git/index.lock': Operation not permitted
→ rm: cannot remove '.git/HEAD.lock':  Operation not permitted
```

이 stale 락이 남아 있어 `git add`조차 실패합니다(직접 확인함):

```
git add ...
→ fatal: Unable to create '.../.git/index.lock': File exists.
   Another git process seems to be running in this repository...
```

즉, **이 환경에서는 락 제거 → 크롤링 → 커밋 → push의 전 과정이 불가능**합니다.
락 파일은 stale 상태이며(HEAD.lock: 05-20, index.lock: 05-22 생성),
**정상 환경에서도 수동 제거 전까지 모든 git 쓰기 작업이 막힙니다.**

---

## 데이터 보존 상태

신규 수집이 0건이므로 작업 지침에 따라 **아무 데이터 파일도 생성·수정·삭제하지
않았습니다.** 가장 최근 정상 데이터는 다음과 같습니다.

- 경쟁사 가격(CSV): `exports/sono_competitor_prices_20260523.csv` (05-23 수집분, 약 316MB)
- 경쟁사 가격(XLSX): 05-19분부터 파일 크기가 3,275 bytes로 사실상 빈 파일 — **xlsx export가 05-19 이후 깨진 상태** (아래 이슈 2 참고)
- 골프 가격: `exports/golf_prices_20260519.csv` (05-19 수집분 — 이후 신규 없음, 5일째 정체)
- 대시보드: `dashboard/index.html`(05-19), `docs/index.html`(05-20)

이 리포트 파일(`SCHEDULED_TASK_REPORT_competitor-crawling_20260524.md`)만 새로 추가되었습니다.

---

## 추가로 발견된 이슈 (점검 권장)

1. **작업 지침과 실제 코드 불일치 (직전 리포트에서도 지적됨, 미수정 상태)**
   지침 4·5·6단계는 `python3 crawler.py --platform yanolja|tripcom|naver` 를 명시하지만,
   `crawler.py`는 `sys.argv`에서 `--test`만 인식하며 `--platform` 인자를 **파싱하지
   않습니다**(argparse 미사용). 해당 명령을 그대로 실행하면 플랫폼 필터가 무시됩니다.
   실제 진입점(`run_once.py` / `scheduler.py`)에 맞춰 지침 명령어 갱신을 권장합니다.

2. **경쟁사 XLSX export 손상 (신규 발견 — 우선 점검 권장)**
   `exports/sono_competitor_prices_2026051Y.xlsx` 파일이 05-18까지는 정상 크기
   (50~72MB)였으나, **05-19분부터 3,275 bytes로 급감**(05-19~05-23 동일).
   같은 날 CSV는 ~316MB로 정상 생성되므로, xlsx 변환 단계가 약 05-19부터
   깨진 것으로 보입니다. export 코드의 xlsx 저장 로직 점검이 필요합니다.

3. **`exports/` CSV 비정상적 누적 (직전 리포트 지적, 미수정 상태)**
   일별 CSV가 계속 커지고 있습니다: 05-13 ~131MB → 05-18 ~261MB → 05-23 ~316MB.
   일별 스냅샷이 아니라 과거 데이터가 누적되는 구조로 보입니다. 매일 export·
   대시보드 생성 시간이 늘어나고 GitHub 100MB 제한도 초과합니다
   (현재 `.gitignore`로 제외되어 커밋되진 않음). 당일 데이터만 남기도록 점검 필요.

4. **골프 크롤링 5일째 정체**
   `exports/golf_prices_*` 가 05-19 이후 신규 파일이 없습니다. 네트워크 차단 외에
   별도 원인이 있는지(스케줄 누락 등) 정상 환경에서 확인 권장.

---

## 권장 조치

1. **실행 환경 점검 (최우선)** — 이 스케줄 작업은 야놀자·네이버·트립닷컴·골프장
   사이트 및 GitHub에 대한 아웃바운드 네트워크 접근이 필수입니다. 해당 도메인이
   allowlist에 포함되거나 네트워크가 허용되는 환경에서 실행되도록 스케줄 작업
   설정을 확인해 주세요. (이 제약은 2026-05-23 실행에서도 동일하게 발생했습니다.)
2. **.git 락 수동 제거** — 정상 접근 가능한 터미널에서 아래를 1회 실행하세요.
   제거 전까지는 정상 환경에서도 git add/commit/push가 모두 실패합니다.
   ```
   cd ~/Projects/sono-competitor-crawler
   rm -f .git/index.lock .git/HEAD.lock .git/index.lock.bak
   ```
3. **XLSX export 손상 수정** — 05-19부터 깨진 경쟁사 xlsx 저장 로직 점검.
4. **`exports/` CSV 누적 버그 수정** — 일별 스냅샷만 유지하도록 export 로직 점검.
5. **지침 명령어 갱신** — 실제 진입점(`run_once.py` / `scheduler.py`)에 맞게 수정.

---

*이번 실행은 무인 자동 스케줄 실행이며, 환경 제약(네트워크 차단·마운트 권한)으로
크롤링·배포를 수행할 수 없어 작업 지침에 따라 발견 사항 리포트로 갈음했습니다.
신규 데이터가 0건이므로 기존 산출물은 일절 변경하지 않았습니다.*
