# 경쟁사 OTA 가격 크롤링 — 스케줄 실행 리포트

- **작업명:** competitor-crawling (매일 04:00)
- **실행일시:** 2026-05-23 (자동 스케줄 실행, 무인)
- **프로젝트:** ~/Projects/sono-competitor-crawler
- **전체 결과:** ⚠️ **핵심 작업 미완료 — 실행 환경에 외부 네트워크 접근이 차단됨**

---

## 한 줄 요약

이번 실행 환경(샌드박스)은 **모든 외부 HTTP/HTTPS 및 DNS가 차단**되어 있어
OTA 사이트 크롤링과 GitHub 배포를 수행할 수 없었습니다. 크롤링 데이터는 전혀
수집되지 않았으며, 작업 지침("크롤링 결과가 빈 값이면 이전 데이터 보존")에 따라
**기존 데이터(`exports/`, `dashboard/`, `docs/`)는 일절 변경하지 않고 그대로 보존**했습니다.

---

## 단계별 결과

| 단계 | 내용 | 결과 |
|---|---|---|
| 0 | repo 준비 / .git 락 삭제 | ❌ 삭제 불가 (마운트 권한) |
| 1 | 브랜드몰(자사몰) 크롤링 | ❌ 미실행 (네트워크 차단) |
| 2 | 야놀자 크롤링 | ❌ 미실행 (네트워크 차단) |
| 3 | 트립닷컴 크롤링 | ❌ 미실행 (네트워크 차단) |
| 4 | 네이버호텔 크롤링 | ❌ 미실행 (네트워크 차단) |
| 5 | 골프장 크롤링 | ❌ 미실행 (네트워크 차단) |
| 6 | 대시보드 생성 + git push 배포 | ❌ 미실행 (배포 불가 + 신규 데이터 없음) |

---

## 핵심 차단 원인

### 1. 외부 네트워크 전면 차단
이번 실행 환경에서 모든 외부 도메인 접속이 프록시에서 `403 Forbidden`으로 거부됩니다.
크롤링 대상은 물론 일반 도메인(google.com), 패키지 저장소(pypi.org)까지 모두 차단됩니다.

```
nol.yanolja.com   → curl: HTTP 403 from proxy / requests: ProxyError
hotels.naver.com  → ProxyError (Tunnel connection failed: 403 Forbidden)
kr.trip.com       → ProxyError
www.google.com    → ProxyError
pypi.org          → ProxyError
```

→ `crawler.py`(야놀자·트립닷컴·네이버호텔)와 `golf_crawler.py`는 `requests` 기반으로
   대상 사이트에 직접 접속해야 하므로 단 한 건도 수집할 수 없습니다.

### 2. GitHub 배포 불가
git remote는 `ssh://git@ssh.github.com:443/...` 인데, SSH 호스트 이름 해석(DNS) 자체가
실패합니다.

```
ssh -T git@ssh.github.com -p 443
→ ssh: Could not resolve hostname ssh.github.com: Temporary failure in name resolution
```

→ 신규 데이터가 있더라도 `git push origin main`은 이 환경에서 성공할 수 없습니다.

### 3. .git 락 파일 삭제 불가
지침의 1단계(`.git/index.lock`, `.git/HEAD.lock` 삭제)를 시도했으나, 프로젝트 폴더가
삭제(unlink)를 허용하지 않는 FUSE 마운트로 연결되어 있어 실패했습니다.

```
rm .git/HEAD.lock   → Operation not permitted (EPERM)
rm .git/index.lock  → Operation not permitted (EPERM)
```

파일 소유자/권한은 정상(본인 소유, 0600)이지만 마운트 계층에서 unlink가 막혀 있습니다.
이 락 파일들은 stale 상태이며(HEAD.lock: 05-20, index.lock: 05-22 생성),
**남아 있으면 향후 정상 환경의 실행에서도 `git add`/`commit`이 실패**할 수 있습니다.

---

## 추가로 발견된 이슈 (점검 권장)

1. **작업 지침과 실제 코드 불일치**
   지침 4·5·6단계는 `python3 crawler.py --platform yanolja|tripcom|naver` 를 명시하지만,
   현재 `crawler.py`는 `--platform` 인자를 **파싱하지 않습니다**(`--test`만 인식).
   해당 명령을 그대로 실행하면 플랫폼 필터 없이 전체 크롤이 도는 동작이 됩니다.
   또한 `crawler.py`는 단독 실행 시 결과를 출력만 하고 파일로 저장하지 않습니다.
   실제 "크롤 → export → 대시보드" 오케스트레이션은 `scheduler.py`의 `daily_job()`
   (또는 `run_once.py`)이 담당합니다. 지침의 명령어 목록을 실제 진입점에 맞춰
   갱신하는 것을 권장합니다.

2. **`exports/` CSV 비정상적 누적 (용량 급증)**
   `exports/sono_competitor_prices_20260522.csv` = **약 316 MB / 1,068,787 행**.
   파일 내 `수집일시`가 2026-05-09까지 거슬러 올라가, 일별 스냅샷이 아니라
   과거 데이터가 계속 누적되고 있는 것으로 보입니다. 이 상태로는 매일 export·
   대시보드 생성 시간이 계속 늘어나고, GitHub 100MB 제한도 초과합니다
   (현재는 `.gitignore`로 제외되어 커밋되진 않음). export 로직이 매 실행마다
   당일 데이터만 남기도록 점검이 필요합니다.

3. **`.git/` 내 잔여 파일**
   `index.lock`, `index.lock.bak`, `index2`, `testwrite` 등 비정상 파일이 존재합니다.
   리포지토리 정리를 권장합니다.

---

## 데이터 보존 상태

신규 수집이 0건이므로 작업 지침("크롤링 결과가 빈 값이면 이전 데이터 보존")에 따라
**아무 파일도 생성·수정·삭제하지 않았습니다.** 가장 최근 정상 데이터는 다음과 같습니다.

- 경쟁사 가격: `exports/sono_competitor_prices_20260522.csv` (05-22 수집분)
- 골프 가격: `exports/golf_prices_20260519.csv` (05-19 수집분 — 이후 신규 없음)
- 대시보드: `dashboard/index.html`(05-19), `docs/index.html`(05-20)

이 리포트 파일(`SCHEDULED_TASK_REPORT_competitor-crawling_20260523.md`)만 새로
추가되었습니다.

---

## 권장 조치

1. **실행 환경 점검** — 이 스케줄 작업은 야놀자·네이버·트립닷컴·골프장 사이트와
   GitHub에 대한 아웃바운드 네트워크 접근이 필수입니다. 네트워크가 허용되는
   환경에서 실행되도록 스케줄 작업 설정을 확인해 주세요.
2. **.git 락 수동 제거** — 정상 접근 가능한 터미널에서 아래를 1회 실행하세요.
   ```
   cd ~/Projects/sono-competitor-crawler
   rm -f .git/index.lock .git/HEAD.lock .git/index.lock.bak
   ```
3. **지침 명령어 갱신** — 실제 진입점(`run_once.py` 또는 `scheduler.py`)에 맞게
   작업 지침의 단계별 명령어를 수정.
4. **`exports/` CSV 누적 버그 수정** — 일별 스냅샷만 유지하도록 export 로직 점검.

---

*이번 실행은 무인 자동 스케줄 실행이며, 환경 제약으로 크롤링·배포를 수행할 수
없어 작업 지침에 따라 발견 사항 리포트로 갈음했습니다.*
