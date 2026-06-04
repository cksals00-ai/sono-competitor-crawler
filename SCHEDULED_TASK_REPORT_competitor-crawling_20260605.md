# 경쟁사 OTA 가격 크롤링 — 스케줄 실행 리포트

- **작업명:** competitor-crawling (매일 04:00)
- **실행일시:** 2026-06-05 04:03 KST (자동 스케줄 실행, 무인)
- **프로젝트:** ~/Projects/sono-competitor-crawler
- **전체 결과:** ⚠️ **핵심 작업 미완료 — 실행 환경(샌드박스) 외부 네트워크 차단 (연속 지속)**

---

## 한 줄 요약

이번 실행도 직전 다회(05-23~06-04)와 **완전히 동일한 증상**으로, 모든 외부 HTTP/HTTPS가 allowlist 프록시에서 **403/터널 미수립(HTTP 000)** 으로 차단되어 브랜드몰·야놀자·트립닷컴·네이버호텔·골프장 어느 사이트도 크롤링하지 못했습니다. GitHub 배포 엔드포인트(`ssh.github.com:443`)도 차단(HTTP 000)이라 push 불가입니다. 신규 수집 **0건**이며, 지침("크롤링 결과가 빈 값이면 이전 데이터 보존")에 따라 `exports/`·`analytics/`·`docs/`·`dashboard/` 기존 산출물을 **일절 변경하지 않았습니다.**

**긍정 신호:** 호스트(사용자 PC)의 별도 파이프라인은 **06-04 13:46~13:57 KST에 정상 동작**해 최신 데이터가 이미 확보되어 있습니다. 즉 문제는 크롤러 코드가 아니라 **이 스케줄 작업이 도는 환경의 네트워크 제약**입니다.

---

## 단계별 결과

| 단계 | 내용 | 결과 |
|---|---|---|
| 0 | repo 준비 / `.git` 락 삭제 | ⚠️ `HEAD.lock`·`index.lock` 잔존 (EPERM, 샌드박스에서 삭제 불가) |
| 1 | 브랜드몰(자사몰) 크롤링 | ❌ 0건 (프록시 차단) |
| 2 | 야놀자 크롤링 | ❌ 0건 (프록시 403 차단) |
| 3 | 트립닷컴 크롤링 | ❌ 0건 (프록시 403 차단, 재시도 3회 모두 실패) |
| 4 | 네이버호텔 크롤링 | ❌ 0건 (세션 초기화·GraphQL 모두 차단) |
| 5 | 골프장 크롤링 | ❌ 0건 (프록시 차단 + 환율/공휴일 API 차단) |
| 6 | 대시보드 생성 + git push 배포 | ❌ 미실행 (신규 0건 → 보존, SSH 차단 + 락 잔존) |

탭 순서(브랜드몰 → 야놀자 → 트립닷컴 → 네이버호텔)는 유지했으나 전 단계가 동일 원인으로 0건이었습니다.

---

## 이번 실행 실측 로그

### 네트워크 차단 근거 (직접 확인, curl --max-time 6)
```
https://www.google.com         -> HTTP 000
https://nol.yanolja.com        -> HTTP 000
https://kr.trip.com            -> HTTP 000
https://hotels.naver.com       -> HTTP 000
https://date.nager.at          -> HTTP 000   (공휴일 API → "연휴 자동 판단" 불가)
https://ssh.github.com         -> HTTP 000   (배포 엔드포인트)
```
모든 아웃바운드가 프록시에서 차단(HTTP 000 = 터널 미수립). crawler.py 실행 시 로그에서도
`ProxyError('Unable to connect to proxy', OSError('Tunnel connection failed: 403 Forbidden'))`
가 야놀자/네이버/트립닷컴 전 요청에 동일하게 발생했습니다. 공휴일 API 차단으로 투숙일 기준은 금/토만 적용 가능한 상태였습니다.

### 0단계 — `.git` 락 파일
```
.git/HEAD.lock    (05-27 00:52, 0 byte)
.git/index.lock   (05-28 19:05, 0 byte)
→ rm 시 "Operation not permitted" (EPERM). 샌드박스 권한으로 삭제 불가.
```
이 락은 호스트에서 정리 필요. 단, push 자체가 네트워크 차단으로 불가하므로 이번 실행 영향은 없음.

---

## 데이터 보존 상태 (변경 없음)

호스트 파이프라인이 만든 최신 산출물이 그대로 보존되어 있습니다.

| 산출물 | 최종 갱신 | 비고 |
|---|---|---|
| `exports/sono_competitor_prices_20260604.csv` | 06-04 13:46 | 약 615MB (호텔 가격 원본) |
| `exports/golf_prices_20260604.csv/.xlsx` | 06-04 13:53 | 골프장 가격 |
| `analytics/daily_summary.csv` 외 pivot/trends | 06-04 13:57 | 집계 데이터 |
| `docs/index.html` | 06-04 13:55 | 배포용 대시보드 |

이번 무인 실행에서 위 파일을 포함해 어떤 산출물도 덮어쓰지 않았습니다.

---

## 권장 조치 (호스트에서)

1. **네트워크 허용:** 이 스케줄 작업이 도는 샌드박스에서 `nol.yanolja.com`, `kr.trip.com`, `hotels.naver.com`, `hermes-hotel-svc-api.naver.com`, `date.nager.at`, 자사몰 도메인, `ssh.github.com:443`을 allowlist에 추가해야 크롤링·배포가 정상화됩니다. (현재는 호스트 PC의 별도 파이프라인이 13시대에 대신 수집 중)
2. **`.git` 락 정리:** 호스트에서 `rm -f .git/HEAD.lock .git/index.lock` 1회 실행 권장.
3. 위 두 가지가 해결되기 전까지는 매 04:00 실행이 동일하게 0건/미배포로 종료됩니다.

---

*무인 자동 실행. 신규 크롤링 0건이므로 지침에 따라 기존 데이터 보존, commit/push 미수행.*
