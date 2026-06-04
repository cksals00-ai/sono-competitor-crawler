# 트렌드리포트 권역별 정보 + 경쟁사 프로모션/인사이트 — 스케줄 실행 리포트

- **작업명:** trend-report-regional (매일 02:00 KST)
- **실행일시:** 2026-05-29 02:07~02:08 KST (자동, 무인)
- **프로젝트:** ~/Desktop/gs_daily_trend_news_public_temp
- **전체 결과:** ⚠️ **빌드/커밋 성공 · 푸시 실패(샌드박스 외부망 차단)**

---

## 한 줄 요약

5단계 파이프라인(락 정리 → 경쟁사 수집 → 인사이트 → 빌드 → 커밋/푸시) 중 **로컬 4단계까지 정상 완료**되어 `docs/index.html`(1.79MB)이 02:07 KST 기준으로 리빌드되었고 로컬 커밋 `1c8af8f` 까지 만들어졌습니다. 다만 **GitHub 푸시는 SSH(ssh.github.com:443 → `403 Forbidden` via 프록시)와 HTTPS(github.com DNS 해석 비활성) 모두 차단**되어 실패했으며, `git pull --rebase` 재시도도 동일 사유로 실패했습니다. 호스트(macOS)에서 한 번 `git push origin main`만 실행해주시면 원격에 반영됩니다.

또한 경쟁사 수집기(`scripts/collect_gs_monitor.py`)는 sono-competitor-crawler 의 GitHub Pages/raw 8개 후보 URL 모두 응답이 없어, 지침대로 **기존 `data/competitors.json`을 그대로 보존**(9건, `_updated_at` 2026-04-21)했습니다.

---

## 단계별 결과

| 단계 | 내용 | 결과 |
|---|---|---|
| 0 | `.git/index.lock`, `.git/HEAD.lock` 정리 | ⚠️ 신규 락은 없음. 잔존 락(`HEAD.lock`, `index.lock`)은 FUSE 마운트 권한 문제로 제거 불가 (호스트 1회 수동 권장) |
| 1 | 경쟁사 프로모션 수집 (`scripts/collect_gs_monitor.py`) | ⚠️ 8개 원격 소스 모두 응답 없음 → 기존 JSON 보존(9건) |
| 2 | 인사이트 자동 생성 (`scripts/generate_insights.py`) | ✅ `enriched_notes.json` 갱신 — `today_headline` + 자동 헤드라인 3건 + `action_alerts` 4권역 |
| 3 | 전체 HTML 빌드 (`scripts/build.py`) | ✅ `docs/index.html` 1,794,446 bytes · `docs/otb.html` 225,759 bytes |
| 4 | `git add -A && git commit` | ✅ commit `1c8af8f` (11 files, +3,901 / -3,780) |
| 5 | `git push origin main` | ❌ SSH/HTTPS 모두 차단 — 로컬 main이 origin/main보다 1 커밋 앞섬 |

---

## 단계별 상세

### 0단계 — `.git` 락 파일
```
rm -f .git/index.lock .git/HEAD.lock
→ 둘 다 신규 락 아닌 잔존 파일 (기존 보고서들과 동일 상태)
→ commit 중에도 다음 경고 발생:
  warning: unable to unlink '.git/HEAD.lock': Operation not permitted
  warning: unable to unlink '.git/objects/**/tmp_obj_***': Operation not permitted (14건)
```
호스트(macOS) 터미널에서 1회 정리 권장:
```
cd ~/Desktop/gs_daily_trend_news_public_temp
rm -f .git/HEAD.lock .git/index.lock
find .git/objects -name 'tmp_obj_*' -delete
```

### 1단계 — 경쟁사 프로모션 수집 (`scripts/collect_gs_monitor.py`)
> 지침서에는 `scripts/gs_monitor_collector.py` 로 표기되어 있으나 실제 파일명은 `scripts/collect_gs_monitor.py`. 동일 목적 스크립트로 실행했습니다.

```
시도: https://cksals00-ai.github.io/sono-competitor-crawler/data/latest.json
시도: https://cksals00-ai.github.io/sono-competitor-crawler/data/competitors.json
시도: https://cksals00-ai.github.io/sono-competitor-crawler/data.json
시도: https://cksals00-ai.github.io/sono-competitor-crawler/competitors.json
시도: https://raw.githubusercontent.com/cksals00-ai/sono-competitor-crawler/main/data/latest.json
시도: https://raw.githubusercontent.com/cksals00-ai/sono-competitor-crawler/main/data/competitors.json
시도: https://raw.githubusercontent.com/cksals00-ai/sono-competitor-crawler/main/data.json
시도: https://cksals00-ai.github.io/sono-competitor-crawler/
[WARNING] ⚠ 모든 소스 실패. 기존 competitors.json 유지.
[INFO]   ✓ 기존 competitors.json 유지 (9건)
```
원인 추정: (a) sono-competitor-crawler 레포가 위 경로에 `latest.json/competitors.json`을 게시하지 않음, (b) 샌드박스 allowlist가 해당 호스트를 차단. 어느 쪽이든 지침의 "에러 발생 시 기존 JSON 보존" 정책에 부합하므로 더미 데이터는 일절 작성하지 않았습니다.

### 2단계 — 인사이트 자동 생성 (`scripts/generate_insights.py`)
```
✓ KPI 로드: K1=None, K2=None, K3=None
✓ db_aggregated 로드: monthly_total 58개월
✓ 뉴스 로드: 12 TOP / 전체 320건
✓ 자동 생성 완료
  DB 인사이트: 3개
  [1] 5월 온북 77,857RN · 전월(4월 78,974RN) 대비 ▼ 1.4% 감소.
  [2] 사업장별 5월 온북 상위 소노캄 고양 8,949RN (11.5%) · 하위 오션월드빌리지 86RN (0.1%) — 25개 사업장 집계.
  [3] 전년 동월(2025/05) 대비 ▲ 18.7% 성장 · 올해 77,857RN vs 전년 65,576RN.
  저장: data/enriched_notes.json
```
체크포인트 키 확인 — `today_headline`, `today_headlines`(3건), `action_alerts`(4건: vivaldi/central/south/apac), `region_status`(4권역) 모두 생성됨. 지침에 적힌 `headline` 키는 현재 스키마에는 없고 동일 의미인 `today_headline`을 사용 중.

### 3단계 — 빌드 (`scripts/build.py`)
```
✓ patch_channel_daily (skip — 이미 존재)
✓ generate_otb_data
✓ build_weekly_comparison
✓ admin_suggestions.json 생성 (4개 인사이트)
✓ vivaldi: 5개 신호등 / central: 7개 / south: 7개 / apac: 3개 주입
✓ 경쟁사 카드 주입: 9개
✓ Daily OTB 주입 / 주간 리포트 / YoY 사업장별 추이 90개 / 인사이트 패널 / 카테고리별 뉴스 320건 / Daily Booking 25사업장×3개월
✓ index.html 빌드 완료 (1,716,058 → 1,794,446 bytes 최종)
✓ otb.html 빌드 완료 (225,759 bytes)
✓ docs/data/db_aggregated.json, package_series_trend.json, rm_fcst.json 동기화
✓ parse_overseas / data_freshness.json (5개 소스)
✓ 전체 빌드 완료 · Auto-Built 2026-05-29 02:07 KST
```

### 4단계 — commit
```
[main 1c8af8f] chore(auto): trend regional update 2026-05-29 02:08 KST [skip ci]
 11 files changed, 3901 insertions(+), 3780 deletions(-)
```
변경 파일: `data/admin_input.json`, `data/enriched_notes.json`, `docs/admin.html`, `docs/admin_suggestions.json`, `docs/data/admin_input.json`, `docs/data/daily_analysis_validation.json`, `docs/data/data_freshness.json`, `docs/data/otb_data.json`, `docs/data/weekly_comparison.json`, `docs/index.html`, `docs/otb.html`.

### 5단계 — push (실패)
```
$ git push origin main
2026/05/28 17:08:15 socat[10] E CONNECT ssh.github.com:443: Forbidden
kex_exchange_identification: Connection closed by remote host
fatal: Could not read from remote repository.

$ git pull --rebase origin main      # 지침대로 재시도
(동일한 ssh.github.com:443 Forbidden)

$ git push https://github.com/...    # HTTPS 폴백 시도
fatal: unable to access ...: Could not resolve host: github.com
```
- 원격: `ssh://git@ssh.github.com:443/cksals00-ai/gs_daily_trend_news_public_temp.git`
- 현재 상태: `## main...origin/main [ahead 1]`

---

## 체크포인트 확인 (지침서 기준)

| 체크 | 결과 |
|---|---|
| competitors.json 경쟁사 프로모션 수 / 최신 수집일시 | competitors 9건 보존 · `_updated_at` 2026-04-21 (이번 실행에서 신규 수집 0건) |
| enriched_notes.json headline / action_alerts 갱신 | ✅ `today_headline` + 3건 `today_headlines` 신규, `action_alerts` 4권역 신규, `_generated_at` 2026-05-29T02:07:09+09:00 |
| docs/index.html 파일 크기 ≥ 100KB | ✅ 1,794,446 bytes (약 1.79MB) |

---

## 다음 액션 (호스트에서 1회 실행 권장)

```bash
cd ~/Desktop/gs_daily_trend_news_public_temp
rm -f .git/HEAD.lock .git/index.lock
find .git/objects -name 'tmp_obj_*' -delete
git push origin main         # 로컬 커밋 1c8af8f 원격 반영
```

`competitors.json`을 실제로 갱신하고 싶다면 sono-competitor-crawler 측에서 `data/latest.json` 또는 `data/competitors.json` 산출물을 GitHub Pages 경로(`https://cksals00-ai.github.io/sono-competitor-crawler/data/...`)에 게시해두어야 다음 스케줄 실행에서 수집기가 받아옵니다. 현재는 어떤 후보 URL에도 파일이 노출돼 있지 않은 상태입니다.
