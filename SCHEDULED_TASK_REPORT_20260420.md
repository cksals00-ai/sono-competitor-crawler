# 예약 작업 보고 — 2026-04-20 저녁

## 완료된 작업

### 1. 사전 확인 — 대기 완료
- `logs/phased.log` 에 19:00 에 새 phased run 이 시작되어 있었음 (phase 1 야놀자부터 시작).
- 5 분 간격으로 진행 상황을 모니터링하며 완료를 대기 (19:00 → 21:05, 약 2 시간 5 분 소요).
- phase 1 → 4 → 5 → 골프 → Power BI RNS 수집까지 모두 정상 완료 확인.
- 완료 시점 기준 main 은 `f414ef4` (phase 5 완료 커밋) 에 있었고, `73561dc`(phase1), `b86f790`(phase4), `f414ef4`(phase5) 3개 커밋이 이미 push 된 상태.

### 2. Trip.com 파서 수정 머지 — 완료
- `claude/distracted-ramanujan` 워크트리의 `crawler.py` 를 main 브랜치의 `crawler.py` 로 복사 (task 지시대로 distracted-ramanujan 쪽 우선 채택).
- diff 검증 결과 핵심 변경사항 3 건 모두 포함:
  1. `_parse_tripcom_detail_rooms()` — `spiderRoomList_roomNameA` CSS 셀렉터로 방 이름 SSR 파싱 추가, price=0 허용.
  2. `_fetch_tripcom_detail_rooms()` — User-Agent 를 Googlebot 으로 변경 (Trip.com v2 페이지는 Googlebot 에게만 SSR 렌더).
  3. `crawl_tripcom()` — 방 이름 파싱 성공 시 2차/3차 폴백 가격과 결합, room_type 에 실제 방 이름 기록.
- 워크트리 쪽 crawler.py 가 이미 main 의 모든 변경사항(`dc431ba`, `6b8d1ee`)을 포함하고 있었으므로 충돌 없음.

### 3. 골프 대시보드 누락 수정 — 완료
`run_phased.py` 2 곳 수정:
1. `_generate_dashboard()` — `load_golf_df` import 추가, `golf_df = load_golf_df(export_dir)` 로드, `generate_dashboard(..., golf_df=golf_df)` 로 전달.
2. `main()` 의 골프 크롤링 블록 뒤 — 골프 CSV 저장 직후 대시보드를 다시 생성/복사/커밋/push 하는 블록 추가.

### 4. "객실요금" 라벨 확인 — 이미 적용되어 있었음 (커밋 대기 중)
`dashboard_generator.py` 는 working tree 에 이미 수정되어 있었음 (과거 세션 산출물).
- `fit-toggle-label`: `브래드닷컴 객실요금` → `객실요금` ✓
- `hp-th-price`: `브래드닷컴 객실요금` → `객실요금` ✓

이 변경은 아직 커밋되지 않은 상태였는데, `FINISH_MANUAL.sh` 에서 같이 커밋하도록 포함함.

### 5. 대시보드 재생성 (골프 포함) — 로컬 반영 완료
`docs/index.html` 을 새 `generate_dashboard(golf_df=...)` 로 재생성.
- 재생성 전: `<p ...>골프 데이터가 없습니다.</p>` (빈 섹션)
- 재생성 후: 실제 골프 행 12 건 이상 포함 (망길라오 골프 클럽, 탈로포포 골프 클럽, 드래곤 골프 링크스, 뚜언쩌우 골프 리조트, 빈펄 골프 하이퐁 등).
- "객실요금" 라벨이 결과물에 올바르게 포함되어 있음 (사용자 노출 라벨 기준; `/* ── 브래드닷컴 객실요금 Section ── */` 는 CSS/JS 주석이라 2 건 남아있으나 무해).

## 완료하지 못한 작업 (샌드박스 제약)

### A. `.git/index.lock` 제거 (권한 부족)
- phased run 종료 직후 stale lock 이 남아있음 (0 byte, 21:08:20).
- 샌드박스에서 `rm -f` 가 "Operation not permitted" 로 실패. mount 의 read-only 성격 때문으로 보임.
- → 사용자 Mac 에서 `rm -f .git/index.lock` 실행 필요.

### B. Trip.com phase 5 재크롤링
- `python3 run_phased.py --phase 5` 시도했으나 모든 `kr.trip.com` 요청이 "Tunnel connection failed: 403 Forbidden" 으로 실패.
- 샌드박스 프록시가 Trip.com 도메인을 차단함.
- → 사용자 Mac 에서 재실행 필요 (~40 분).

### C. Git push
- `git push` 시도 시 "Received HTTP code 403 from proxy after CONNECT" 발생.
- GitHub 접속도 프록시에서 차단됨.
- → 사용자 Mac 에서 수동 push 필요.

## 다음 행동 — 사용자 수동 실행 필요

프로젝트 루트에 `FINISH_MANUAL.sh` 를 만들어 두었음. Mac 터미널에서:

```bash
cd ~/Projects/sono-competitor-crawler
./FINISH_MANUAL.sh
```

이 스크립트가 순서대로 처리:
1. `.git/index.lock` 제거
2. 코드 수정 3 개 커밋 (crawler.py, run_phased.py, dashboard_generator.py)
3. `python3 run_phased.py --phase 5` 실행 (새 파서로 Trip.com 재크롤링, ~40 분)
4. `docs/index.html` 복사
5. 데이터 파일 + 대시보드 커밋 & push
6. `distracted-ramanujan` worktree 정리

## Trip.com 사전 데이터 (새 파서 적용 전, 구 파서 결과)

현재 `exports/sono_competitor_prices_20260420.csv` 의 Trip.com 행 600 건의 `객실유형`:
- `최저가(참고)`: 425 건
- `최저가`: 159 건
- 실제 방 이름 0 건 (0.0%)

새 파서로 재크롤링하면 `spiderRoomList_roomNameA` 에서 추출한 실제 방 이름이 들어가야 함 (예: "디럭스 더블룸", "스탠다드 트윈" 등). FINISH_MANUAL.sh 실행 후 동일한 집계를 돌려 검증할 것.

## 참고

- 샌드박스 시각: 2026-04-20 21:10 KST
- phased run 총 소요: 19:00:02 → 21:05:09 (2 시간 5 분 7 초)
- `logs/phased.log` 마지막 줄: `=== Power BI RNS 수집 완료 ===` (21:05:09)
- `logs/phase5_rerun.log`: 샌드박스 phase 5 재시도 로그 (프록시 403 에러로 전수 실패)
