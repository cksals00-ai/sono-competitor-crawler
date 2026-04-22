#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# FINISH_MANUAL.sh — 2026-04-20 예약 작업 자동 완료 스크립트 (사용자 Mac에서 실행)
#
# 샌드박스에서 할 수 없었던 일:
#   1) .git/index.lock 제거 (권한 부족)
#   2) kr.trip.com 접속 (프록시 403 차단)
#   3) GitHub push (프록시 403 차단)
#
# 이 스크립트는 Mac에서 위 3가지를 순서대로 마무리한다.
# ──────────────────────────────────────────────────────────────────────────────
set -e

cd "$(dirname "$0")"

echo "=== 1) .git/index.lock 제거 ==="
rm -f .git/index.lock

echo ""
echo "=== 2) 코드 수정 커밋 (crawler.py, run_phased.py, dashboard_generator.py) ==="
git add crawler.py run_phased.py dashboard_generator.py
git commit -m "fix: Trip.com 파서 수정 + 골프 대시보드 통합 + 객실요금 라벨

- crawler.py: Trip.com 상세 페이지에서 Googlebot UA 사용 + spiderRoomList
  CSS 파싱으로 방 이름 추출, 가격은 2차/3차 폴백과 결합
- run_phased.py: _generate_dashboard에 golf_df 로드/전달 추가하여
  대시보드에 골프 섹션 포함, main()에서 골프 크롤링 완료 후 대시보드
  재생성하는 블록 추가
- dashboard_generator.py: '브래드닷컴 객실요금' → '객실요금' 라벨 단순화" || echo "(변경사항 없음이면 무시)"

echo ""
echo "=== 3) Trip.com 재크롤링 (Phase 5, 새 파서 적용, ~40분) ==="
python3 run_phased.py --phase 5

echo ""
echo "=== 4) 대시보드 docs/ 복사 (이미 run_phased 가 처리했을 것) ==="
cp dashboard/index.html docs/index.html

echo ""
echo "=== 5) 변경사항 전부 push ==="
TODAY=$(date +%Y%m%d)
git add docs/index.html \
        "exports/sono_competitor_prices_${TODAY}.csv" \
        "exports/sono_competitor_prices_${TODAY}.xlsx" \
        "exports/sono_competitor_prices_latest.xlsx" \
        "exports/golf_prices_${TODAY}.csv" \
        "exports/golf_prices_${TODAY}.xlsx" \
        "data/powerbi_rns_${TODAY}.json" \
        "data/powerbi_rns_latest.json" \
        channel_sales_data.json 2>/dev/null || true
git commit -m "dashboard: Trip.com 재크롤링(새 파서) + 골프 섹션 포함 $(date '+%Y-%m-%d %H:%M')" || true
git push

echo ""
echo "=== 6) 불필요한 worktree 정리 ==="
git worktree remove --force .claude/worktrees/distracted-ramanujan 2>/dev/null || true
git worktree prune
git branch -D claude/distracted-ramanujan 2>/dev/null || true

echo ""
echo "=== 7) GitHub Pages 배포 확인 ==="
echo "   https://cksals00-ai.github.io/sono-competitor-crawler/ 에서 확인"

echo ""
echo "=== 완료! ==="
