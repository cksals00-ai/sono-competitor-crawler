#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=============================================="
echo "  전체 대시보드 빌드 시작"
echo "  시작: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="

TREND_DIR="$HOME/Desktop/gs_daily_trend_news_public_temp"
PALATIUM_DIR="$HOME/Projects/sono-competitor-crawler"

# ─── 1. 팔라티움 (이미 완료됨 - 스킵 가능) ───
echo ""
echo "[1/5] 팔라티움 빌드 (이미 완료 - 스킵)"

# ─── 2. parse_raw_db.py ───
echo ""
echo "[2/5] parse_raw_db.py 실행 중..."
START=$(date +%s)
cd "$TREND_DIR"
python3 scripts/parse_raw_db.py
END=$(date +%s)
echo "[2/5] parse_raw_db.py 완료 - $(( END - START ))초"

# ─── 3. 기획전 실적 집계 ───
echo ""
echo "[3/6] generate_campaign_performance.py 실행 중..."
python3 scripts/generate_campaign_performance.py || echo "⚠ 기획전 실적 집계 실패 - 기존 데이터 유지"

# ─── 4. build.py ───
echo ""
echo "[4/6] build.py 실행 중..."
START=$(date +%s)
python3 scripts/build.py
END=$(date +%s)
echo "[4/6] build.py 완료 - $(( END - START ))초"

# ─── 5. 빌드 검증 ───
echo ""
echo "[5/6] 빌드 검증 중..."
python3 "$PALATIUM_DIR/verify_build.py"
VERIFY_EXIT=$?
if [ $VERIFY_EXIT -ne 0 ]; then
  echo "⚠ 빌드 검증 실패 — Git push를 건너뜁니다"
  exit 1
fi

# ─── 6. Git push ───
echo ""
echo "[6/6] Git push..."
cd "$TREND_DIR"
git add -A
git commit -m "chore(auto): daily update $(date '+%Y-%m-%d %H:%M') KST [skip ci]" || echo "커밋할 변경 없음"
git push || echo "⚠ 푸시 실패"

cd "$PALATIUM_DIR"
git add -A
git commit -m "chore(auto): palatium update $(date '+%Y-%m-%d %H:%M') KST [skip ci]" || echo "커밋할 변경 없음"
git push || echo "⚠ 푸시 실패"

echo ""
echo "=============================================="
echo "  전체 빌드 완료: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="
