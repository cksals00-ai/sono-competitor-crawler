#!/bin/bash
# 소노 경쟁사 크롤러 - 3 Phase 순차 실행 스크립트
# 생성일: 2026-04-16
# 사용법: cd ~/Projects/sono-competitor-crawler && bash run_all_phases.sh

set -e
cd "$(dirname "$0")"
source venv/bin/activate

echo "=============================================="
echo "  소노 경쟁사 OTA 크롤러 전체 실행"
echo "  시작: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="

# Phase 1: 야놀자 (약 10-15분)
echo ""
echo "[Phase 1/3] 야놀자 크롤링 시작... (예상: 10-15분)"
START1=$(date +%s)
python run_phased.py --phase 1
END1=$(date +%s)
echo "[Phase 1/3] 야놀자 완료 - 소요시간: $(( (END1-START1) / 60 ))분 $(( (END1-START1) % 60 ))초"

# Phase 2: 아고다 (약 15-20분, 재시도 포함)
echo ""
echo "[Phase 2/3] 아고다 크롤링 시작... (예상: 15-20분)"
START2=$(date +%s)
python run_phased.py --phase 2
END2=$(date +%s)
echo "[Phase 2/3] 아고다 완료 - 소요시간: $(( (END2-START2) / 60 ))분 $(( (END2-START2) % 60 ))초"

# Phase 3: 여기어때 (약 10-15분)
echo ""
echo "[Phase 3/3] 여기어때 크롤링 시작... (예상: 10-15분)"
START3=$(date +%s)
python run_phased.py --phase 3
END3=$(date +%s)
echo "[Phase 3/3] 여기어때 완료 - 소요시간: $(( (END3-START3) / 60 ))분 $(( (END3-START3) % 60 ))초"

# 결과 요약
TOTAL=$(( END3 - START1 ))
echo ""
echo "=============================================="
echo "  전체 크롤링 완료!"
echo "  총 소요시간: $(( TOTAL / 60 ))분 $(( TOTAL % 60 ))초"
echo "  완료: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="

# CSV 결과 확인
TODAY=$(date +%Y%m%d)
CSV_FILE="exports/sono_competitor_prices_${TODAY}.csv"
if [ -f "$CSV_FILE" ]; then
    TOTAL_ROWS=$(wc -l < "$CSV_FILE")
    echo ""
    echo "결과 CSV: $CSV_FILE"
    echo "총 행 수: $TOTAL_ROWS"
    echo ""
    echo "OTA별 데이터 수:"
    awk -F',' 'NR>1 {print $5}' "$CSV_FILE" | sort | uniq -c | sort -rn
else
    echo "WARNING: CSV 파일이 생성되지 않았습니다!"
fi

echo ""
echo "로그 확인: logs/phased.log"
echo "대시보드: https://cksals00-ai.github.io/sono-competitor-crawler/"
