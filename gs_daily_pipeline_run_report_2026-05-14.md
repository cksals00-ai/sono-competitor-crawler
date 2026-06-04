# GS 데일리 트렌드 리포트 자동 업데이트 — 실행 보고서

**스케줄 실행 일시:** 2026-05-14 (KST)
**작업명:** daily-db-update-build
**결과:** 실행 실패 (인프라 장애)

## 요약

이번 자동 실행에서는 파이프라인을 한 단계도 진행하지 못했습니다.
사유: 셸 샌드박스가 호스트 디스크 공간 부족으로 매 호출마다 실패.

## 진단 내용

### 1. `mcp__workspace__bash` 호출 결과
모든 셸 호출이 동일한 호스트 레벨 오류로 실패:

```
bash failed on resume, create, and re-resume.
resume: RPC error: ensure user: useradd failed: exit status 1:
useradd: /etc/passwd.NNNNN: No space left on device
useradd: cannot lock /etc/passwd; try again later.
```

`echo test`처럼 가장 단순한 명령도 실패. 일시적이지 않고 같은 메시지로 10회 이상 반복 재현됨. 본 세션의 샌드박스 호스트 디스크가 가득 차서 `useradd`(사용자 셸 생성 단계)에서 막힌 것으로, 본 세션 내부에서 해결할 수 없는 인프라 문제로 판단됩니다.

### 2. 대안 경로 시도 — 컴퓨터 사용(AppleScript)
- macOS 터미널은 "click" 등급이라 키 입력이 차단됨 → 직접 명령 실행 불가.
- AppleScript Editor 실행을 위해 `request_access` 호출:
  - `"스크립트 편집기"`, `"Script Editor"` 둘 다 설치 목록 매칭 실패.
  - `com.apple.ScriptEditor2` 번들 ID로 재시도 → 180초 타임아웃 (이 실행은 *예약 작업*이라 사용자가 부재해 권한 승인 대화상자에 응답 불가).

결과적으로 셸 명령을 실행할 수 있는 모든 경로가 막혀 있어 1단계(`scripts/check_raw_db_changes.py`)부터 실행 불가.

### 3. 파일 시스템 점검
- 작업 디렉토리 `~/Desktop/gs_daily_trend_news_public_temp` 접근 OK.
- 필요한 스크립트 모두 존재 확인:
  - `scripts/check_raw_db_changes.py`
  - `scripts/parse_raw_db.py`
  - `scripts/compare_and_update.py`
  - `scripts/generate_otb_data.py`
  - `scripts/generate_insights.py`
  - `scripts/build.py`
- `data/.raw_db_snapshot.json` 존재. 현재 등록된 txt 파일 mtime 최신값은 약 `1777127523` (2026-04-26 KST). 5/14 시점 기준 약 2주 묵은 스냅샷이지만, **실제 파일의 현재 mtime을 셸 없이 확인할 수 없어 변경 여부 판단 불가**.

## 빌드 결과
**진행하지 않음.** 1단계 변경 감지를 실행하지 못했으므로 빌드/커밋/푸시 모두 스킵.

## 수동 복구를 위해 준비한 산출물

작업 디렉토리에 다음 셸 래퍼 스크립트를 작성해 두었습니다. 실행 환경이 복구되면 한 줄로 전체 파이프라인을 재현할 수 있습니다:

- 경로: `~/Desktop/gs_daily_trend_news_public_temp/scripts/_run_daily_pipeline.sh`
- 동작:
  1. `check_raw_db_changes.py --wait --interval 600 --max-wait 7200`
  2. 변경 감지 시 `parse_raw_db.py → compare_and_update.py → generate_otb_data.py → generate_insights.py → build.py` 순차 실행
  3. `git add -A && git commit && git push origin main` (push 실패 시 `pull --rebase` 후 1회 재시도)
  4. 진행 로그를 `_pipeline_run.log`, 단계별 상태를 `_pipeline_status.json` 에 기록
  5. `.git/index.lock`, `.git/HEAD.lock` 사전 정리 포함

수동 실행 명령:
```
bash ~/Desktop/gs_daily_trend_news_public_temp/scripts/_run_daily_pipeline.sh
```

## 권장 조치
1. 본 세션의 샌드박스가 망가져 있으므로 Cowork 세션 재시작 또는 호스트 디스크 정리 후 스케줄 작업 재실행.
2. 스케줄 작업이 사용자 부재 시 컴퓨터 사용(권한 승인) 경로로 폴백하지 못함 — 파이프라인이 셸 샌드박스 가용성에 전적으로 의존하므로 헬스체크 권장.
3. 다음 정기 실행에서는 셸 가용 여부를 먼저 한 줄(`echo ok`)로 확인하고 실패 시 즉시 본 형식의 보고서로 종료하도록 SKILL.md를 보완하는 것을 권장.
