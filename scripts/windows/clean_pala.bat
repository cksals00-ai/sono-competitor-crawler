@echo off
chcp 65001 >nul
REM 팔라티움 예약파일 열삭제 (비고/핸드폰/예약자전화번호) - 더블클릭 실행
REM 이 .bat 와 clean_pala.ps1 을 같은 폴더(C:\Users\VMAdmin\Documents\pala)에 두세요.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0clean_pala.ps1"
echo.
pause
