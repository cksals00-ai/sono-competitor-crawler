# RemovePrivacy.ps1
# 예약정보조회 xlsx 에서 개인정보 3열(비고 / 핸드폰 / 예약자전화번호)을 삭제.
#  - 이 스크립트(.bat)가 놓인 "같은 폴더" 안의 *예약정보조회*.xlsx 를 처리(제자리 저장).
#  - Excel 을 화면에 안 띄우고 백그라운드로 처리. 헤더명으로 열을 찾아 삭제하므로 열 위치 무관.
#  - 이미 지워진 파일/빈 양식/사업계획/객실계획 파일은 자동 건너뜀. 파일 안 열어도 됨.
#  - 한글 컬럼명은 유니코드 코드포인트로 구성(파일 인코딩 깨짐 방지).

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 대상 폴더 = 이 스크립트가 놓인 폴더
$base = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }

# 컬럼/키워드(코드포인트)
$colStatus = -join (@(0xC0C1,0xD0DC)                                    | ForEach-Object {[char]$_})  # 상태
$colBigo   = -join (@(0xBE44,0xACE0)                                    | ForEach-Object {[char]$_})  # 비고
$colPhone  = -join (@(0xD578,0xB4DC,0xD3F0)                             | ForEach-Object {[char]$_})  # 핸드폰
$colTel    = -join (@(0xC608,0xC57D,0xC790,0xC804,0xD654,0xBC88,0xD638) | ForEach-Object {[char]$_})  # 예약자전화번호
$kwRes     = -join (@(0xC608,0xC57D,0xC815,0xBCF4,0xC870,0xD68C)        | ForEach-Object {[char]$_})  # 예약정보조회
$kwForm    = -join (@(0xC591,0xC2DD)                                    | ForEach-Object {[char]$_})  # 양식
$kwPlan1   = -join (@(0xC0AC,0xC5C5,0xACC4,0xD68D)                      | ForEach-Object {[char]$_})  # 사업계획
$kwPlan2   = -join (@(0xAC1D,0xC2E4,0xACC4,0xD68D)                      | ForEach-Object {[char]$_})  # 객실계획
$targets   = @($colBigo, $colPhone, $colTel)

$files = Get-ChildItem -Path $base -Filter *.xlsx -File | Where-Object {
    $_.Name -like "*$kwRes*" -and $_.Name -notlike '~$*' `
    -and $_.Name -notlike "*$kwForm*" -and $_.Name -notlike "*$kwPlan1*" -and $_.Name -notlike "*$kwPlan2*"
}

if (-not $files -or $files.Count -eq 0) {
    Write-Host ("[i] 처리할 예약정보조회 파일이 없습니다: {0}" -f $base)
    return
}

Write-Host ("[i] {0}개 파일 처리 시작 (비고/핸드폰/예약자전화번호 삭제)..." -f $files.Count)
Write-Host ""

$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$excel.DisplayAlerts = $false
$total = 0

foreach ($f in $files) {
    $wb = $null
    try {
        $wb = $excel.Workbooks.Open($f.FullName)
        $removed = 0
        foreach ($ws in $wb.Worksheets) {
            $ur = $ws.UsedRange
            $rowOff = $ur.Row; $colOff = $ur.Column; $nCols = $ur.Columns.Count
            # 헤더행 탐지: '상태' 셀이 있는 첫 행(앞 10행 내 — 타이틀/날짜행 스킵)
            $hdrRow = 0
            for ($r = $rowOff; $r -lt ($rowOff + 10); $r++) {
                for ($c = $colOff; $c -lt ($colOff + $nCols); $c++) {
                    if (("" + $ws.Cells.Item($r, $c).Text).Trim() -eq $colStatus) { $hdrRow = $r; break }
                }
                if ($hdrRow) { break }
            }
            if (-not $hdrRow) { continue }
            # 대상 열 인덱스 수집 후 오른쪽→왼쪽으로 삭제(인덱스 밀림 방지)
            $del = @()
            for ($c = $colOff; $c -lt ($colOff + $nCols); $c++) {
                $name = ("" + $ws.Cells.Item($hdrRow, $c).Text).Trim()
                if ($targets -contains $name) { $del += $c }
            }
            [array]::Reverse($del)
            foreach ($c in $del) { [void]$ws.Cells.Item($hdrRow, $c).EntireColumn.Delete(); $removed++ }
        }
        if ($removed -gt 0) { $wb.Save() }
        $wb.Close($false)
        $total += $removed
        if ($removed -gt 0) { Write-Host ("  [{0}열 삭제] {1}" -f $removed, $f.Name) }
        else                { Write-Host ("  [건너뜀·삭제할 열 없음] {0}" -f $f.Name) }
    } catch {
        Write-Host ("  [오류] {0} : {1}" -f $f.Name, $_.Exception.Message)
        if ($wb) { try { $wb.Close($false) } catch {} }
    }
}

$excel.Quit()
[void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel)
Write-Host ""
Write-Host ("[완료] 총 {0}개 열 삭제됨. 정리된 파일을 맥으로 옮기세요." -f $total)
