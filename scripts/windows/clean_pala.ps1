# clean_pala.ps1
# 예약정보조회 xlsx 에서 비고 / 핸드폰 / 예약자전화번호 열을 일괄 삭제 (파일 안 열고, 제자리 저장)
# - 이 스크립트가 놓인 폴더(pala)의 하위 날짜폴더들을 훑어 *예약정보조회*.xlsx 처리
# - Excel 을 백그라운드(비표시)로 구동. 헤더명으로 열을 찾아 삭제하므로 위치 무관, 이미 없으면 건너뜀
# - 한글 인코딩 안전성을 위해 컬럼명은 유니코드 코드포인트로 구성

$ErrorActionPreference = "Stop"

# 대상 폴더 = 스크립트가 놓인 폴더 (없으면 기본 경로)
$base = if ($PSScriptRoot) { $PSScriptRoot } else { "C:\Users\VMAdmin\Documents\pala" }

# 컬럼명(코드포인트)  비고 / 핸드폰 / 예약자전화번호 / 상태 / 예약정보조회
$colBigo   = -join (@(0xBE44,0xACE0)                                   | ForEach-Object {[char]$_})
$colPhone  = -join (@(0xD578,0xB4DC,0xD3F0)                            | ForEach-Object {[char]$_})
$colTel    = -join (@(0xC608,0xC57D,0xC790,0xC804,0xD654,0xBC88,0xD638)| ForEach-Object {[char]$_})
$colStatus = -join (@(0xC0C1,0xD0DC)                                   | ForEach-Object {[char]$_})
$kwRes     = -join (@(0xC608,0xC57D,0xC815,0xBCF4,0xC870,0xD68C)       | ForEach-Object {[char]$_})
$targets   = @($colBigo, $colPhone, $colTel)

if (-not (Test-Path $base)) { Write-Host "[!] Folder not found: $base"; Start-Sleep 5; exit 1 }

$files = Get-ChildItem -Path $base -Recurse -Filter *.xlsx |
    Where-Object { $_.Name -like "*$kwRes*" -and $_.Name -notlike '~$*' }

if (-not $files -or $files.Count -eq 0) {
    Write-Host "[i] No target xlsx found under $base"; Start-Sleep 5; exit 0
}

Write-Host ("[i] {0} file(s) found. Removing columns (bigo/phone/tel)..." -f $files.Count)
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
            # 헤더행 찾기(상태 위치, 앞 10행 내)
            $hdrRow = 0
            for ($r = $rowOff; $r -lt ($rowOff + 10); $r++) {
                for ($c = $colOff; $c -lt ($colOff + $nCols); $c++) {
                    if (("" + $ws.Cells.Item($r, $c).Text).Trim() -eq $colStatus) { $hdrRow = $r; break }
                }
                if ($hdrRow) { break }
            }
            if (-not $hdrRow) { continue }
            # 대상 열 인덱스 수집 후 오른쪽→왼쪽 삭제
            $del = @()
            for ($c = $colOff; $c -lt ($colOff + $nCols); $c++) {
                $name = ("" + $ws.Cells.Item($hdrRow, $c).Text).Trim()
                if ($targets -contains $name) { $del += $c }
            }
            [array]::Reverse($del)
            foreach ($c in $del) { [void]$ws.Cells.Item(1, $c).EntireColumn.Delete(); $removed++ }
        }
        if ($removed -gt 0) { $wb.Save() }
        $wb.Close($false)
        $total += $removed
        Write-Host ("    [{0} removed] {1}\{2}" -f $removed, (Split-Path $f.DirectoryName -Leaf), $f.Name)
    } catch {
        Write-Host ("    [ERROR] {0}: {1}" -f $f.Name, $_.Exception.Message)
        if ($wb) { try { $wb.Close($false) } catch {} }
    }
}

$excel.Quit()
[void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel)
Write-Host ("[OK] Done. Total {0} columns removed. Close this window." -f $total)
Start-Sleep 6
