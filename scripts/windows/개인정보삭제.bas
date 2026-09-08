Attribute VB_Name = "개인정보삭제"
' ============================================================
'  예약정보조회 개인정보 3열 삭제 (비고 / 핸드폰 / 예약자전화번호)
'  - 이 .xlsm 이 있는 "같은 폴더"의 *예약정보조회*.xlsx 를 열어
'    3열을 지운 뒤, 하위 "정리완료" 폴더에 복사본으로 저장(원본 보존).
'  - 실행: Alt+F8 → 개인정보삭제_일괄 → 실행  (또는 버튼 연결)
' ============================================================
Option Explicit

Sub 개인정보삭제_일괄()
    Dim srcFolder As String, outFolder As String
    srcFolder = ThisWorkbook.Path
    If srcFolder = "" Then
        MsgBox "먼저 이 파일을 원하는 폴더에 저장(.xlsm)한 뒤 실행하세요.", vbExclamation
        Exit Sub
    End If
    outFolder = srcFolder & "\정리완료"
    If Dir(outFolder, vbDirectory) = "" Then MkDir outFolder

    ' 1) 대상 파일명 먼저 수집 (Dir 재진입 방지)
    Dim names() As String, n As Long, fn As String
    ReDim names(1 To 2000)
    fn = Dir(srcFolder & "\*예약정보조회*.xlsx")
    Do While fn <> ""
        If InStr(fn, "양식") = 0 And InStr(fn, "사업계획") = 0 _
           And InStr(fn, "객실계획") = 0 And Left(fn, 2) <> "~$" Then
            n = n + 1: names(n) = fn
        End If
        fn = Dir()
    Loop
    If n = 0 Then
        MsgBox "이 폴더에 예약정보조회 파일이 없습니다:" & vbCrLf & srcFolder, vbInformation
        Exit Sub
    End If

    ' 2) 처리
    Dim targets As Variant
    targets = Array("비고", "핸드폰", "예약자전화번호")
    Dim i As Long, cnt As Long, totCols As Long
    Application.ScreenUpdating = False
    Application.DisplayAlerts = False
    On Error GoTo cleanup

    For i = 1 To n
        Dim wb As Workbook, ws As Worksheet, removed As Long
        Set wb = Workbooks.Open(srcFolder & "\" & names(i))
        removed = 0
        For Each ws In wb.Worksheets
            Dim r As Long, c As Long, hdrRow As Long, lastCol As Long, t As Variant
            hdrRow = 0
            For r = 1 To 10
                For c = 1 To 60
                    If Trim(CStr(ws.Cells(r, c).Value)) = "상태" Then hdrRow = r: Exit For
                Next c
                If hdrRow > 0 Then Exit For
            Next r
            If hdrRow > 0 Then
                lastCol = ws.Cells(hdrRow, ws.Columns.Count).End(xlToLeft).Column
                For c = lastCol To 1 Step -1
                    For Each t In targets
                        If Trim(CStr(ws.Cells(hdrRow, c).Value)) = CStr(t) Then
                            ws.Columns(c).Delete
                            removed = removed + 1
                            Exit For
                        End If
                    Next t
                Next c
            End If
        Next ws
        ' 다른 폴더(정리완료)에 xlsx 로 저장 → 원본은 그대로
        wb.SaveAs Filename:=outFolder & "\" & names(i), FileFormat:=xlOpenXMLWorkbook
        wb.Close SaveChanges:=False
        cnt = cnt + 1: totCols = totCols + removed
    Next i

cleanup:
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
    If Err.Number <> 0 Then
        MsgBox "오류 발생: " & Err.Description & vbCrLf & "(" & cnt & "개까지 처리됨)", vbCritical
    Else
        MsgBox cnt & "개 파일 처리 완료 · 총 " & totCols & "개 열 삭제" & vbCrLf & vbCrLf & _
               "정리된 파일 저장 위치:" & vbCrLf & outFolder & vbCrLf & vbCrLf & _
               "이 폴더의 파일들을 맥으로 옮기세요.", vbInformation, "완료"
    End If
End Sub
