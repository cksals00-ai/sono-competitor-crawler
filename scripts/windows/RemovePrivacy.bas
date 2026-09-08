Attribute VB_Name = "RemovePrivacy"
' ============================================================
'  Remove 3 privacy columns (Bigo / Handphone / Reserver-tel)
'  from *reservation-list*.xlsx in THIS workbook's folder,
'  save cleaned copies into the "cleaned" subfolder (originals kept).
'  Korean text is built with ChrW() so the source is pure ASCII
'  and never breaks on import/encoding.
'  Run:  Alt+F8 -> RemovePrivacy_Run -> Run   (or assign to a button)
' ============================================================
Option Explicit

Sub RemovePrivacy_Run()
    ' --- Korean strings via Unicode (encoding-proof) ---
    Dim COL_STATUS As String, COL_BIGO As String, COL_PHONE As String, COL_TEL As String
    Dim KW_RES As String, KW_FORM As String, KW_PLAN1 As String, KW_PLAN2 As String, OUT_DIR As String
    COL_STATUS = ChrW(&HC0C1) & ChrW(&HD0DC)                                             ' sangtae
    COL_BIGO = ChrW(&HBE44) & ChrW(&HACE0)                                               ' bigo
    COL_PHONE = ChrW(&HD578) & ChrW(&HB4DC) & ChrW(&HD3F0)                               ' handphone
    COL_TEL = ChrW(&HC608) & ChrW(&HC57D) & ChrW(&HC790) & ChrW(&HC804) & ChrW(&HD654) & ChrW(&HBC88) & ChrW(&HD638) ' reserver-tel
    KW_RES = ChrW(&HC608) & ChrW(&HC57D) & ChrW(&HC815) & ChrW(&HBCF4) & ChrW(&HC870) & ChrW(&HD68C) ' reservation-list
    KW_FORM = ChrW(&HC591) & ChrW(&HC2DD)                                                ' template
    KW_PLAN1 = ChrW(&HC0AC) & ChrW(&HC5C5) & ChrW(&HACC4) & ChrW(&HD68D)                 ' biz-plan
    KW_PLAN2 = ChrW(&HAC1D) & ChrW(&HC2E4) & ChrW(&HACC4) & ChrW(&HD68D)                 ' room-plan
    OUT_DIR = ChrW(&HC815) & ChrW(&HB9AC) & ChrW(&HC644) & ChrW(&HB8CC)                  ' cleaned

    Dim srcFolder As String, outFolder As String
    srcFolder = ThisWorkbook.Path
    If srcFolder = "" Then
        MsgBox "Save this file as .xlsm into your export folder first.", vbExclamation
        Exit Sub
    End If
    outFolder = srcFolder & "\" & OUT_DIR
    If Dir(outFolder, vbDirectory) = "" Then MkDir outFolder

    ' 1) collect target file names first (avoid Dir re-entry)
    Dim names() As String, n As Long, fn As String
    ReDim names(1 To 2000)
    fn = Dir(srcFolder & "\*" & KW_RES & "*.xlsx")
    Do While fn <> ""
        If InStr(fn, KW_FORM) = 0 And InStr(fn, KW_PLAN1) = 0 _
           And InStr(fn, KW_PLAN2) = 0 And Left(fn, 2) <> "~$" Then
            n = n + 1: names(n) = fn
        End If
        fn = Dir()
    Loop
    If n = 0 Then
        MsgBox "No reservation-list files in this folder:" & vbCrLf & srcFolder, vbInformation
        Exit Sub
    End If

    ' 2) process
    Dim targets As Variant
    targets = Array(COL_BIGO, COL_PHONE, COL_TEL)
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
                    If Trim(CStr(ws.Cells(r, c).Value)) = COL_STATUS Then hdrRow = r: Exit For
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
        wb.SaveAs Filename:=outFolder & "\" & names(i), FileFormat:=xlOpenXMLWorkbook
        wb.Close SaveChanges:=False
        cnt = cnt + 1: totCols = totCols + removed
    Next i

cleanup:
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
    If Err.Number <> 0 Then
        MsgBox "Error: " & Err.Description & vbCrLf & "(processed " & cnt & " files)", vbCritical
    Else
        MsgBox "Done: " & cnt & " files, " & totCols & " columns removed." & vbCrLf & _
               "Saved into: " & outFolder, vbInformation, "OK"
    End If
End Sub
