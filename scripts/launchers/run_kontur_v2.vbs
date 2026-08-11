' Alias launcher ? same as run_kontur.vbs (repo root is two levels up from scripts/launchers)
Option Explicit
Dim fso, sh, repo, pythonw
Set fso = CreateObject("Scripting.FileSystemObject")
Set sh = CreateObject("WScript.Shell")
repo = fso.GetParentFolderName(fso.GetParentFolderName(fso.GetParentFolderName(WScript.ScriptFullName)))
pythonw = repo & "\.venv\Scripts\pythonw.exe"
If Not fso.FileExists(pythonw) Then
  pythonw = repo & "\.venv\Scripts\python.exe"
End If
If Not fso.FileExists(pythonw) Then
  pythonw = "pythonw.exe"
End If
sh.CurrentDirectory = repo
sh.Run """" & pythonw & """ """ & repo & "\main.py""", 0, False
