' Alias launcher — same as run_kontur.vbs
Set fso = CreateObject("Scripting.FileSystemObject")
Set sh = CreateObject("WScript.Shell")
repo = fso.GetParentFolderName(WScript.ScriptFullName)
pythonw = repo & "\.venv\Scripts\pythonw.exe"
If Not fso.FileExists(pythonw) Then
  pythonw = "pythonw.exe"
End If
sh.CurrentDirectory = repo
sh.Run """" & pythonw & """ """ & repo & "\main.py""", 0, False
