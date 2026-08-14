Option Explicit

Dim fso, shell, projectDir, pythonw, python, mainScript, cmd
Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")

' scripts/launchers -> repo root
projectDir = fso.GetParentFolderName(fso.GetParentFolderName(fso.GetParentFolderName(WScript.ScriptFullName)))
pythonw = fso.BuildPath(fso.BuildPath(fso.BuildPath(projectDir, ".venv"), "Scripts"), "pythonw.exe")
python = fso.BuildPath(fso.BuildPath(fso.BuildPath(projectDir, ".venv"), "Scripts"), "python.exe")
mainScript = fso.BuildPath(fso.BuildPath(fso.BuildPath(projectDir, "backend"), "app"), "server_only.py")

If fso.FileExists(pythonw) Then
    cmd = """" & pythonw & """ """ & mainScript & """"
    shell.Run cmd, 0, False
ElseIf fso.FileExists(python) Then
    cmd = """" & python & """ """ & mainScript & """"
    shell.Run cmd, 0, False
Else
    MsgBox "Python venv not found for Kontur API (.venv). Run Install.bat first.", vbExclamation, "CRPT server"
End If
