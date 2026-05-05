Option Explicit

Dim fso, shell, projectDir, pythonw, python, mobileScript, cmd
Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")

projectDir = fso.GetParentFolderName(WScript.ScriptFullName)
pythonw = fso.BuildPath(fso.BuildPath(fso.BuildPath(projectDir, ".venv"), "Scripts"), "pythonw.exe")
python = fso.BuildPath(fso.BuildPath(fso.BuildPath(projectDir, ".venv"), "Scripts"), "python.exe")
mobileScript = fso.BuildPath(fso.BuildPath(projectDir, "ui_mobile"), "server_mobile.py")

If fso.FileExists(pythonw) Then
    cmd = """" & pythonw & """ """ & mobileScript & """ --host 0.0.0.0 --port 8787 --https-port 8788"
    shell.Run cmd, 0, False
ElseIf fso.FileExists(python) Then
    cmd = """" & python & """ """ & mobileScript & """ --host 0.0.0.0 --port 8787 --https-port 8788"
    shell.Run cmd, 0, False
Else
    MsgBox "Не найдена установленная среда KonturMobile (.venv). Запустите setup.bat заново.", vbExclamation, "Kontur Mobile"
End If
