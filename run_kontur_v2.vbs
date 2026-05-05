Option Explicit

Dim fso, shell, projectDir, pythonw, python, mainScript, cmd
Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")

projectDir = fso.GetParentFolderName(WScript.ScriptFullName)
pythonw = fso.BuildPath(fso.BuildPath(fso.BuildPath(projectDir, ".venv"), "Scripts"), "pythonw.exe")
python = fso.BuildPath(fso.BuildPath(fso.BuildPath(projectDir, ".venv"), "Scripts"), "python.exe")
mainScript = fso.BuildPath(fso.BuildPath(projectDir, "ui_v2"), "main_v2.py")

If fso.FileExists(pythonw) Then
    cmd = """" & pythonw & """ """ & mainScript & """"
    shell.Run cmd, 0, False
ElseIf fso.FileExists(python) Then
    cmd = """" & python & """ """ & mainScript & """"
    shell.Run cmd, 0, False
Else
    MsgBox "Не найдена установленная среда KonturTestAPI (.venv). Запустите setup.bat заново.", vbExclamation, "KonturTestAPI"
End If
