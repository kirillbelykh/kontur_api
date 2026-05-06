Option Explicit

Dim fso, shell, projectDir, pythonw, python, mainScript, cmd
Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")

projectDir = fso.GetParentFolderName(WScript.ScriptFullName)
pythonw = fso.BuildPath(fso.BuildPath(fso.BuildPath(projectDir, ".venv"), "Scripts"), "pythonw.exe")
python = fso.BuildPath(fso.BuildPath(fso.BuildPath(projectDir, ".venv"), "Scripts"), "python.exe")
mainScript = fso.BuildPath(projectDir, "kontur_access_prolongation.py")

If fso.FileExists(pythonw) Then
    cmd = """" & pythonw & """ """ & mainScript & """"
    shell.Run cmd, 0, False
ElseIf fso.FileExists(python) Then
    cmd = """" & python & """ """ & mainScript & """"
    shell.Run cmd, 0, False
Else
    MsgBox "Не найдена установленная среда KonturAccessProlongation (.venv). Запустите setup.bat заново.", vbExclamation, "Kontur Access Prolongation"
End If
