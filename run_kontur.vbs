Option Explicit

Dim fso, shell, projectDir, pythonw, mainScript, cmd
Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")

projectDir = fso.GetParentFolderName(WScript.ScriptFullName)
pythonw = fso.BuildPath(fso.BuildPath(fso.BuildPath(projectDir, ".venv"), "Scripts"), "pythonw.exe")
mainScript = fso.BuildPath(projectDir, "main.pyw")

If fso.FileExists(pythonw) Then
    cmd = """" & pythonw & """ """ & mainScript & """"
    shell.Run cmd, 0, False
Else
    MsgBox "Не найдена установленная среда KonturAPI (.venv). Запустите setup.bat заново.", vbExclamation, "KonturAPI"
End If
