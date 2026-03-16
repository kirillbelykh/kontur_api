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
    ' Fallback for missing .venv: try uv runtime from PATH.
    cmd = "cmd /c cd /d """ & projectDir & """ && uv run --python 3.12 main.pyw"
    shell.Run cmd, 0, False
End If
