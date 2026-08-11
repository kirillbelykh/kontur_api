' Thin stub ? real launcher lives in scripts\launchers\
Option Explicit
Dim fso, sh, here, target
Set fso = CreateObject("Scripting.FileSystemObject")
Set sh = CreateObject("WScript.Shell")
here = fso.GetParentFolderName(WScript.ScriptFullName)
target = here & "\scripts\launchers\run_crpt_server.vbs"
If fso.FileExists(target) Then
  sh.Run "wscript.exe //nologo """ & target & """", 0, False
Else
  MsgBox "Missing launcher: " & target, vbExclamation, "CRPT server"
End If
