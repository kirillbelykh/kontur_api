; Inno Setup script for Kontur Markirovka
; Build: scripts\build_installer.ps1 (downloads ISCC if needed)

#define MyAppName "Контур Маркировка"
#define MyAppNameAscii "KonturMarkirovka"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "Grundlage"
#define MyAppURL "https://github.com/kirillbelykh/kontur_api"
#define MyAppExeName "KonturMarkirovka.bat"

[Setup]
AppId={{8F2A1C9E-4B67-4D91-9E2A-3C8F5B1D0A77}}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
DefaultDirName={localappdata}\Programs\{#MyAppNameAscii}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
PrivilegesRequired=lowest
OutputDir=..\dist\installer
OutputBaseFilename=KonturMarkirovka-Setup
SetupIconFile=..\assets\icons\icon.ico
UninstallDisplayIcon={app}\assets\icons\icon.ico
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
ArchitecturesInstallIn64BitMode=x64compatible
CloseApplications=force
RestartApplications=no

[Languages]
Name: "russian"; MessagesFile: "compiler:Languages\Russian.isl"
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Создать ярлык на рабочем столе"; GroupDescription: "Дополнительно:"

[Files]
; Payload prepared by build_installer.ps1 into installer\payload
Source: "payload\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; IconFilename: "{app}\assets\icons\icon.ico"; WorkingDir: "{app}"; Tasks: desktopicon
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; IconFilename: "{app}\assets\icons\icon.ico"; WorkingDir: "{app}"
Name: "{group}\Удалить {#MyAppName}"; Filename: "{uninstallexe}"

[Run]
Filename: "powershell.exe"; Parameters: "-NoProfile -ExecutionPolicy Bypass -File ""{app}\scripts\post_install.ps1"" -ProjectDir ""{app}"""; StatusMsg: "Настройка окружения Python и драйвера..."; Flags: runhidden waituntilterminated
Filename: "{app}\{#MyAppExeName}"; Description: "Запустить Контур Маркировка"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: filesandordirs; Name: "{app}\.venv"
Type: filesandordirs; Name: "{app}\runtime"
Type: filesandordirs; Name: "{app}\frontend\node_modules"
Type: files; Name: "{userdesktop}\Контур Маркировка.lnk"
Type: files; Name: "{userdesktop}\KonturAPI.lnk"
Type: files; Name: "{userdesktop}\KonturTestAPI.lnk"
Type: files; Name: "{userdesktop}\KonturMobile.lnk"
Type: files; Name: "{userdesktop}\CRPT server.lnk"






