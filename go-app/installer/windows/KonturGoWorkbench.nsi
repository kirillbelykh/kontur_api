Unicode True
RequestExecutionLevel admin

!include "MUI2.nsh"
!include "LogicLib.nsh"
!include "x64.nsh"
!include "FileFunc.nsh"

!ifndef APP_NAME
  !define APP_NAME "Kontur Go Workbench"
!endif

!ifndef APP_VERSION
  !define APP_VERSION "0.1.0"
!endif

!ifndef APP_EXE_NAME
  !define APP_EXE_NAME "KonturGoWorkbench.exe"
!endif

!ifndef APP_PUBLISHER
  !define APP_PUBLISHER "kirillbelykh"
!endif

!ifndef INSTALL_DIR_NAME
  !define INSTALL_DIR_NAME "KonturGoWorkbench"
!endif

!ifndef SOURCE_DIR
  !error "SOURCE_DIR must point to the staged application package directory"
!endif

!ifndef OUTPUT_DIR
  !error "OUTPUT_DIR must point to the target installer directory"
!endif

!define PRODUCT_UNINST_KEY "Software\Microsoft\Windows\CurrentVersion\Uninstall\${INSTALL_DIR_NAME}"
!define WEBVIEW2_CLIENT_GUID "{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}"

Name "${APP_NAME}"
OutFile "${OUTPUT_DIR}\${INSTALL_DIR_NAME}-Setup-${APP_VERSION}.exe"
InstallDir "$PROGRAMFILES64\${APP_NAME}"
InstallDirRegKey HKLM "${PRODUCT_UNINST_KEY}" "InstallLocation"

!define MUI_ABORTWARNING
!define MUI_ICON "..\..\build\windows\icon.ico"
!define MUI_UNICON "..\..\build\windows\icon.ico"
!define MUI_FINISHPAGE_RUN "$INSTDIR\${APP_EXE_NAME}"
!define MUI_FINISHPAGE_RUN_TEXT "Запустить ${APP_NAME}"

!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES
!insertmacro MUI_UNPAGE_FINISH

!insertmacro MUI_LANGUAGE "Russian"

Var WebView2Version

Function .onInit
  ${IfNot} ${RunningX64}
    MessageBox MB_ICONSTOP "Этот установщик поддерживает только 64-битную Windows."
    Abort
  ${EndIf}
FunctionEnd

Function EnsureWebView2Runtime
  StrCpy $WebView2Version ""
  SetRegView 64
  ReadRegStr $WebView2Version HKLM "SOFTWARE\Microsoft\EdgeUpdate\Clients\${WEBVIEW2_CLIENT_GUID}" "pv"
  ${If} $WebView2Version == ""
    ReadRegStr $WebView2Version HKCU "SOFTWARE\Microsoft\EdgeUpdate\Clients\${WEBVIEW2_CLIENT_GUID}" "pv"
  ${EndIf}
  SetRegView lastused

  ${If} $WebView2Version == ""
    DetailPrint "Microsoft Edge WebView2 Runtime не найден. Запускаем тихую установку..."
    !ifdef WEBVIEW2_BOOTSTRAPPER
      InitPluginsDir
      File "/oname=$PLUGINSDIR\MicrosoftEdgeWebview2Setup.exe" "${WEBVIEW2_BOOTSTRAPPER}"
      ExecWait '"$PLUGINSDIR\MicrosoftEdgeWebview2Setup.exe" /silent /install' $0
      ${If} $0 != 0
        MessageBox MB_ICONSTOP|MB_OK "Не удалось установить Microsoft Edge WebView2 Runtime. Код: $0"
        Abort
      ${EndIf}
    !else
      MessageBox MB_ICONSTOP|MB_OK "В installer не был встроен Microsoft Edge WebView2 Runtime bootstrapper. Установка будет прервана."
      Abort
    !endif
  ${Else}
    DetailPrint "Microsoft Edge WebView2 Runtime уже установлен: $WebView2Version"
  ${EndIf}
FunctionEnd

Section "Install"
  SetShellVarContext all

  Call EnsureWebView2Runtime

  SetOutPath "$INSTDIR"
  File /r "${SOURCE_DIR}\*.*"
  WriteUninstaller "$INSTDIR\Uninstall.exe"

  CreateDirectory "$SMPROGRAMS\${APP_NAME}"
  CreateShortcut "$SMPROGRAMS\${APP_NAME}\${APP_NAME}.lnk" "$INSTDIR\${APP_EXE_NAME}"
  CreateShortcut "$SMPROGRAMS\${APP_NAME}\Удалить ${APP_NAME}.lnk" "$INSTDIR\Uninstall.exe"
  CreateShortcut "$DESKTOP\${APP_NAME}.lnk" "$INSTDIR\${APP_EXE_NAME}"

  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "DisplayName" "${APP_NAME}"
  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "DisplayVersion" "${APP_VERSION}"
  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "Publisher" "${APP_PUBLISHER}"
  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "InstallLocation" "$INSTDIR"
  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "DisplayIcon" "$INSTDIR\${APP_EXE_NAME}"
  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "UninstallString" "$INSTDIR\Uninstall.exe"
  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "QuietUninstallString" "$INSTDIR\Uninstall.exe /S"
  WriteRegDWORD HKLM "${PRODUCT_UNINST_KEY}" "NoModify" 1
  WriteRegDWORD HKLM "${PRODUCT_UNINST_KEY}" "NoRepair" 1

  ${GetSize} "$INSTDIR" "/S=0K" $0 $1 $2
  IntFmt $0 "0x%08X" $0
  WriteRegDWORD HKLM "${PRODUCT_UNINST_KEY}" "EstimatedSize" "$0"
SectionEnd

Section "Uninstall"
  SetShellVarContext all

  Delete "$DESKTOP\${APP_NAME}.lnk"
  Delete "$SMPROGRAMS\${APP_NAME}\${APP_NAME}.lnk"
  Delete "$SMPROGRAMS\${APP_NAME}\Удалить ${APP_NAME}.lnk"
  RMDir "$SMPROGRAMS\${APP_NAME}"

  Delete "$INSTDIR\Uninstall.exe"
  RMDir /r "$INSTDIR"

  DeleteRegKey HKLM "${PRODUCT_UNINST_KEY}"
SectionEnd
