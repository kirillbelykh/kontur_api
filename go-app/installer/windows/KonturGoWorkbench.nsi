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
  !error "SOURCE_DIR must point to the Wails build/bin directory"
!endif

!ifndef OUTPUT_DIR
  !error "OUTPUT_DIR must point to the target installer directory"
!endif

!define PRODUCT_UNINST_KEY "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\${INSTALL_DIR_NAME}"

Name "${APP_NAME}"
OutFile "${OUTPUT_DIR}\\${INSTALL_DIR_NAME}-Setup-${APP_VERSION}.exe"
InstallDir "$PROGRAMFILES64\\${APP_NAME}"
InstallDirRegKey HKLM "${PRODUCT_UNINST_KEY}" "InstallLocation"

!define MUI_ABORTWARNING
!define MUI_ICON "..\\..\\build\\windows\\icon.ico"
!define MUI_UNICON "..\\..\\build\\windows\\icon.ico"

!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES
!insertmacro MUI_UNPAGE_FINISH

!insertmacro MUI_LANGUAGE "Russian"

Function .onInit
  ${IfNot} ${RunningX64}
    MessageBox MB_ICONSTOP "Этот установщик поддерживает только 64-битную Windows."
    Abort
  ${EndIf}
FunctionEnd

Section "Install"
  SetShellVarContext all
  SetOutPath "$INSTDIR"

  File /r "${SOURCE_DIR}\\*.*"
  WriteUninstaller "$INSTDIR\\Uninstall.exe"

  CreateDirectory "$SMPROGRAMS\\${APP_NAME}"
  CreateShortcut "$SMPROGRAMS\\${APP_NAME}\\${APP_NAME}.lnk" "$INSTDIR\\${APP_EXE_NAME}"
  CreateShortcut "$SMPROGRAMS\\${APP_NAME}\\Удалить ${APP_NAME}.lnk" "$INSTDIR\\Uninstall.exe"
  CreateShortcut "$DESKTOP\\${APP_NAME}.lnk" "$INSTDIR\\${APP_EXE_NAME}"

  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "DisplayName" "${APP_NAME}"
  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "DisplayVersion" "${APP_VERSION}"
  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "Publisher" "${APP_PUBLISHER}"
  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "InstallLocation" "$INSTDIR"
  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "DisplayIcon" "$INSTDIR\\${APP_EXE_NAME}"
  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "UninstallString" "$INSTDIR\\Uninstall.exe"
  WriteRegStr HKLM "${PRODUCT_UNINST_KEY}" "QuietUninstallString" "$INSTDIR\\Uninstall.exe /S"
  WriteRegDWORD HKLM "${PRODUCT_UNINST_KEY}" "NoModify" 1
  WriteRegDWORD HKLM "${PRODUCT_UNINST_KEY}" "NoRepair" 1

  ${GetSize} "$INSTDIR" "/S=0K" $0 $1 $2
  IntFmt $0 "0x%08X" $0
  WriteRegDWORD HKLM "${PRODUCT_UNINST_KEY}" "EstimatedSize" "$0"
SectionEnd

Section "Uninstall"
  SetShellVarContext all

  Delete "$DESKTOP\\${APP_NAME}.lnk"
  Delete "$SMPROGRAMS\\${APP_NAME}\\${APP_NAME}.lnk"
  Delete "$SMPROGRAMS\\${APP_NAME}\\Удалить ${APP_NAME}.lnk"
  RMDir "$SMPROGRAMS\\${APP_NAME}"

  Delete "$INSTDIR\\Uninstall.exe"
  RMDir /r "$INSTDIR"

  DeleteRegKey HKLM "${PRODUCT_UNINST_KEY}"
SectionEnd
