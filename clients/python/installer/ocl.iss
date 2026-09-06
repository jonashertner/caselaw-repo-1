; OpenCaseLaw CLI, Windows installer script for Inno Setup 6.3 or newer.
;
; What it packages: the tree that installer/build_tree.py lays out, i.e. the
; python.org embeddable runtime exactly as the PSF ships it (python.exe and its
; DLLs are the only executables; they carry the PSF Authenticode signature), the
; pure-stdlib opencaselaw_cli package unzipped from its wheel, a python3XX._pth
; that pins what the runtime sees, and three launcher scripts (ocl.cmd,
; check-draft.cmd, pull-pack.cmd). No PyInstaller, no Nuitka, nothing compiled
; here.
;
; Compile (the workflow .github/workflows/installer-cli.yml does exactly this):
;   ISCC.exe /DAppVersion=0.8.0 /DSourceDir=C:\path\build\tree /OC:\path\dist ocl.iss
;
; Per-machine by default (admin, {autopf}\OpenCaseLaw); a per-user install lands
; in {localappdata}\OpenCaseLaw (/CURRENTUSER on the command line, or the dialog).
; Silent deployment: setup.exe /VERYSILENT /SUPPRESSMSGBOXES /NORESTART /LOG=...
; See docs/court-it-install.md for the allow-listing and rollout notes.

#ifndef AppVersion
  #define AppVersion "0.0.0"
#endif
#ifndef SourceDir
  #define SourceDir "..\..\..\build\tree"
#endif
#ifndef OutputDir
  #define OutputDir "..\..\..\dist"
#endif
#define AppName "OpenCaseLaw CLI"
#define AppPublisher "OpenCaseLaw"

[Setup]
AppId={{7B2E9C7A-3F4D-4C33-9B2B-2D0F8E6A5C11}
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL=https://opencaselaw.ch/cli/
AppSupportURL=https://github.com/jonashertner/opencaselaw/issues
AppUpdatesURL=https://github.com/jonashertner/opencaselaw/releases
DefaultDirName={code:GetDefaultDir}
DisableDirPage=no
DefaultGroupName=OpenCaseLaw
DisableProgramGroupPage=yes
PrivilegesRequired=admin
PrivilegesRequiredOverridesAllowed=dialog commandline
UsedUserAreasWarning=no
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0.17763
OutputDir={#OutputDir}
OutputBaseFilename=OpenCaseLaw-CLI-{#AppVersion}-setup
Compression=lzma2/max
SolidCompression=yes
WizardStyle=modern
LicenseFile={#SourceDir}\LICENSE-opencaselaw-cli.txt
UninstallDisplayName={#AppName}
UninstallDisplayIcon={app}\python.exe
SetupLogging=yes
ChangesEnvironment=yes

[Languages]
Name: "de"; MessagesFile: "compiler:Languages\German.isl"
Name: "en"; MessagesFile: "compiler:Default.isl"

[CustomMessages]
de.TaskSendTo=Verknüpfung "Entwurf prüfen (offline)" im Menü "Senden an" anlegen (nur für den installierenden Benutzer)
en.TaskSendTo=Add "Entwurf prüfen (offline)" to the "Send to" menu (installing user only)
de.TaskPath=Installationsordner zum PATH hinzufügen (ocl in jeder Eingabeaufforderung)
en.TaskPath=Add the installation folder to PATH (ocl from any prompt)
de.RunPull=Verifikationspaket jetzt laden (mehrere GB; Internet oder interner Spiegel nötig)
en.RunPull=Download the verification pack now (several GB; needs the internet or an internal mirror)
de.IconPull=Wöchentliches Verifikationspaket laden oder aktualisieren
en.IconPull=Download or refresh the weekly verification pack
de.IconCheck=Einen Entwurf offline gegen das Verifikationspaket prüfen
en.IconCheck=Check a draft offline against the verification pack

[Tasks]
Name: "sendto"; Description: "{cm:TaskSendTo}"
Name: "addtopath"; Description: "{cm:TaskPath}"; Flags: unchecked

[Files]
Source: "{#SourceDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\OpenCaseLaw - Verifikationspaket laden"; Filename: "{app}\pull-pack.cmd"; WorkingDir: "{app}"; Comment: "{cm:IconPull}"
Name: "{group}\OpenCaseLaw CLI deinstallieren"; Filename: "{uninstallexe}"
; The "Send to" entry is per user (there is no all-users SendTo folder). The same
; shortcut is also written to {app}\shortcuts as a template IT can copy into
; %APPDATA%\Microsoft\Windows\SendTo of every user (logon script or GPO).
Name: "{usersendto}\Entwurf prüfen (offline)"; Filename: "{app}\check-draft.cmd"; WorkingDir: "{app}"; Comment: "{cm:IconCheck}"; Tasks: sendto
Name: "{app}\shortcuts\Entwurf prüfen (offline)"; Filename: "{app}\check-draft.cmd"; WorkingDir: "{app}"; Comment: "{cm:IconCheck}"

[Registry]
Root: HKLM; Subkey: "SYSTEM\CurrentControlSet\Control\Session Manager\Environment"; ValueType: expandsz; ValueName: "Path"; ValueData: "{olddata};{app}"; Tasks: addtopath; Check: IsAdminInstallMode and NeedsAddPath('{app}')
Root: HKCU; Subkey: "Environment"; ValueType: expandsz; ValueName: "Path"; ValueData: "{olddata};{app}"; Tasks: addtopath; Check: (not IsAdminInstallMode) and NeedsAddPath('{app}')

[Run]
Filename: "{cmd}"; Parameters: "/C ""{app}\pull-pack.cmd"""; Description: "{cm:RunPull}"; Flags: postinstall nowait skipifsilent unchecked

[UninstallDelete]
Type: filesandordirs; Name: "{app}\Lib\site-packages\opencaselaw_cli\__pycache__"
Type: dirifempty; Name: "{app}\shortcuts"
Type: dirifempty; Name: "{app}\Lib\site-packages"
Type: dirifempty; Name: "{app}\Lib"
Type: dirifempty; Name: "{app}"

[Code]
function GetDefaultDir(Param: String): String;
begin
  if IsAdminInstallMode then
    Result := ExpandConstant('{autopf}\OpenCaseLaw')
  else
    Result := ExpandConstant('{localappdata}\OpenCaseLaw');
end;

procedure PathKey(var RootKey: Integer; var SubKey: String);
begin
  if IsAdminInstallMode then
  begin
    RootKey := HKEY_LOCAL_MACHINE;
    SubKey := 'SYSTEM\CurrentControlSet\Control\Session Manager\Environment';
  end
  else
  begin
    RootKey := HKEY_CURRENT_USER;
    SubKey := 'Environment';
  end;
end;

{ True when Dir is not yet on the PATH of the install mode's scope. }
function NeedsAddPath(Param: String): Boolean;
var
  RootKey: Integer;
  SubKey, Dir, Current: String;
begin
  Dir := ExpandConstant(Param);
  PathKey(RootKey, SubKey);
  if not RegQueryStringValue(RootKey, SubKey, 'Path', Current) then
  begin
    Result := True;
    exit;
  end;
  Result := Pos(';' + Uppercase(Dir) + ';', ';' + Uppercase(Current) + ';') = 0;
end;

{ Remove ";Dir" from the PATH written by the addtopath task; other entries stay as they are. }
procedure RemoveFromPath(Dir: String);
var
  RootKey, Index: Integer;
  SubKey, Current, Upper: String;
begin
  PathKey(RootKey, SubKey);
  if not RegQueryStringValue(RootKey, SubKey, 'Path', Current) then
    exit;
  Upper := ';' + Uppercase(Current) + ';';
  Index := Pos(';' + Uppercase(Dir) + ';', Upper);
  if Index = 0 then
    exit;
  Current := ';' + Current + ';';
  Delete(Current, Index, Length(Dir) + 1);
  if (Length(Current) > 0) and (Current[1] = ';') then
    Delete(Current, 1, 1);
  if (Length(Current) > 0) and (Current[Length(Current)] = ';') then
    Delete(Current, Length(Current), 1);
  RegWriteExpandStringValue(RootKey, SubKey, 'Path', Current);
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if CurUninstallStep = usPostUninstall then
    RemoveFromPath(ExpandConstant('{app}'));
end;
