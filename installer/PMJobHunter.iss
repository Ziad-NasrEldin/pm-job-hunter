#define MyAppName "PM Job Hunter"
#define MyAppExeName "PMJobHunter.exe"
#ifndef AppVersion
  #define AppVersion "0.0.0"
#endif
#ifndef RepoRoot
  #define RepoRoot ".."
#endif

[Setup]
AppId={{5C5C14A1-11D2-40EF-A113-FCF4F2ACF7EF}
AppName={#MyAppName}
AppVersion={#AppVersion}
DefaultDirName={autopf}\PM Job Hunter
DefaultGroupName=PM Job Hunter
OutputDir={#RepoRoot}\dist
OutputBaseFilename=PMJobHunter-Setup
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
PrivilegesRequired=admin
Compression=lzma
SolidCompression=yes
WizardStyle=modern
UninstallDisplayIcon={app}\{#MyAppExeName}

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; GroupDescription: "Additional shortcuts:"

[Files]
Source: "{#RepoRoot}\dist\PMJobHunter.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#RepoRoot}\.env.local.example"; DestDir: "{localappdata}\PMJobHunter"; DestName: ".env.local.example"; Flags: ignoreversion
Source: "{#RepoRoot}\installer\FIRST_RUN.txt"; DestDir: "{localappdata}\PMJobHunter"; Flags: ignoreversion
Source: "{#RepoRoot}\build\ms-playwright\*"; DestDir: "{localappdata}\PMJobHunter\ms-playwright"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\PM Job Hunter"; Filename: "{app}\{#MyAppExeName}"
Name: "{autodesktop}\PM Job Hunter"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Launch PM Job Hunter"; Flags: nowait postinstall skipifsilent
