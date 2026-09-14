# Installs the latest mmysql release (or $env:MMYSQL_VERSION, e.g. v1.2.0).
#   irm https://raw.githubusercontent.com/mitchpaulus/mmysql/main/install.ps1 | iex
# Set $env:MMYSQL_INSTALL_DIR to change the destination
# (default: %LOCALAPPDATA%\Programs\mmysql). The directory is added to the user PATH.
$ErrorActionPreference = "Stop"

$repo = "mitchpaulus/mmysql"
$installDir = if ($env:MMYSQL_INSTALL_DIR) { $env:MMYSQL_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA "Programs\mmysql" }
$version = $env:MMYSQL_VERSION

$arch = switch ($env:PROCESSOR_ARCHITECTURE) {
    "AMD64" { "amd64" }
    "ARM64" { "arm64" }
    default { throw "Unsupported architecture: $env:PROCESSOR_ARCHITECTURE" }
}

$asset = "mmysql-windows-$arch.exe"
$url = if ($version) {
    "https://github.com/$repo/releases/download/$version/$asset"
} else {
    "https://github.com/$repo/releases/latest/download/$asset"
}

New-Item -ItemType Directory -Force -Path $installDir | Out-Null
$dest = Join-Path $installDir "mmysql.exe"

Write-Host "Downloading $url"
Invoke-WebRequest -Uri $url -OutFile $dest -UseBasicParsing

$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if (($userPath -split ";") -notcontains $installDir) {
    [Environment]::SetEnvironmentVariable("Path", "$userPath;$installDir", "User")
    $env:Path = "$env:Path;$installDir"
    Write-Host "Added $installDir to your user PATH. Restart your terminal to pick it up."
}

Write-Host "Installed mmysql $(& $dest version) to $dest"
