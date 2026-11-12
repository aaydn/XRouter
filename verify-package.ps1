# Verify installer payload of CURRENT version: decode base64 -> zip -> compare with llm_router.py
$ErrorActionPreference = 'Stop'
$src = Split-Path -Parent $MyInvocation.MyCommand.Path
$ver = ([regex]::Match((Get-Content (Join-Path $src 'llm_router.py') -Raw -Encoding UTF8),
        '__version__\s*=\s*"([\d.]+)"')).Groups[1].Value
$pkg = Join-Path $src ("dist\XRouter-川流-v$ver")
$zipFile = Join-Path $src ("dist\XRouter-川流-v$ver.zip")
Write-Host ("version: v{0}" -f $ver)

$names = (Get-ChildItem $pkg).Name | Sort-Object
Write-Host ("pkg entries ({0}): {1}" -f $names.Count, ($names -join ' | '))
if ($names.Count -ne 4) { throw "包内应为 3 个文件 + 1 个材料目录，实际 $($names.Count)" }
if (-not (Test-Path (Join-Path $pkg 'docs'))) { throw '缺少 docs 材料目录' }
$mats = (Get-ChildItem (Join-Path $pkg 'docs')).Name
Write-Host ("materials ({0}): {1}" -f $mats.Count, ($mats -join ' | '))
if ($mats.Count -lt 4) { throw '上架材料不足 4 项' }

$installer = Get-ChildItem $pkg -Filter '*.bat' | Sort-Object Length -Descending | Select-Object -First 1
Write-Host ("installer: {0} ({1:N0} KB)" -f $installer.Name, ($installer.Length / 1KB))

$t = [IO.File]::ReadAllText($installer.FullName, [Text.Encoding]::GetEncoding(936))
$i = $t.LastIndexOf('#PAYLOAD#')
if ($i -lt 0) { throw 'marker #PAYLOAD# not found' }
$b = ($t.Substring($i + 9)) -replace '[^A-Za-z0-9+/=]', ''
$zip = Join-Path $env:TEMP 'xr_verify.zip'
[IO.File]::WriteAllBytes($zip, [Convert]::FromBase64String($b))

$out = Join-Path $env:TEMP 'xr_verify'
Remove-Item $out -Recurse -Force -ErrorAction SilentlyContinue
Expand-Archive -LiteralPath $zip -DestinationPath $out -Force
Get-ChildItem $out | ForEach-Object { Write-Host ("  {0,-18} {1,8:N0} KB" -f $_.Name, ($_.Length / 1KB)) }

$h1 = (Get-FileHash (Join-Path $out 'llm_router.py')).Hash
$h2 = (Get-FileHash (Join-Path $src 'llm_router.py')).Hash
Write-Host ("hash match: {0}" -f ($h1 -eq $h2))
Write-Host ("zip size: {0:N0} KB" -f ((Get-Item $zipFile).Length / 1KB))
Remove-Item $zip, $out -Recurse -Force -ErrorAction SilentlyContinue
if ($h1 -ne $h2) { exit 1 }