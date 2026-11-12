# XRouter·川流 打包脚本
# 产出：dist/XRouter-川流-v<版本>/  （一级目录只含：双击安装.bat、双击卸载.bat、使用说明.txt）
#       dist/XRouter-川流-v<版本>.zip（UTF-8 文件名，解压不乱码）
$ErrorActionPreference = 'Stop'
$src = Split-Path -Parent $MyInvocation.MyCommand.Path
$py  = Join-Path $src 'llm_router.py'

$ver = ([regex]::Match((Get-Content $py -Raw -Encoding UTF8), '__version__\s*=\s*"([\d.]+)"')).Groups[1].Value
if (-not $ver) { throw '未找到 __version__' }
$name = "XRouter-川流-v$ver"
$dist = Join-Path $src 'dist'
$pkg  = Join-Path $dist $name
Write-Host "[1/5] 版本 v$ver"

Remove-Item $pkg -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Path $pkg -Force | Out-Null
Add-Type -AssemblyName System.IO.Compression.FileSystem

# --- 载荷：程序本体 + 启动器 + 依赖清单 + 许可证，打成 zip 再 base64 嵌入安装器 ---
Write-Host '[2/5] 生成载荷 ...'
$stage = Join-Path $dist '_stage'
Remove-Item $stage -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Path $stage -Force | Out-Null
Copy-Item $py (Join-Path $stage 'llm_router.py')
Copy-Item (Join-Path $src 'requirements.txt') $stage
Copy-Item (Join-Path $src 'pack\launcher.bat') $stage
Copy-Item (Join-Path $src 'LICENSE') $stage
$payload = Join-Path $dist '_payload.zip'
Remove-Item $payload -Force -ErrorAction SilentlyContinue
[IO.Compression.ZipFile]::CreateFromDirectory($stage, $payload,
    [IO.Compression.CompressionLevel]::Optimal, $false)

# --- 安装器 = 头部脚本 + 标记 + base64 ---
Write-Host '[3/5] 生成安装程序 ...'
$b64 = [Convert]::ToBase64String([IO.File]::ReadAllBytes($payload))
$sb = New-Object System.Text.StringBuilder
for ($i = 0; $i -lt $b64.Length; $i += 76) {
    [void]$sb.AppendLine($b64.Substring($i, [Math]::Min(76, $b64.Length - $i)))
}
$head = [IO.File]::ReadAllText((Join-Path $src 'pack\install-header.bat'), [Text.Encoding]::UTF8)
$head = ($head -replace "`r`n", "`n") -replace "`n", "`r`n"
$body = $head.TrimEnd() + "`r`n`r`n#PAYLOAD#`r`n" + $sb.ToString()
[IO.File]::WriteAllText((Join-Path $pkg '双击安装.bat'), $body, [Text.Encoding]::GetEncoding(936))

# --- 卸载程序 + 使用说明（统一转 GBK，中文路径不乱码） ---
Write-Host '[4/5] 生成卸载程序与说明 ...'
$u = [IO.File]::ReadAllText((Join-Path $src 'pack\双击卸载.bat'), [Text.Encoding]::UTF8)
$u = ($u -replace "`r`n", "`n") -replace "`n", "`r`n"
[IO.File]::WriteAllText((Join-Path $pkg '双击卸载.bat'), $u, [Text.Encoding]::GetEncoding(936))
Copy-Item (Join-Path $src 'docs\user-guide.txt') (Join-Path $pkg '使用说明.txt')

# --- 文档材料（需求文档/版本记录/安全自查表/使用说明/更新日志，以 docs 子目录随包附带，一级目录保持只有三个入口文件） ---
Write-Host '      附带文档材料（子目录） ...'
$mat = Join-Path $pkg 'docs'
New-Item -ItemType Directory -Path $mat -Force | Out-Null
Copy-Item (Join-Path $src 'docs\requirements.md') $mat
Copy-Item (Join-Path $src 'docs\version-history.md') $mat
Copy-Item (Join-Path $src 'docs\security-checklist.md') $mat
Copy-Item (Join-Path $src 'docs\user-guide.txt') $mat
Copy-Item (Join-Path $src 'CHANGELOG.md') $mat

# --- 压缩包 ---
Write-Host '[5/5] 生成 zip ...'
$zip = Join-Path $dist "$name.zip"
Remove-Item $zip -Force -ErrorAction SilentlyContinue
[IO.Compression.ZipFile]::CreateFromDirectory($pkg, $zip,
    [IO.Compression.CompressionLevel]::Optimal, $true, [Text.Encoding]::UTF8)

Remove-Item $stage -Recurse -Force
Remove-Item $payload -Force

Get-ChildItem $pkg | ForEach-Object { Write-Host ("  {0,-16} {1,10:N0} KB" -f $_.Name, ($_.Length / 1KB)) }
Write-Host ("PKG: {0}" -f $pkg)
Write-Host ("ZIP: {0}  ({1:N0} KB)" -f $zip, ((Get-Item $zip).Length / 1KB))