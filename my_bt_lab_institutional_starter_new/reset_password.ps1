# PostgreSQL 密码重置脚本
# 请以管理员身份运行 PowerShell，然后执行此脚本

Write-Host "==========================================" -ForegroundColor Green
Write-Host "PostgreSQL 密码重置工具" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""

# 检查是否以管理员身份运行
$currentPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "错误：请以管理员身份运行 PowerShell！" -ForegroundColor Red
    Write-Host "请右键点击 PowerShell，选择'以管理员身份运行'" -ForegroundColor Yellow
    exit 1
}

Write-Host "[1/6] 停止 PostgreSQL 服务..." -ForegroundColor Cyan
Stop-Service postgresql-x64-16 -Force
Start-Sleep 2

Write-Host ""
Write-Host "[2/6] 备份 pg_hba.conf 文件..." -ForegroundColor Cyan
$pgHbaPath = "C:\Program Files\PostgreSQL\16\data\pg_hba.conf"
$backupPath = "C:\Program Files\PostgreSQL\16\data\pg_hba.conf.backup"
Copy-Item $pgHbaPath $backupPath -Force
Write-Host "备份完成: $backupPath" -ForegroundColor Green

Write-Host ""
Write-Host "[3/6] 修改认证方式为 trust..." -ForegroundColor Cyan
$content = Get-Content $pgHbaPath
$content = $content -replace 'scram-sha-256', 'trust'
$content | Set-Content $pgHbaPath
Write-Host "认证方式已修改为 trust" -ForegroundColor Green

Write-Host ""
Write-Host "[4/6] 启动 PostgreSQL 服务..." -ForegroundColor Cyan
Start-Service postgresql-x64-16
Start-Sleep 3

Write-Host ""
Write-Host "[5/6] 重置 postgres 用户密码..." -ForegroundColor Cyan
$env:PGPASSWORD = ""
& "C:\Program Files\PostgreSQL\16\bin\psql.exe" -U postgres -c "ALTER USER postgres WITH PASSWORD 'postgres';"
if ($LASTEXITCODE -eq 0) {
    Write-Host "密码重置成功！新密码: postgres" -ForegroundColor Green
} else {
    Write-Host "密码重置失败，尝试其他方法..." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "[6/6] 恢复安全配置..." -ForegroundColor Cyan
Copy-Item $backupPath $pgHbaPath -Force
Write-Host "安全配置已恢复" -ForegroundColor Green

Write-Host ""
Write-Host "重启 PostgreSQL 服务..." -ForegroundColor Cyan
Restart-Service postgresql-x64-16
Start-Sleep 2

Write-Host ""
Write-Host "==========================================" -ForegroundColor Green
Write-Host "密码重置完成！" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""
Write-Host "连接信息:" -ForegroundColor Yellow
Write-Host "  用户名: postgres" -ForegroundColor White
Write-Host "  密码: postgres" -ForegroundColor White
Write-Host "  主机: localhost" -ForegroundColor White
Write-Host "  端口: 5432" -ForegroundColor White
Write-Host ""
Write-Host "测试连接命令:" -ForegroundColor Yellow
Write-Host '  $env:PGPASSWORD="postgres"; & "C:\Program Files\PostgreSQL\16\bin\psql.exe" -h localhost -p 5432 -U postgres -c "\l"' -ForegroundColor Cyan
Write-Host ""

# 测试连接
Write-Host "正在测试连接..." -ForegroundColor Cyan
$env:PGPASSWORD = "postgres"
& "C:\Program Files\PostgreSQL\16\bin\psql.exe" -h localhost -p 5432 -U postgres -c "SELECT '连接成功!' as status;"
