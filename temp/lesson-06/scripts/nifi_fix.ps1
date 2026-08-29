<#
.SYNOPSIS
    Исправляет конфиг ConsumeKafka и запускает процессоры NiFi.
    Все UUID резолвятся по именам компонентов через NiFi API.
#>
$ErrorActionPreference = "Stop"
. "$PSScriptRoot/nifi_common.ps1"

Connect-NiFi

$PG  = Get-ProcessGroupIdByName   $script:DefaultNames.ProcessGroup
$SSL = Get-ControllerServiceIdByName -ProcessGroupId $PG -Name $script:DefaultNames.SslService
$CK  = Get-ProcessorIdByName      -ProcessGroupId $PG -Name $script:DefaultNames.ConsumeKafka
$LA  = Get-ProcessorIdByName      -ProcessGroupId $PG -Name $script:DefaultNames.LogAttribute
$PF  = Get-ProcessorIdByName      -ProcessGroupId $PG -Name $script:DefaultNames.PutFile

# 1) включаем SSL-сервис (повторное включение безопасно)
Write-Host "=== ENABLE SSL ==="
$sslState = (Invoke-NiFiApi GET "controller-services/$SSL").component.state
Write-Host "SSL_STATE=$sslState"
if ($sslState -ne "ENABLED") {
    $s = Enable-ControllerService -Id $SSL
    Write-Host "SSL_STATE_AFTER=$s"
    Start-Sleep -Seconds 2
}

# 2) корректные свойства ConsumeKafka (конфиг менять можно только на остановленном процессоре)
Write-Host "=== PUT ConsumeKafka config ==="
Stop-Processor -Id $CK | Out-Null
Start-Sleep -Seconds 1
$props = @{
    "bootstrap.servers"   = "rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net:9091"
    "topic"               = "topic-1"
    "group.id"            = "nifi-consumer-group"
    "security.protocol"   = "SASL_SSL"
    "sasl.mechanism"      = "SCRAM-SHA-512"
    "sasl.username"       = "practicumuser"
    "sasl.password"       = "SecurePass2026"
    "auto.offset.reset"   = "latest"
    "max.poll.records"    = "1000"
    "ssl.context.service" = $SSL
}
$r = Set-ProcessorProperties -Id $CK -Properties $props
Write-Host "CK_VALIDATION=$($r.component.validationStatus)"
if ($r.component.validationErrors) { Write-Host "CK_ERRORS=$($r.component.validationErrors)" }

# 3) запуск процессоров по именам
Write-Host "=== START processors ==="
@($CK, $LA, $PF) | ForEach-Object {
    $st = Start-Processor -Id $_
    Write-Host "START $_ -> $st"
    Start-Sleep -Seconds 1
}

Write-Host "DONE"
