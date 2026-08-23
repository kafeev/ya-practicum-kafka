<#
.SYNOPSIS
    Финальная активация флоу NiFi: включение SSL и запуск процессоров.
    Идентификаторы резолвятся по именам через NiFi API (без хардкода UUID).
#>
$ErrorActionPreference = "Stop"
. "$PSScriptRoot/nifi_common.ps1"

Connect-NiFi

$PG  = Get-ProcessGroupIdByName   $script:DefaultNames.ProcessGroup
$SSL = Get-ControllerServiceIdByName -ProcessGroupId $PG -Name $script:DefaultNames.SslService
$CK  = Get-ProcessorIdByName      -ProcessGroupId $PG -Name $script:DefaultNames.ConsumeKafka
$LA  = Get-ProcessorIdByName      -ProcessGroupId $PG -Name $script:DefaultNames.LogAttribute
$PF  = Get-ProcessorIdByName      -ProcessGroupId $PG -Name $script:DefaultNames.PutFile

# включаем SSL-сервис
Write-Host "=== ENABLE SSL ==="
$s = Enable-ControllerService -Id $SSL
Write-Host "SSL_STATE=$s"
Start-Sleep -Seconds 2

# запускаем 3 процессора по именам
Write-Host "=== START processors ==="
@($CK, $LA, $PF) | ForEach-Object {
    $st = Start-Processor -Id $_
    Write-Host "START $_ -> $st"
    Start-Sleep -Seconds 1
}

Write-Host "DONE"
