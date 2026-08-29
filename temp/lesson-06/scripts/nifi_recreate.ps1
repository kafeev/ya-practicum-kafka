<#
.SYNOPSIS
    Идемпотентное пересоздание флоу NiFi (ConsumeKafka -> LogAttribute -> PutFile).
    Все идентификаторы резолвятся ПО ИМЕНИ через API, поэтому скрипт
    работает на чистой инсталляции без каких-либо захардкоженных UUID.
#>
$ErrorActionPreference = "Stop"
. "$PSScriptRoot/nifi_common.ps1"

Connect-NiFi

$PG = Get-ProcessGroupIdByName $script:DefaultNames.ProcessGroup

# --- SSL context service: найти по имени или создать ---
try {
    $SSL = Get-ControllerServiceIdByName -ProcessGroupId $PG -Name $script:DefaultNames.SslService
    Write-Host "SSL found: $SSL"
} catch {
    $SSL = New-SslContextService -ProcessGroupId $PG -Name $script:DefaultNames.SslService
    Write-Host "SSL created: $SSL"
}
$sslState = (Invoke-NiFiApi GET "controller-services/$SSL").component.state
if ($sslState -ne "ENABLED") { Enable-ControllerService -Id $SSL | ForEach-Object { Write-Host "SSL_STATE=$_" } }
Start-Sleep -Seconds 2

# --- остановить все наши процессоры (по именам, включая дубликаты) ---
$allNames = @($script:DefaultNames.ConsumeKafka, $script:DefaultNames.LogAttribute, $script:DefaultNames.PutFile)
foreach ($nm in $allNames) {
    foreach ($id in (Get-ProcessorIdsByName -ProcessGroupId $PG -Name $nm)) {
        try { Stop-Processor -Id $id | Out-Null } catch { Write-Host "warn: не остановлен $id" }
    }
}
Start-Sleep -Seconds 2

# --- удалить коннекшены ДО процессоров (иначе NiFi откажет: процессор-приёмник занят) ---
foreach ($pair in @(
        @($script:DefaultNames.ConsumeKafka, $script:DefaultNames.LogAttribute),
        @($script:DefaultNames.LogAttribute, $script:DefaultNames.PutFile))) {
    foreach ($cid in (Get-ConnectionIdsByEndpoints -ProcessGroupId $PG -SourceName $pair[0] -DestinationName $pair[1])) {
        Remove-ComponentById -Kind "connections" -Id $cid
    }
}

# --- удалить процессоры по именам (все совпадения, толерантно) ---
foreach ($nm in $allNames) {
    foreach ($id in (Get-ProcessorIdsByName -ProcessGroupId $PG -Name $nm)) {
        Remove-ComponentById -Kind "processors" -Id $id
    }
    Start-Sleep -Seconds 1
}

# --- создать процессоры ---
$CK = New-Processor -ProcessGroupId $PG `
    -Type "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6" `
    -Name $script:DefaultNames.ConsumeKafka `
    -Properties @{
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
Write-Host "CK_ID=$CK"

$LA = New-Processor -ProcessGroupId $PG `
    -Type "org.apache.nifi.processors.standard.LogAttribute" `
    -Name $script:DefaultNames.LogAttribute `
    -Properties @{ "Log Level" = "info" }
Write-Host "LA_ID=$LA"

$PF = New-Processor -ProcessGroupId $PG `
    -Type "org.apache.nifi.processors.standard.PutFile" `
    -Name $script:DefaultNames.PutFile `
    -Properties @{ "Directory" = "/opt/nifi/data/consumed"; "Conflict Resolution Strategy" = "replace" } `
    -AutoTerminated @("success", "failure")
Write-Host "PF_ID=$PF"

# --- коннекшены по именам (резолвим id только что созданных процессоров) ---
New-Connection -ProcessGroupId $PG -SourceId $CK -DestId $LA  | ForEach-Object { Write-Host "CONN_CK_LA=$_" }
New-Connection -ProcessGroupId $PG -SourceId $LA -DestId $PF  | ForEach-Object { Write-Host "CONN_LA_PF=$_" }

# --- запуск ---
@($CK, $LA, $PF) | ForEach-Object {
    $st = Start-Processor -Id $_
    Write-Host "START $_ -> $st"
    Start-Sleep -Seconds 1
}

Write-Host "DONE CK=$CK LA=$LA PF=$PF"
