<#
.SYNOPSIS
    Первичная установка флоу NiFi (идемпотентная).
    Создаёт SSL-сервис и процессоры, если их нет, и запускает флоу.
    Все UUID резолвятся по именам через API — хардкод отсутствует.
#>
$ErrorActionPreference = "Stop"
. "$PSScriptRoot/nifi_common.ps1"

Connect-NiFi

$PG = Get-ProcessGroupIdByName $script:DefaultNames.ProcessGroup

# --- SSL: найти или создать ---
try {
    $SSL = Get-ControllerServiceIdByName -ProcessGroupId $PG -Name $script:DefaultNames.SslService
    Write-Host "SSL found: $SSL"
} catch {
    $SSL = New-SslContextService -ProcessGroupId $PG -Name $script:DefaultNames.SslService
    Write-Host "SSL created: $SSL"
}
if ((Invoke-NiFiApi GET "controller-services/$SSL").component.state -ne "ENABLED") {
    Write-Host "SSL_STATE=$(Enable-ControllerService -Id $SSL)"
}
Start-Sleep -Seconds 2

# --- процессоры: найти или создать ---
function Ensure-Processor {
    param([string]$Name, [string]$Type, [hashtable]$Properties, [string[]]$AutoTerminated = @())
    try { return Get-ProcessorIdByName -ProcessGroupId $PG -Name $Name }
    catch {
        $id = New-Processor -ProcessGroupId $PG -Type $Type -Name $Name -Properties $Properties -AutoTerminated $AutoTerminated
        Write-Host "created $Name -> $id"
        return $id
    }
}

$CK = Ensure-Processor -Name $script:DefaultNames.ConsumeKafka `
    -Type "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6" `
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

$LA = Ensure-Processor -Name $script:DefaultNames.LogAttribute `
    -Type "org.apache.nifi.processors.standard.LogAttribute" `
    -Properties @{ "Log Level" = "INFO" }
Write-Host "LA_ID=$LA"

$PF = Ensure-Processor -Name $script:DefaultNames.PutFile `
    -Type "org.apache.nifi.processors.standard.PutFile" `
    -Properties @{ "Directory" = "/opt/nifi/data/consumed"; "Conflict Resolution Strategy" = "Replace" } `
    -AutoTerminated @("success", "failure")
Write-Host "PF_ID=$PF"

# --- коннекшены: найти или создать ---
function Ensure-Connection {
    param([string]$SourceName, [string]$DestName, [string]$SourceId, [string]$DestId)
    try { return Get-ConnectionIdByEndpoints -ProcessGroupId $PG -SourceName $SourceName -DestinationName $DestName }
    catch { return (New-Connection -ProcessGroupId $PG -SourceId $SourceId -DestId $DestId) }
}
Ensure-Connection -SourceName $script:DefaultNames.ConsumeKafka -DestName $script:DefaultNames.LogAttribute -SourceId $CK -DestId $LA | ForEach-Object { Write-Host "CONN_CK_LA=$_" }
Ensure-Connection -SourceName $script:DefaultNames.LogAttribute -DestName $script:DefaultNames.PutFile -SourceId $LA -DestId $PF | ForEach-Object { Write-Host "CONN_LA_PF=$_" }

# --- запуск ---
@($CK, $LA, $PF) | ForEach-Object {
    Write-Host "START $_ -> $(Start-Processor -Id $_)"
    Start-Sleep -Seconds 1
}

Write-Host "DONE CK=$CK LA=$LA PF=$PF"
