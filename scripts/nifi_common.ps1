<#
.SYNOPSIS
    Общий модуль для работы с NiFi REST API.

.DESCRIPTION
    Все идентификаторы (process group, controller services, процессоры,
    коннекшены) резолвятся ПО ИМЕНИ через API. Никаких хардкод-UUID:
    скрипты работают на любой инсталляции NiFi, где компоненты названы
    одинаково, независимо от сгенерированных при создании идентификаторов.

    Подключение:  . "$PSScriptRoot/nifi_common.ps1"
#>

$script:NiFiBase   = if ($env:NIFI_URL)  { $env:NIFI_URL }  else { "https://localhost:8443/nifi-api" }
$script:NiFiUser   = if ($env:NIFI_USER) { $env:NIFI_USER } else { "admin" }
$script:NiFiPass   = if ($env:NIFI_PASS) { $env:NIFI_PASS } else { "Admin123!nifi" }
$script:Token      = $null
$script:ClientId   = [guid]::NewGuid().ToString()
$script:RootPgId   = "root"

# Конфигурация флоу по умолчанию (имена компонентов, а не UUID)
$script:DefaultNames = @{
    ProcessGroup       = ""                      # пусто -> корневая группа (root)
    SslService         = "YandexKafkaSSL"
    ConsumeKafka       = "ConsumeKafka-topic-1"
    LogAttribute       = "LogAttribute"
    PutFile            = "PutFile"
}

function Connect-NiFi {
    param(
        [string]$BaseUrl = $script:NiFiBase,
        [string]$User    = $script:NiFiUser,
        [string]$Password = $script:NiFiPass
    )
    $script:NiFiBase = $BaseUrl
    $script:NiFiUser = $User
    $script:NiFiPass = $Password
    $script:Token = (curl.exe -sk -X POST "$BaseUrl/access/token" `
        -d "username=$User&password=$Password").Trim()
    if (-not $script:Token) { throw "Не удалось получить токен NiFi (проверьте URL/учётные данные)" }
}

# Универсальный вызов API. Возвращает распарсенный JSON при успехе (HTTP 2xx)
# либо БРОСАЕТ исключение с кодом и телом ответа при ошибке (чтобы сбои не
# проглатывались молча, как это было с HTML-страницами логина/ошибок NiFi).
# Тело запроса всегда пишется во временный файл (-d "@file"), чтобы избежать
# проблем с экранированием спецсимволов в PowerShell + curl.
function Invoke-NiFiApi {
    param(
        [Parameter(Mandatory)] [string]$Method,
        [Parameter(Mandatory)] [string]$Path,
        [string]$BodyFile,
        [string]$Body
    )
    $base = $script:NiFiBase.TrimEnd('/')
    $normPath = '/' + $Path.TrimStart('/')
    $curlArgs = @("-sk", "-X", $Method, "$base$normPath",
              "-H", "Authorization: Bearer $($script:Token)",
              "-H", "Content-Type: application/json",
              "-w", "`n%{http_code}")
    $tmp = $null
    if ($Body) {
        $tmp = [System.IO.Path]::GetTempFileName()
        Set-Content -Path $tmp -Value $Body -Encoding ASCII
        $curlArgs += @("-d", "@$tmp")
    } elseif ($BodyFile) {
        $curlArgs += @("-d", "@$BodyFile")
    }
    try {
        $resp = curl.exe @curlArgs 2>$null
        $lines = $resp -split "`n"
        $status = $lines[-1]
        $body = ($lines[0..($lines.Length - 2)] -join "`n")
        if ($status -match '^2\d\d') {
            try { return $body | ConvertFrom-Json } catch { return $body }
        }
        throw "NiFi API $Method $normPath -> HTTP $status : $body"
    } finally {
        if ($tmp -and (Test-Path $tmp)) { Remove-Item $tmp -Force }
    }
}

# ---- резолвинг по имени ----

function Get-ProcessGroupIdByName {
    param([string]$Name)
    if (-not $Name) { return $script:RootPgId }
    $q = [uri]::EscapeDataString($Name)
    $r = Invoke-NiFiApi GET "/flow/search-results?q=$q"
    $pg = $r.processGroupResults | Where-Object { $_.name -eq $Name } | Select-Object -First 1
    if (-not $pg) { throw "Process Group '$Name' не найден" }
    return $pg.id
}

function Get-ControllerServiceIdByName {
    param([string]$ProcessGroupId = $script:RootPgId, [string]$Name)
    $r = Invoke-NiFiApi GET "/flow/process-groups/$ProcessGroupId/controller-services"
    $cs = $r.controllerServices | Where-Object { $_.component.name -eq $Name } | Select-Object -First 1
    if (-not $cs) { throw "Controller Service '$Name' не найден в PG $ProcessGroupId" }
    return $cs.id
}

function Get-ProcessorIdByName {
    param([string]$ProcessGroupId = $script:RootPgId, [string]$Name)
    $r = Invoke-NiFiApi GET "/process-groups/$ProcessGroupId/processors"
    $p = $r.processors | Where-Object { $_.component.name -eq $Name } | Select-Object -First 1
    if (-not $p) { throw "Processor '$Name' не найден в PG $ProcessGroupId" }
    return $p.id
}

function Get-ConnectionIdByEndpoints {
    param([string]$ProcessGroupId = $script:RootPgId, [string]$SourceName, [string]$DestinationName)
    $r = Invoke-NiFiApi GET "/process-groups/$ProcessGroupId/connections"
    $c = $r.connections | Where-Object {
        $_.component.source.name -eq $SourceName -and $_.component.destination.name -eq $DestinationName
    } | Select-Object -First 1
    if (-not $c) { throw "Connection $SourceName -> $DestinationName не найден в PG $ProcessGroupId" }
    return $c.id
}

# Возвращает ВСЕ id процессоров с заданным именем (важно при дубликатах).
function Get-ProcessorIdsByName {
    param([string]$ProcessGroupId = $script:RootPgId, [string]$Name)
    $r = Invoke-NiFiApi GET "/process-groups/$ProcessGroupId/processors"
    return @($r.processors | Where-Object { $_.component.name -eq $Name } | ForEach-Object { $_.id })
}

# Возвращает ВСЕ id коннекшенов между именованными процессорами.
function Get-ConnectionIdsByEndpoints {
    param([string]$ProcessGroupId = $script:RootPgId, [string]$SourceName, [string]$DestinationName)
    $r = Invoke-NiFiApi GET "/process-groups/$ProcessGroupId/connections"
    return @($r.connections | Where-Object {
        $_.component.source.name -eq $SourceName -and $_.component.destination.name -eq $DestinationName
    } | ForEach-Object { $_.id })
}

function Get-EntityRevision { param([string]$Path) return (Invoke-NiFiApi GET $Path).revision.version }

# ---- управление состоянием ----

function Enable-ControllerService {
    param([string]$Id)
    $v = Get-EntityRevision "controller-services/$Id"
    $b = @{revision=@{version=$v; clientId=$script:ClientId}; component=@{state="ENABLED"}} | ConvertTo-Json -Depth 10
    Invoke-NiFiApi PUT "controller-services/$Id" -Body $b | Out-Null
    # подтверждаем реальное состояние отдельным GET (PUT не всегда возвращает state)
    return (Invoke-NiFiApi GET "controller-services/$Id").component.state
}

function Start-Processor {
    param([string]$Id)
    $v = Get-EntityRevision "processors/$Id"
    $b = @{revision=@{version=$v; clientId=$script:ClientId}; component=@{id=$Id; state="RUNNING"}} | ConvertTo-Json -Depth 10
    $r = Invoke-NiFiApi PUT "processors/$Id" -Body $b
    return $r.component.state
}

function Stop-Processor {
    param([string]$Id)
    try {
        $v = Get-EntityRevision "processors/$Id"
        $b = @{revision=@{version=$v; clientId=$script:ClientId}; component=@{id=$Id; state="STOPPED"}} | ConvertTo-Json -Depth 10
        $r = Invoke-NiFiApi PUT "processors/$Id" -Body $b
        return $r.component.state
    } catch { Write-Host "skip stop $Id (absent)" }
}

function New-SslContextService {
    param([string]$ProcessGroupId = $script:RootPgId, [string]$Name = "YandexKafkaSSL",
          [string]$TruststoreFile = "/opt/nifi/certs/nifi-truststore.jks",
          [string]$TruststorePass = "changeit", [string]$TruststoreType = "JKS")
    $b = @{revision=@{version=0; clientId=$script:ClientId};
           component=@{type="org.apache.nifi.ssl.StandardSSLContextService"; name=$Name;
                       properties=@{
                           "Truststore Filename"=$TruststoreFile;
                           "Truststore Password"=$TruststorePass;
                           "Truststore Type"=$TruststoreType;
                           "SSL Protocol"="TLS"}}} | ConvertTo-Json -Depth 10
    $r = Invoke-NiFiApi POST "process-groups/$ProcessGroupId/controller-services" -Body $b
    return $r.id
}

function Set-ProcessorProperties {
    param([string]$Id, [hashtable]$Properties)
    $v = Get-EntityRevision "processors/$Id"
    $cfg = @{properties = $Properties}
    $b = @{revision=@{version=$v; clientId=$script:ClientId}; component=@{id=$Id; config=$cfg}} | ConvertTo-Json -Depth 10
    $r = Invoke-NiFiApi PUT "processors/$Id" -Body $b
    return $r
}

# ---- создание / удаление (идемпотентные обёртки) ----

function New-Processor {
    param([string]$ProcessGroupId = $script:RootPgId, [string]$Type, [string]$Name,
          [hashtable]$Properties = @{}, [string[]]$AutoTerminated = @())
    $cfg = @{properties = $Properties}
    if ($AutoTerminated.Count) { $cfg["autoTerminatedRelationships"] = $AutoTerminated }
    $b = @{revision=@{version=0; clientId=$script:ClientId};
           component=@{type=$Type; name=$Name; config=$cfg}} | ConvertTo-Json -Depth 10
    $r = Invoke-NiFiApi POST "process-groups/$ProcessGroupId/processors" -Body $b
    return $r.id
}

function New-Connection {
    param([string]$ProcessGroupId = $script:RootPgId, [string]$SourceId, [string]$DestId,
          [string[]]$Relationships = @("success"))
    $b = @{revision=@{version=0; clientId=$script:ClientId};
           component=@{source=@{id=$SourceId; groupId=$ProcessGroupId; type="PROCESSOR"};
                       destination=@{id=$DestId; groupId=$ProcessGroupId; type="PROCESSOR"};
                       selectedRelationships=$Relationships}} | ConvertTo-Json -Depth 10
    $r = Invoke-NiFiApi POST "process-groups/$ProcessGroupId/connections" -Body $b
    return $r.id
}

# Удаление по ID; толерантное — не падает, если компонент уже отсутствует.
# Для процессоров сначала останавливаем (конфиг менять/удалять можно только
# на остановленном), затем удаляем с актуальной ревизией, с повтором при 409.
function Remove-ComponentById {
    param([string]$Kind, [string]$Id)
    try {
        if ($Kind -eq "processors") {
            try { Stop-Processor -Id $Id | Out-Null } catch { Write-Host "warn: не удалось остановить $Id (возможно, уже удалён)" }
        }
        for ($attempt = 1; $attempt -le 3; $attempt++) {
            try {
                $v = Get-EntityRevision "$Kind/$Id"
                Invoke-NiFiApi DELETE "$Kind/$Id`?version=$v" | Out-Null
                Write-Host "removed $Kind $Id (rev $v)"
                return
            } catch {
                Write-Host "retry $attempt удаления $Kind $Id : $_"
                Start-Sleep -Seconds 1
            }
        }
    } catch {
        Write-Host "skip $Kind $Id (already absent or unreachable): $_"
    }
}
