$ErrorActionPreference = "Continue"
$NI = "https://localhost:8443/nifi-api"
$PG = "296ae51a-01a0-1000-4cd2-fd63aaf57fcd"
$SSL = "2972aae5-01a0-1000-cb1a-92fb9304c57d"
$CK = "29739d28-01a0-1000-a786-8a8e5afea8ed"
$CID = "33333333-3333-3333-3333-333333333333"
$t = (curl.exe -sk -X POST "$NI/access/token" -d "username=admin&password=Admin123!nifi").Trim()

# 1) ensure SSL enabled
$sslState = (curl.exe -sk -H "Authorization: Bearer $t" "$NI/controller-services/$SSL" | ConvertFrom-Json).component.state
Write-Host "SSL_STATE=$sslState"
if ($sslState -ne "ENABLED") {
    $rev = (curl.exe -sk -H "Authorization: Bearer $t" "$NI/controller-services/$SSL" | ConvertFrom-Json).revision.version
    $b = "{`"revision`":{`"version`":$rev,`"clientId`":`"$CID`"},`"component`":{`"state`":`"ENABLED`"}}"
    $b | Set-Content -Path "tmp_enable.json" -Encoding ASCII
    curl.exe -sk -X PUT "$NI/controller-services/$SSL" -H "Authorization: Bearer $t" -H "Content-Type: application/json" -d "@tmp_enable.json" | Select-String -Pattern '"state"' | Out-Host
    Start-Sleep -Seconds 2
}

# 2) get current CK revision and PUT corrected properties
$ckJson = curl.exe -sk -H "Authorization: Bearer $t" "$NI/processors/$CK" | ConvertFrom-Json
$rev = $ckJson.revision.version
Write-Host "CK_REV=$rev"

$body = @"
{
  "revision": { "version": $rev, "clientId": "$CID" },
  "component": {
    "id": "$CK",
    "config": {
      "properties": {
        "bootstrap.servers": "rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net:9091",
        "topic": "topic-1",
        "group.id": "nifi-consumer-group",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "practicumuser",
        "sasl.password": "SecurePass2026",
        "auto.offset.reset": "latest",
        "max.poll.records": "1000",
        "ssl.context.service": "$SSL"
      }
    }
  }
}
"@
$body | Set-Content -Path "tmp_ckfix.json" -Encoding ASCII
Write-Host "=== PUT ConsumeKafka config ==="
$upd = curl.exe -sk -X PUT "$NI/processors/$CK" -H "Authorization: Bearer $t" -H "Content-Type: application/json" -d "@tmp_ckfix.json"
($upd | ConvertFrom-Json).component.validationStatus | Out-Host
($upd | ConvertFrom-Json).component.validationErrors | Out-Host

# 3) start all three processors
function StartProc($Id) {
    $r = (curl.exe -sk -H "Authorization: Bearer $t" "$NI/processors/$Id" | ConvertFrom-Json).revision.version
    $b = "{`"revision`":{`"version`":$r,`"clientId`":`"$CID`"},`"component`":{`"id`":`"$Id`",`"state`":`"RUNNING`"}}"
    $b | Set-Content -Path "tmp_start.json" -Encoding ASCII
    curl.exe -sk -X PUT "$NI/processors/$Id" -H "Authorization: Bearer $t" -H "Content-Type: application/json" -d "@tmp_start.json" | Select-String -Pattern '"runStatus"|"state"' | Out-Host
}
Write-Host "=== START processors ==="
StartProc $CK
Start-Sleep -Seconds 1
StartProc "29739e28-01a0-1000-97e0-93e6b28f1a8f"
StartProc "29739eb8-01a0-1000-8307-baf594e956ce"