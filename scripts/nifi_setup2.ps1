$ErrorActionPreference = "Continue"
$NI = "https://localhost:8443/nifi-api"
$PG = "296ae51a-01a0-1000-4cd2-fd63aaf57fcd"
$SSL = "2972aae5-01a0-1000-cb1a-92fb9304c57d"
$CID = "22222222-2222-2222-2222-222222222222"
$t = (curl.exe -sk -X POST "$NI/access/token" -d "username=admin&password=Admin123!nifi").Trim()
$H = "-H", "Authorization: Bearer $t", "-H", "Content-Type: application/json"

function SendReq($Method, $Path, $File) {
    if ($File) {
        curl.exe -sk -X $Method "$NI$Path" -H "Authorization: Bearer $t" -H "Content-Type: application/json" -d "@$File"
    } else {
        curl.exe -sk -X $Method "$NI$Path" -H "Authorization: Bearer $t"
    }
}

# enable SSL service
$enableBody = "{`"revision`":{`"version`":1,`"clientId`":`"$CID`"},`"component`":{`"state`":`"ENABLED`"}}"
$enableBody | Set-Content -Path "tmp_enable.json" -Encoding ASCII
Write-Host "=== ENABLE SSL ==="
SendReq PUT "/controller-services/$SSL" "tmp_enable.json" | Select-String -Pattern '"state"' | Out-Host

# ConsumeKafka_2_6
$ckBody = @"
{
  "revision": { "version": 0, "clientId": "$CID" },
  "component": {
    "type": "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6",
    "name": "ConsumeKafka-topic-1",
    "config": {
      "properties": {
        "kafka-brokers": "rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net:9091",
        "topic": "topic-1",
        "group.id": "nifi-consumer-group",
        "security-protocol": "SASL_SSL",
        "sasl-mechanism": "SCRAM_SHA_512",
        "username": "practicumuser",
        "password": "SecurePass2026",
        "auto.offset.reset": "latest",
        "max.poll.records": "1000",
        "SSL Context Service": "$SSL"
      }
    }
  }
}
"@
$ckBody | Set-Content -Path "tmp_ck.json" -Encoding ASCII
Write-Host "=== CREATE ConsumeKafka ==="
$ckResp = SendReq POST "/process-groups/$PG/processors" "tmp_ck.json"
$ckResp | Out-Host
$CK = ($ckResp | ConvertFrom-Json).id

# LogAttribute
$laBody = @"
{
  "revision": { "version": 0, "clientId": "$CID" },
  "component": {
    "type": "org.apache.nifi.processors.standard.LogAttribute",
    "name": "LogAttribute",
    "config": { "properties": { "Log Level": "INFO" } }
  }
}
"@
$laBody | Set-Content -Path "tmp_la.json" -Encoding ASCII
Write-Host "=== CREATE LogAttribute ==="
$laResp = SendReq POST "/process-groups/$PG/processors" "tmp_la.json"
$LA = ($laResp | ConvertFrom-Json).id
Write-Host "LA_ID=$LA"

# PutFile
$pfBody = @"
{
  "revision": { "version": 0, "clientId": "$CID" },
  "component": {
    "type": "org.apache.nifi.processors.standard.PutFile",
    "name": "PutFile",
    "config": { "properties": { "Directory": "/opt/nifi/data/consumed", "Conflict Resolution Strategy": "Replace" } }
  }
}
"@
$pfBody | Set-Content -Path "tmp_pf.json" -Encoding ASCII
Write-Host "=== CREATE PutFile ==="
$pfResp = SendReq POST "/process-groups/$PG/processors" "tmp_pf.json"
$PF = ($pfResp | ConvertFrom-Json).id
Write-Host "PF_ID=$PF"

# connections
function Connect($Src, $Dst) {
    $b = @"
{
  "revision": { "version": 0, "clientId": "$CID" },
  "component": {
    "source": { "id": "$Src", "groupId": "$PG", "type": "PROCESSOR" },
    "destination": { "id": "$Dst", "groupId": "$PG", "type": "PROCESSOR" },
    "selectedRelationships": [ "success" ]
  }
}
"@
    $b | Set-Content -Path "tmp_conn.json" -Encoding ASCII
    SendReq POST "/process-groups/$PG/connections" "tmp_conn.json" | Select-String -Pattern '"id"' | Select-Object -First 1 | Out-Host
}
Write-Host "=== CONNECT CK->LA ==="
Connect $CK $LA
Write-Host "=== CONNECT LA->PF ==="
Connect $LA $PF

# start processors
function StartProc($Id, $Rev) {
    $b = "{`"revision`":{`"version`":$Rev,`"clientId`":`"$CID`"},`"component`":{`"state`":`"RUNNING`"}}"
    $b | Set-Content -Path "tmp_start.json" -Encoding ASCII
    SendReq PUT "/processors/$Id" "tmp_start.json" | Select-String -Pattern '"state"' | Out-Host
}
Write-Host "=== START CK ==="
StartProc $CK 0
Start-Sleep -Seconds 1
Write-Host "=== START LA ==="
StartProc $LA 0
Write-Host "=== START PF ==="
StartProc $PF 0

Write-Host "CK_ID=$CK LA_ID=$LA PF_ID=$PF"