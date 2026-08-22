$ErrorActionPreference = "Continue"
$NI = "https://localhost:8443/nifi-api"
$PG = "296ae51a-01a0-1000-4cd2-fd63aaf57fcd"
$SSL = "2972aae5-01a0-1000-cb1a-92fb9304c57d"
$CID = "44444444-4444-4444-4444-444444444444"
$t = (curl.exe -sk -X POST "$NI/access/token" -d "username=admin&password=Admin123!nifi").Trim()

function GetRev($Path) { (curl.exe -sk -H "Authorization: Bearer $t" "$NI$Path" | ConvertFrom-Json).revision.version }

# ---- delete existing processors + connections ----
$ids = @(
  "processors/29739d28-01a0-1000-a786-8a8e5afea8ed",
  "processors/29739e28-01a0-1000-97e0-93e6b28f1a8f",
  "processors/29739eb8-01a0-1000-8307-baf594e956ce",
  "connections/29739f38-01a0-1000-d4ec-dafb297c0368",
  "connections/2973a039-01a0-1000-8f4c-c5824fb30c34"
)
foreach ($p in $ids) {
  $r = GetRev $p
  Write-Host "DELETE $p (rev $r)"
  curl.exe -sk -X DELETE "$NI/$p`?version=$r" -H "Authorization: Bearer $t" | Out-Null
}

# ---- enable SSL service ----
$sslState = (curl.exe -sk -H "Authorization: Bearer $t" "$NI/controller-services/$SSL" | ConvertFrom-Json).component.state
Write-Host "SSL_STATE_BEFORE=$sslState"
if ($sslState -ne "ENABLED") {
  $r = GetRev "controller-services/$SSL"
  $b = "{`"revision`":{`"version`":$r,`"clientId`":`"$CID`"},`"component`":{`"state`":`"ENABLED`"}}"
  $b | Set-Content -Path "tmp_enable.json" -Encoding ASCII
  $resp = curl.exe -sk -X PUT "$NI/controller-services/$SSL" -H "Authorization: Bearer $t" -H "Content-Type: application/json" -d "@tmp_enable.json"
  Write-Host "SSL_ENABLE_RESP_STATE=$(($resp | ConvertFrom-Json).component.state)"
}

# ---- create ConsumeKafka (correct props) ----
$ckBody = @"
{
  "revision": { "version": 0, "clientId": "$CID" },
  "component": {
    "type": "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6",
    "name": "ConsumeKafka-topic-1",
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
$ckBody | Set-Content -Path "tmp_ck.json" -Encoding ASCII
$ckResp = curl.exe -sk -X POST "$NI/process-groups/$PG/processors" -H "Authorization: Bearer $t" -H "Content-Type: application/json" -d "@tmp_ck.json"
$CK = ($ckResp | ConvertFrom-Json).id
Write-Host "CK_ID=$CK"
Write-Host "CK_VALID=$($ckResp | ConvertFrom-Json).component.validationStatus"

# ---- create LogAttribute ----
$laBody = @"
{
  "revision": { "version": 0, "clientId": "$CID" },
  "component": {
    "type": "org.apache.nifi.processors.standard.LogAttribute",
    "name": "LogAttribute",
    "config": { "properties": { "Log Level": "info" } }
  }
}
"@
$laBody | Set-Content -Path "tmp_la.json" -Encoding ASCII
$LA = (curl.exe -sk -X POST "$NI/process-groups/$PG/processors" -H "Authorization: Bearer $t" -H "Content-Type: application/json" -d "@tmp_la.json" | ConvertFrom-Json).id
Write-Host "LA_ID=$LA"

# ---- create PutFile ----
$pfBody = @"
{
  "revision": { "version": 0, "clientId": "$CID" },
  "component": {
    "type": "org.apache.nifi.processors.standard.PutFile",
    "name": "PutFile",
    "config": {
      "properties": { "Directory": "/opt/nifi/data/consumed", "Conflict Resolution Strategy": "replace" },
      "autoTerminatedRelationships": [ "success", "failure" ]
    }
  }
}
"@
$pfBody | Set-Content -Path "tmp_pf.json" -Encoding ASCII
$PF = (curl.exe -sk -X POST "$NI/process-groups/$PG/processors" -H "Authorization: Bearer $t" -H "Content-Type: application/json" -d "@tmp_pf.json" | ConvertFrom-Json).id
Write-Host "PF_ID=$PF"

# ---- connections ----
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
  curl.exe -sk -X POST "$NI/process-groups/$PG/connections" -H "Authorization: Bearer $t" -H "Content-Type: application/json" -d "@tmp_conn.json" | Select-String -Pattern '"id":"297' | Select-Object -First 1 | Out-Host
}
Connect $CK $LA
Connect $LA $PF

# ---- start ----
function StartProc($Id) {
  $r = GetRev "processors/$Id"
  $b = "{`"revision`":{`"version`":$r,`"clientId`":`"$CID`"},`"component`":{`"id`":`"$Id`",`"state`":`"RUNNING`"}}"
  $b | Set-Content -Path "tmp_start.json" -Encoding ASCII
  $resp = curl.exe -sk -X PUT "$NI/processors/$Id" -H "Authorization: Bearer $t" -H "Content-Type: application/json" -d "@tmp_start.json"
  Write-Host "START $Id -> $(($resp | ConvertFrom-Json).component.state)"
}
StartProc $CK
StartProc $LA
StartProc $PF

Write-Host "DONE CK=$CK LA=$LA PF=$PF"