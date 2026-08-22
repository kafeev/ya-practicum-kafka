$ErrorActionPreference = "Continue"
$NI = "https://localhost:8443/nifi-api"
$SSL = "2972aae5-01a0-1000-cb1a-92fb9304c57d"
$CID = "55555555-5555-5555-5555-555555555555"
$t = (curl.exe -sk -X POST "$NI/access/token" -d "username=admin&password=Admin123!nifi").Trim()
function J($p) { curl.exe -sk -H "Authorization: Bearer $t" "$NI$p" }

# ---- delete old duplicate connections + processors ----
$oldConns = @("29739f38-01a0-1000-d4ec-dafb297c0368","2973a039-01a0-1000-8f4c-c5824fb30c34")
$oldPros  = @("29739d28-01a0-1000-a786-8a8e5afea8ed","29739e28-01a0-1000-97e0-93e6b28f1a8f","29739eb8-01a0-1000-8307-baf594e956ce")
foreach ($c in $oldConns) {
  $v = (J "/connections/$c" | ConvertFrom-Json).revision.version
  curl.exe -sk -X DELETE "$NI/connections/$c`?version=$v" -H "Authorization: Bearer $t" | Out-Null
  Write-Host "deleted connection $c"
}
foreach ($p in $oldPros) {
  $v = (J "/processors/$p" | ConvertFrom-Json).revision.version
  curl.exe -sk -X DELETE "$NI/processors/$p`?version=$v" -H "Authorization: Bearer $t" | Out-Null
  Write-Host "deleted processor $p"
}

# ---- enable SSL service (with component.id) ----
$enBody = "{`"revision`":{`"version`":1,`"clientId`":`"$CID`"},`"component`":{`"id`":`"$SSL`",`"state`":`"ENABLED`"}}"
$enBody | Set-Content -Path "tmp_enable3.json" -Encoding ASCII
$en = curl.exe -sk -w "`nHTTP=%{http_code}" -X PUT "$NI/controller-services/$SSL" -H "Authorization: Bearer $t" -H "Content-Type: application/json" -d "@tmp_enable3.json"
$en | Select-String -Pattern '"state":"(ENABLED|DISABLED|ENABLING)"|HTTP=' | Out-Host
Start-Sleep -Seconds 3

# ---- start the 3 valid processors ----
$pros = @("2974eb86-01a0-1000-bf00-d0b21f7aca2c","2974ebe6-01a0-1000-c0e3-81f9e6e6a617","2974ec3c-01a0-1000-b7d2-0aec3866b5e5")
foreach ($p in $pros) {
  $j = J "/processors/$p" | ConvertFrom-Json
  $v = $j.revision.version
  $st = $j.component.state
  $vb = "{`"revision`":{`"version`":$v,`"clientId`":`"$CID`"},`"component`":{`"id`":`"$p`",`"state`":`"RUNNING`"}}"
  $vb | Set-Content -Path "tmp_start.json" -Encoding ASCII
  $r = curl.exe -sk -w " HTTP=%{http_code}" -X PUT "$NI/processors/$p" -H "Authorization: Bearer $t" -H "Content-Type: application/json" -d "@tmp_start.json"
  $r | Select-String -Pattern '"state":"(RUNNING|STOPPED|INVALID)"|HTTP=' | Out-Host
}
Write-Host "DONE"