$ErrorActionPreference = "Continue"
$NI = "https://localhost:8443/nifi-api"
$CRED = "admin:Admin123!nifi"

function Invoke-NiFi($Method, $Path, $Body) {
    $args = @("-sk", "-X", $Method, "$NI$Path", "-H", "Content-Type: application/json")
    if ($Body) { $args += @("-d", $Body) }
    $out = curl.exe @args 2>$null
    return $out
}

# 1) token
$token = (curl.exe -sk -X POST "$NI/access/token" -d "username=admin&password=Admin123!nifi").Trim()
Write-Host "TOKEN_LEN=$($token.Length)"
$H = "-H", "Authorization: Bearer $token", "-H", "Content-Type: application/json"

function Post-J($Path, $Body) {
    curl.exe -sk -X POST "$NI$Path" -H "Authorization: Bearer $token" -H "Content-Type: application/json" -d $Body
}
function Put-J($Path, $Body) {
    curl.exe -sk -X PUT "$NI$Path" -H "Authorization: Bearer $token" -H "Content-Type: application/json" -d $Body
}

$clientId = [guid]::NewGuid().ToString()

# 2) root pg id
$root = (curl.exe -sk -H "Authorization: Bearer $token" "$NI/process-groups/root")
Write-Host "ROOT=$root"
$rootId = ($root | ConvertFrom-Json).id
Write-Host "ROOT_ID=$rootId"

# 3) create SSL context service under root
$sslBody = @{
    revision = @{version=0; clientId=$clientId}
    component = @{
        type="org.apache.nifi.controller.StandardSSLContextService"
        name="YandexKafkaSSL"
        properties = @{
            "Truststore Filename"="/opt/nifi/certs/nifi-truststore.jks"
            "Truststore Password"="changeit"
            "Truststore Type"="JKS"
            "SSL Protocol"="TLS"
        }
    }
} | ConvertTo-Json -Depth 10

$sslResp = Post-J "/process-groups/$rootId/controller-services" $sslBody
Write-Host "SSL_RESP=$sslResp"
$sslId = ($sslResp | ConvertFrom-Json).id
$sslRev = ($sslResp | ConvertFrom-Json).revision.version
Write-Host "SSL_ID=$sslId SSL_REV=$sslRev"

# enable SSL service
$enableBody = @{
    revision = @{version=$sslRev; clientId=$clientId}
    component = @{state="ENABLED"}
} | ConvertTo-Json -Depth 10
$en = Put-J "/controller-services/$sslId" $enableBody
Write-Host "ENABLE_SSL=$en"
