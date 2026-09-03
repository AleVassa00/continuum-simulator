param(
    [string]$EdgeId = "edge-3",
    [int]$MaxEvents = 1000,
    [string]$AccelerationFactor = "10000"
)

$ErrorActionPreference = "Stop"

$ComposeFile = "deploy/compose/continuum.generated.yml"
$ReplayFile = "dataset/derived/replay_by_edge/$EdgeId.csv"

if (-not (Test-Path $ComposeFile)) {
    throw "Compose non trovato: $ComposeFile"
}

if (-not (Test-Path $ReplayFile)) {
    throw "Replay shard non trovato: $ReplayFile"
}

Write-Host ""
Write-Host "=== 1. RESET COMPLETO DELLA RUN ==="
docker compose -f $ComposeFile down -v --remove-orphans

Write-Host ""
Write-Host "Container di progetto eventualmente rimasti:"
docker ps -a --filter "name=^kafka$" --filter "name=^kafka-init$" --filter "name=^mqtt-$EdgeId$" --filter "name=^$EdgeId$"

Write-Host ""
Write-Host "=== 2. VERIFICA CODICE GO ==="
go test ./...
go vet ./...

Write-Host ""
Write-Host "=== 3. REBUILD EDGE CON IL CODICE ATTUALE ==="
docker build -f deploy/docker/edge.Dockerfile -t continuum-edge:local .

Write-Host ""
Write-Host "=== 4. AVVIO SOLO INFRASTRUTTURA NECESSARIA PER $EdgeId ==="
docker compose -f $ComposeFile up -d kafka kafka-init "mqtt-$EdgeId" $EdgeId

Write-Host ""
Write-Host "=== 5. ATTESA READY DELL'EDGE ==="

$healthy = $false
for ($i = 0; $i -lt 60; $i++) {
    $status = docker inspect -f "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $EdgeId 2>$null

    if ($status -eq "healthy") {
        $healthy = $true
        break
    }

    if ($status -eq "unhealthy" -or $status -eq "exited" -or $status -eq "dead") {
        Write-Host ""
        Write-Host "Edge non avviato correttamente. Ultimi log:"
        docker logs --tail 100 $EdgeId
        throw "Stato ${EdgeId}: $status"
    }

    Start-Sleep -Seconds 1
}

if (-not $healthy) {
    Write-Host ""
    Write-Host "Ultimi log Edge:"
    docker logs --tail 100 $EdgeId
    throw "$EdgeId non e diventato healthy"
}

Write-Host ""
Write-Host "=== INFRASTRUTTURA PRONTA ==="
docker compose -f $ComposeFile ps kafka kafka-init "mqtt-$EdgeId" $EdgeId

Write-Host ""
Write-Host "Ora NON chiudere Docker."
Write-Host ""
Write-Host "TERMINALE 2 - log Edge:"
Write-Host "  docker logs -f $EdgeId"
Write-Host ""
Write-Host "TERMINALE 3 - osserva i messaggi MQTT:"
Write-Host "  docker exec mqtt-$EdgeId mosquitto_sub -h localhost -p 1883 -t `"sensors/+/telemetry`" -v"
Write-Host ""
Write-Host "TERMINALE 4 - avvia il Simulator dall'host:"
Write-Host "  `$env:SITE_ID=`"$EdgeId`""
Write-Host "  `$env:MQTT_ENDPOINT=`"tcp://localhost:18833`""
Write-Host "  `$env:REPLAY_FILE=`"$ReplayFile`""
Write-Host "  `$env:REPLAY_EPOCH=`"2025-01-01T00:00:00Z`""
Write-Host "  `$env:REPLAY_START_AT=(Get-Date).ToUniversalTime().AddSeconds(10).ToString(`"o`")"
Write-Host "  `$env:ACCELERATION_FACTOR=`"$AccelerationFactor`""
Write-Host "  `$env:TELEMETRY_QUEUE_CAPACITY=`"1000`""
Write-Host "  `$env:MAX_EVENTS=`"$MaxEvents`""
Write-Host "  go run ./cmd/simulator"
Write-Host ""
Write-Host "Quando hai finito la prova:"
Write-Host "  docker compose -f $ComposeFile down -v --remove-orphans"
