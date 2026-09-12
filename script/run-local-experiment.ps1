param(
    [string]$Experiment = "experiments/baseline.yaml",

    # 0 = usa il numero di worker definito nel file YAML.
    [ValidateRange(0, 128)]
    [int]$Workers = 0,

    [string]$RunId = "",

    [ValidateRange(1, 3600)]
    [int]$MetricsIntervalSeconds = 5,

    [ValidateRange(30, 86400)]
    [int]$TimeoutSeconds = 3600,

    # Utile per run successive: evita di ricostruire le immagini se il codice non è cambiato.
    [switch]$SkipBuild,

    # Di default lo stack viene spento e i volumi vengono eliminati a fine run.
    [switch]$KeepContainers,

    # Conserva anche log e campioni grezzi nella sottocartella raw/.
    # In caso di run/post-processing fallito i raw vengono comunque mantenuti.
    [switch]$KeepRawArtifacts
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Find-RepoRoot {
    $candidates = @($PSScriptRoot, (Get-Location).Path)

    foreach ($candidate in $candidates) {
        $current = $candidate

        while (-not [string]::IsNullOrWhiteSpace($current)) {
            if (Test-Path (Join-Path $current "go.mod")) {
                return (Resolve-Path $current).Path
            }

            $parent = Split-Path $current -Parent
            if ([string]::IsNullOrWhiteSpace($parent) -or $parent -eq $current) {
                break
            }
            $current = $parent
        }
    }

    throw "Repository root non trovata. Eseguire lo script dentro il repository continuum-simulator oppure copiarlo al suo interno."
}

function Resolve-RepoPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }

    return [System.IO.Path]::GetFullPath((Join-Path $script:RepoRoot $Path))
}

function Invoke-External {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Command,

        [string[]]$Arguments = @()
    )

    Write-Host ("`n> {0} {1}" -f $Command, ($Arguments -join " ")) -ForegroundColor Cyan

    # Windows PowerShell 5.1 può trasformare righe stderr di un programma nativo
    # in ErrorRecord e, con $ErrorActionPreference="Stop", interrompere lo script
    # anche quando il processo termina con exit code 0. Per i programmi nativi
    # consideriamo quindi autorevole $LASTEXITCODE.
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & $Command @Arguments
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    if ($exitCode -ne 0) {
        throw "Comando fallito con exit code $exitCode`: $Command $($Arguments -join ' ')"
    }
}

function Get-ExperimentWorkers {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Yaml
    )

    $match = [regex]::Match(
        $Yaml,
        '(?ms)^cloud:\s*\r?\n(?:(?:[ \t]+[^\r\n]*\r?\n)*?)[ \t]+workers:\s*(\d+)'
    )

    if (-not $match.Success) {
        throw "Impossibile trovare cloud.workers nella configurazione sperimentale."
    }

    return [int]$match.Groups[1].Value
}

function Set-ExperimentWorkers {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Yaml,

        [Parameter(Mandatory = $true)]
        [int]$WorkerCount
    )

    $regex = [regex]'(?ms)(^cloud:\s*\r?\n(?:(?:[ \t]+[^\r\n]*\r?\n)*?)[ \t]+workers:\s*)\d+'

    if (-not $regex.IsMatch($Yaml)) {
        throw "Impossibile trovare cloud.workers nella configurazione sperimentale."
    }

    return $regex.Replace($Yaml, ('${1}' + $WorkerCount), 1)
}

function Get-ExperimentName {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Yaml
    )

    $match = [regex]::Match(
        $Yaml,
        '(?ms)^experiment:\s*\r?\n[ \t]+name:\s*([^\r\n#]+)'
    )

    if (-not $match.Success) {
        return "unknown"
    }

    return $match.Groups[1].Value.Trim().Trim('"').Trim("'")
}

function Get-RunContainerState {
    param([string]$Name)
    $inspection = (& docker inspect --format '{{json .}}' $Name 2>$null) -join "`n"
    if ($LASTEXITCODE -ne 0) { throw "Container $Name non trovato." }
    $container = $inspection | ConvertFrom-Json
    if ($container.RestartCount -ne 0 -or $container.State.OOMKilled) {
        throw "Lifecycle non valido per $Name`: restart=$($container.RestartCount) OOM=$($container.State.OOMKilled)"
    }
    return $container.State
}

function Wait-RunContainerHealthy {
    param([string]$Name, [int]$WaitSeconds = 300)
    $deadline = (Get-Date).AddSeconds($WaitSeconds)
    while ((Get-Date) -lt $deadline) {
        $state = Get-RunContainerState -Name $Name
        if ($state.Status -ne "running" -or $state.Health.Status -eq "unhealthy") {
            throw "$Name non pronto: state=$($state.Status) health=$($state.Health.Status)"
        }
        if ($state.Health.Status -eq "healthy") { return }
        Start-Sleep -Seconds 2
    }
    throw "Timeout readiness di $Name."
}

function Wait-CloudWorkerGroup {
    param([int]$ExpectedWorkers, [int]$WaitSeconds = 300)
    $deadline = (Get-Date).AddSeconds($WaitSeconds)
    while ((Get-Date) -lt $deadline) {
        $groupState = (& docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
            --bootstrap-server kafka:29092 --describe --group cloud-workers --state 2>$null) -join "`n"
        if ($LASTEXITCODE -eq 0) {
            foreach ($line in ($groupState -split "`n")) {
                $fields = $line.Trim() -split '\s+'
                if ($fields.Count -ge 3 -and $fields[0] -eq "cloud-workers" -and
                    $fields[-2] -eq "Stable" -and $fields[-1] -eq [string]$ExpectedWorkers) { return }
            }
        }
        Start-Sleep -Seconds 2
    }
    throw "Consumer group cloud-workers non Stable con $ExpectedWorkers membri."
}

function Get-PartitionCoordinatorStatus {
    $state = Get-RunContainerState -Name "partition-coordinator"
    if ($state.Status -ne "running" -or $state.Health.Status -ne "healthy") {
        throw "partition-coordinator non running/healthy."
    }
    $response = (& docker exec partition-coordinator wget -q -T 5 -O - http://localhost:8081/status) -join "`n"
    if ($LASTEXITCODE -ne 0) { throw "GET partition-coordinator/status fallita." }
    $status = $response | ConvertFrom-Json
    if ($status.complete -isnot [bool] -or $status.failed -isnot [bool] -or $status.failed) {
        throw "partition-coordinator status non valido o failed: $response"
    }
    return $status
}

function Test-WorkloadContainersCompleted {
    param([string[]]$Names)
    $complete = $true
    foreach ($name in $Names) {
        $state = Get-RunContainerState -Name $name
        if ($state.Status -eq "exited" -and $state.ExitCode -eq 0) { continue }
        if ($state.Status -ne "running") {
            throw "$name terminato in stato $($state.Status), exit=$($state.ExitCode)."
        }
        $complete = $false
    }
    return $complete
}

function Start-MetricsCollector {
    param(
        [Parameter(Mandatory = $true)]
        [string]$DockerStatsPath,

        [Parameter(Mandatory = $true)]
        [string]$KafkaLagPath,

        [Parameter(Mandatory = $true)]
        [string]$StopFile,

        [Parameter(Mandatory = $true)]
        [int]$IntervalSeconds
    )

    return Start-Job -ArgumentList @(
        $DockerStatsPath,
        $KafkaLagPath,
        $StopFile,
        $IntervalSeconds
    ) -ScriptBlock {
        param(
            $DockerStatsPath,
            $KafkaLagPath,
            $StopFile,
            $IntervalSeconds
        )

        $ErrorActionPreference = "Continue"

        $nextSampleAt = Get-Date

        while (-not (Test-Path $StopFile)) {
            $now = Get-Date
            if ($now -lt $nextSampleAt) {
                $sleepMs = [int][math]::Ceiling(($nextSampleAt - $now).TotalMilliseconds)
                if ($sleepMs -gt 0) {
                    Start-Sleep -Milliseconds $sleepMs
                }
            }

            if (Test-Path $StopFile) {
                break
            }

            $sampleStartedAt = Get-Date
            $timestamp = $sampleStartedAt.ToUniversalTime().ToString("o")

            try {
                $statsLines = & docker stats --no-stream --format "{{json .}}" 2>$null

                foreach ($line in $statsLines) {
                    if ([string]::IsNullOrWhiteSpace($line)) {
                        continue
                    }

                    try {
                        $stats = $line | ConvertFrom-Json
                        [ordered]@{
                            timestamp = $timestamp
                            stats     = $stats
                        } |
                            ConvertTo-Json -Compress -Depth 6 |
                            Add-Content -Path $DockerStatsPath -Encoding utf8
                    }
                    catch {
                        [ordered]@{
                            timestamp = $timestamp
                            raw       = $line
                        } |
                            ConvertTo-Json -Compress |
                            Add-Content -Path $DockerStatsPath -Encoding utf8
                    }
                }
            }
            catch {
                [ordered]@{
                    timestamp = $timestamp
                    error     = $_.Exception.Message
                } |
                    ConvertTo-Json -Compress |
                    Add-Content -Path $DockerStatsPath -Encoding utf8
            }

            try {
                Add-Content -Path $KafkaLagPath -Value "===== $timestamp =====" -Encoding utf8

                foreach ($group in @("cloud-workers", "global-aggregator")) {
                    Add-Content -Path $KafkaLagPath -Value "--- group: $group ---" -Encoding utf8

                    $lag = & docker exec kafka `
                        /opt/kafka/bin/kafka-consumer-groups.sh `
                        --bootstrap-server kafka:29092 `
                        --group $group `
                        --describe 2>&1

                    if ($null -ne $lag) {
                        $lag | Add-Content -Path $KafkaLagPath -Encoding utf8
                    }
                }

                Add-Content -Path $KafkaLagPath -Value "" -Encoding utf8
            }
            catch {
                Add-Content -Path $KafkaLagPath -Value "collector error: $($_.Exception.Message)" -Encoding utf8
            }

            # La prossima scadenza è calcolata rispetto all'inizio del campione,
            # così il tempo speso a interrogare Docker/Kafka non viene sommato
            # artificialmente all'intervallo richiesto.
            $nextSampleAt = $sampleStartedAt.AddSeconds($IntervalSeconds)
        }
    }
}

function Get-ActualReplayStartAt {
    param(
        [string]$ContainerName = "simulator-edge-0"
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"

        $envLines = & docker inspect `
            --format "{{range .Config.Env}}{{println .}}{{end}}" `
            $ContainerName 2>$null

        if ($LASTEXITCODE -ne 0) {
            throw "Impossibile leggere REPLAY_START_AT dal container $ContainerName."
        }
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    $replayLine = @(
        $envLines |
            Where-Object { $_ -match '^REPLAY_START_AT=' } |
            Select-Object -First 1
    )

    if ($replayLine.Count -eq 0) {
        throw "Variabile REPLAY_START_AT non trovata nel container $ContainerName."
    }

    $value = ($replayLine[0] -replace '^REPLAY_START_AT=', '').Trim()

    try {
        return [DateTimeOffset]::Parse(
            $value,
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::RoundtripKind
        ).UtcDateTime
    }
    catch {
        throw "REPLAY_START_AT non parsabile: '$value'."
    }
}


function Stop-MetricsCollector {
    param(
        [System.Management.Automation.Job]$Job,
        [string]$StopFile
    )

    if ($null -eq $Job) {
        return
    }

    New-Item -ItemType File -Path $StopFile -Force | Out-Null

    Wait-Job -Job $Job -Timeout 15 | Out-Null

    if ($Job.State -eq "Running") {
        Stop-Job -Job $Job
    }

    Receive-Job -Job $Job -ErrorAction SilentlyContinue | Out-Null
    Remove-Job -Job $Job -Force -ErrorAction SilentlyContinue
}

function Collect-RunArtifacts {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ComposePath,

        [Parameter(Mandatory = $true)]
        [string]$RawDir
    )

    New-Item -ItemType Directory -Path $RawDir -Force | Out-Null

    try {
        & docker compose -f $ComposePath --profile replay ps -a --format json 2>&1 |
            Set-Content -Path (Join-Path $RawDir "compose-ps.json") -Encoding utf8
    }
    catch {
        $_.Exception.Message |
            Set-Content -Path (Join-Path $RawDir "compose-ps-error.txt") -Encoding utf8
    }

    try {
        & docker compose -f $ComposePath --profile replay logs --no-color --timestamps 2>&1 |
            Set-Content -Path (Join-Path $RawDir "compose.log") -Encoding utf8
    }
    catch {
        $_.Exception.Message |
            Set-Content -Path (Join-Path $RawDir "compose-log-error.txt") -Encoding utf8
    }

    try {
        & docker logs --timestamps global-aggregator 2>&1 |
            Set-Content -Path (Join-Path $RawDir "global-aggregator.log") -Encoding utf8
    }
    catch {
        $_.Exception.Message |
            Set-Content -Path (Join-Path $RawDir "global-aggregator-log-error.txt") -Encoding utf8
    }

    # Query esplicite: --all-groups può non riportare un consumer group ormai inattivo.
    $finalGroupsPath = Join-Path $RawDir "kafka-consumer-groups-final.txt"
    Remove-Item $finalGroupsPath -Force -ErrorAction SilentlyContinue

    foreach ($group in @("cloud-workers", "global-aggregator")) {
        Add-Content -Path $finalGroupsPath -Value "===== group: $group =====" -Encoding utf8

        try {
            $previousErrorActionPreference = $ErrorActionPreference
            try {
                $ErrorActionPreference = "Continue"

                & docker exec kafka `
                    /opt/kafka/bin/kafka-consumer-groups.sh `
                    --bootstrap-server kafka:29092 `
                    --group $group `
                    --describe 2>&1 |
                    Add-Content -Path $finalGroupsPath -Encoding utf8
            }
            finally {
                $ErrorActionPreference = $previousErrorActionPreference
            }
        }
        catch {
            Add-Content `
                -Path $finalGroupsPath `
                -Value "collector error: $($_.Exception.Message)" `
                -Encoding utf8
        }

        Add-Content -Path $finalGroupsPath -Value "" -Encoding utf8
    }
}


function Get-ComposeLogRecord {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Line
    )

    $match = [regex]::Match($Line, '^(?<service>[^|]+)\|\s*(?<message>.*)$')
    if (-not $match.Success) {
        return $null
    }

    $service = $match.Groups["service"].Value.Trim()
    $message = $match.Groups["message"].Value.Trim()

    # docker compose logs --timestamps aggiunge il timestamp subito dopo il separatore.
    $message = [regex]::Replace(
        $message,
        '^\d{4}-\d{2}-\d{2}T[^\s]+\s+',
        ''
    )

    return [pscustomobject]@{
        Service = $service
        Message = $message
    }
}

function Convert-ToDoubleInvariant {
    param(
        [AllowNull()]
        [string]$Value
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $null
    }

    $culture = [System.Globalization.CultureInfo]::InvariantCulture
    return [double]::Parse(
        $Value.Trim(),
        [System.Globalization.NumberStyles]::Float,
        $culture
    )
}

function Convert-MemoryToMiB {
    param(
        [AllowNull()]
        [string]$Value
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $null
    }

    $match = [regex]::Match(
        $Value.Trim(),
        '^([0-9]+(?:\.[0-9]+)?)\s*([KMGT]?i?B)$',
        [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
    )

    if (-not $match.Success) {
        return $null
    }

    $number = Convert-ToDoubleInvariant $match.Groups[1].Value
    $unit = $match.Groups[2].Value.ToUpperInvariant()

    switch ($unit) {
        "B"   { return [math]::Round($number / 1MB, 6) }
        "KB"  { return [math]::Round(($number * 1000.0) / 1MB, 6) }
        "KIB" { return [math]::Round($number / 1024.0, 6) }
        "MB"  { return [math]::Round(($number * 1000000.0) / 1MB, 6) }
        "MIB" { return [math]::Round($number, 6) }
        "GB"  { return [math]::Round(($number * 1000000000.0) / 1MB, 6) }
        "GIB" { return [math]::Round($number * 1024.0, 6) }
        "TB"  { return [math]::Round(($number * 1000000000000.0) / 1MB, 6) }
        "TIB" { return [math]::Round($number * 1048576.0, 6) }
        default { return $null }
    }
}

function Export-SimulatorStatsCsv {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ComposeLogPath,

        [Parameter(Mandatory = $true)]
        [string]$OutputPath,

        [Parameter(Mandatory = $true)]
        [object]$ExpectedEdges
    )

    $prefix = "SIMULATOR_STATS "
    $rows = @{}

    foreach ($line in Get-Content -Path $ComposeLogPath) {
        $record = Get-ComposeLogRecord $line
        if ($null -eq $record) { continue }
        $msg = $record.Message.Trim()
        if (-not $msg.StartsWith($prefix)) { continue }

        $json = $msg.Substring($prefix.Length).Trim()
        $stats = $json | ConvertFrom-Json

        $edgeID = $stats.edge_id
        if ([string]::IsNullOrWhiteSpace($edgeID)) {
            throw "SIMULATOR_STATS: riga senza edge_id nel log compose."
        }
        if ($rows.ContainsKey($edgeID)) {
            throw "SIMULATOR_STATS: edge_id duplicato '$edgeID' nel log compose."
        }

        $rows[$edgeID] = $stats
    }

    $expectedIDs = @()
    $expectedCount = 0
    if ($ExpectedEdges -is [int]) {
        $expectedCount = [int]$ExpectedEdges
    }
    else {
        $expectedIDs = @($ExpectedEdges | ForEach-Object { [string]$_ })
        $expectedCount = $expectedIDs.Count
    }

    if ($rows.Count -ne $expectedCount) {
        throw ("SIMULATOR_STATS: attese $expectedCount righe, trovate $($rows.Count)." +
            " Edge trovati: $($rows.Keys -join ', ').")
    }

    if ($expectedIDs.Count -gt 0) {
        foreach ($expectedID in $expectedIDs) {
            if (-not $rows.ContainsKey($expectedID)) {
                throw "SIMULATOR_STATS: statistica mancante per l'edge atteso '$expectedID'."
            }
        }
        foreach ($foundID in $rows.Keys) {
            if ($foundID -notin $expectedIDs) {
                throw "SIMULATOR_STATS: statistica per edge inatteso '$foundID'."
            }
        }
    }

    $requiredFields = @(
        "edge_id", "status", "offered_events", "telemetry_enqueued",
        "telemetry_locally_dropped", "queue_capacity", "mqtt_publish_attempts",
        "mqtt_publish_errors", "avg_scheduling_lag_ms", "max_scheduling_lag_ms",
        "offer_duration_s", "drain_duration_s", "throughput_eps",
        "eos_successes", "eos_failures"
    )

    foreach ($entry in $rows.GetEnumerator()) {
        foreach ($field in $requiredFields) {
            $prop = $entry.Value.PSObject.Properties[$field]
            if ($null -eq $prop -or $null -eq $prop.Value) {
                throw "SIMULATOR_STATS: campo obbligatorio '$field' mancante o nullo per edge '$($entry.Key)'."
            }
        }
    }

    $outputRows = @(
        $rows.Values |
            Select-Object $requiredFields |
            Sort-Object {
                if ($_.edge_id -match '^edge-(\d+)$') { [int]$Matches[1] } else { 999999 }
            }
    )

    if ($outputRows.Count -gt 0) {
        $outputRows | Export-Csv -Path $OutputPath -NoTypeInformation -Encoding utf8
    }

    return $outputRows
}

function Export-EdgeStatsCsv {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ComposeLogPath,

        [Parameter(Mandatory = $true)]
        [string]$OutputPath,

        [Parameter(Mandatory = $true)]
        [object]$ExpectedEdges
    )

    $prefix = "EDGE_STATS "
    $rows = @{}

    foreach ($line in Get-Content -Path $ComposeLogPath) {
        $record = Get-ComposeLogRecord $line
        if ($null -eq $record) { continue }
        $msg = $record.Message.Trim()
        if (-not $msg.StartsWith($prefix)) { continue }

        $json = $msg.Substring($prefix.Length).Trim()
        $stats = $json | ConvertFrom-Json

        $edgeID = $stats.edge_id
        if ([string]::IsNullOrWhiteSpace($edgeID)) {
            throw "EDGE_STATS: riga senza edge_id nel log compose."
        }
        if ($rows.ContainsKey($edgeID)) {
            throw "EDGE_STATS: edge_id duplicato '$edgeID' nel log compose."
        }

        $rows[$edgeID] = $stats
    }

    $expectedIDs = @()
    $expectedCount = 0
    if ($ExpectedEdges -is [int]) {
        $expectedCount = [int]$ExpectedEdges
    }
    else {
        $expectedIDs = @($ExpectedEdges | ForEach-Object { [string]$_ })
        $expectedCount = $expectedIDs.Count
    }

    if ($rows.Count -ne $expectedCount) {
        throw ("EDGE_STATS: attese $expectedCount righe, trovate $($rows.Count)." +
            " Edge trovati: $($rows.Keys -join ', ').")
    }

    if ($expectedIDs.Count -gt 0) {
        foreach ($expectedID in $expectedIDs) {
            if (-not $rows.ContainsKey($expectedID)) {
                throw "EDGE_STATS: statistica mancante per l'edge atteso '$expectedID'."
            }
        }
        foreach ($foundID in $rows.Keys) {
            if ($foundID -notin $expectedIDs) {
                throw "EDGE_STATS: statistica per edge inatteso '$foundID'."
            }
        }
    }

    $requiredFields = @(
        "edge_id", "telemetry_received", "ingress_queue_capacity",
        "max_ingress_queue_depth", "max_ingress_queue_utilization_pct",
        "ingress_accepted", "ingress_queue_dropped", "invalid_telemetry",
        "out_of_order_dropped", "post_eos_dropped", "processed",
        "aggregates_emitted", "end_of_replay_processed"
    )

    foreach ($entry in $rows.GetEnumerator()) {
        foreach ($field in $requiredFields) {
            $prop = $entry.Value.PSObject.Properties[$field]
            if ($null -eq $prop -or $null -eq $prop.Value) {
                throw "EDGE_STATS: campo obbligatorio '$field' mancante o nullo per edge '$($entry.Key)'."
            }
        }
    }

    $outputRows = @(
        $rows.Values |
            Select-Object $requiredFields |
            Sort-Object {
                if ($_.edge_id -match '^edge-(\d+)$') { [int]$Matches[1] } else { 999999 }
            }
    )

    if ($outputRows.Count -gt 0) {
        $outputRows | Export-Csv -Path $OutputPath -NoTypeInformation -Encoding utf8
    }

    return $outputRows
}

function Export-DockerStatsCsv {
    param(
        [Parameter(Mandatory = $true)]
        [string]$JsonlPath,

        [Parameter(Mandatory = $true)]
        [string]$SamplesOutputPath,

        [Parameter(Mandatory = $true)]
        [string]$SummaryOutputPath,

        [AllowNull()]
        [string]$MeasurementStartUtc,

        [AllowNull()]
        [string]$MeasurementEndUtc
    )

    $samples = @()

    if (-not (Test-Path $JsonlPath)) {
        return [pscustomobject]@{
            Samples = @()
            Summary = @()
        }
    }

    foreach ($line in Get-Content -Path $JsonlPath) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }

        try {
            $record = $line | ConvertFrom-Json
            if ($null -eq $record.stats) {
                continue
            }

            $cpu = $null
            if ($record.stats.CPUPerc -match '^([0-9]+(?:\.[0-9]+)?)%$') {
                $cpu = Convert-ToDoubleInvariant $Matches[1]
            }

            $memoryUsedMiB = $null
            if ($record.stats.MemUsage -match '^\s*([^/]+?)\s*/') {
                $memoryUsedMiB = Convert-MemoryToMiB $Matches[1].Trim()
            }

            $samples += [pscustomobject]@{
                timestamp_utc   = [string]$record.timestamp
                container       = [string]$record.stats.Name
                cpu_pct         = $cpu
                memory_used_mib = $memoryUsedMiB
                memory_usage    = [string]$record.stats.MemUsage
                net_io          = [string]$record.stats.NetIO
                block_io        = [string]$record.stats.BlockIO
                pids            = [string]$record.stats.PIDs
            }
        }
        catch {
            continue
        }
    }

    # Conserviamo i campioni grezzi completi solo nel raw.
    if ($samples.Count -gt 0) {
        $samples |
            Sort-Object timestamp_utc, container |
            Export-Csv -Path $SamplesOutputPath -NoTypeInformation -Encoding utf8
    }

    # Per le metriche sperimentali usiamo esclusivamente la finestra:
    # REPLAY_START_AT <= sample <= GLOBAL_REPLAY_COMPLETED.
    $measurementSamples = @($samples)

    if (-not [string]::IsNullOrWhiteSpace($MeasurementStartUtc)) {
        $startAt = [DateTimeOffset]::Parse(
            $MeasurementStartUtc,
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::RoundtripKind
        ).UtcDateTime

        $measurementSamples = @(
            $measurementSamples |
                Where-Object {
                    try {
                        $sampleAt = [DateTimeOffset]::Parse(
                            $_.timestamp_utc,
                            [System.Globalization.CultureInfo]::InvariantCulture,
                            [System.Globalization.DateTimeStyles]::RoundtripKind
                        ).UtcDateTime
                        $sampleAt -ge $startAt
                    }
                    catch {
                        $false
                    }
                }
        )
    }

    if (-not [string]::IsNullOrWhiteSpace($MeasurementEndUtc)) {
        $endAt = [DateTimeOffset]::Parse(
            $MeasurementEndUtc,
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::RoundtripKind
        ).UtcDateTime

        $measurementSamples = @(
            $measurementSamples |
                Where-Object {
                    try {
                        $sampleAt = [DateTimeOffset]::Parse(
                            $_.timestamp_utc,
                            [System.Globalization.CultureInfo]::InvariantCulture,
                            [System.Globalization.DateTimeStyles]::RoundtripKind
                        ).UtcDateTime
                        $sampleAt -le $endAt
                    }
                    catch {
                        $false
                    }
                }
        )
    }

    $summary = @()

    foreach ($group in ($measurementSamples | Group-Object container)) {
        $cpuValues = @($group.Group | Where-Object { $null -ne $_.cpu_pct } | ForEach-Object { [double]$_.cpu_pct })
        $memoryValues = @($group.Group | Where-Object { $null -ne $_.memory_used_mib } | ForEach-Object { [double]$_.memory_used_mib })

        $cpuAvg = $null
        $cpuMax = $null
        $memoryAvg = $null
        $memoryMax = $null

        if ($cpuValues.Count -gt 0) {
            $cpuAvg = [math]::Round(($cpuValues | Measure-Object -Average).Average, 6)
            $cpuMax = [math]::Round(($cpuValues | Measure-Object -Maximum).Maximum, 6)
        }

        if ($memoryValues.Count -gt 0) {
            $memoryAvg = [math]::Round(($memoryValues | Measure-Object -Average).Average, 6)
            $memoryMax = [math]::Round(($memoryValues | Measure-Object -Maximum).Maximum, 6)
        }

        $summary += [pscustomobject]@{
            container      = $group.Name
            samples        = $group.Count
            cpu_avg_pct    = $cpuAvg
            cpu_max_pct    = $cpuMax
            memory_avg_mib = $memoryAvg
            memory_max_mib = $memoryMax
        }
    }

    if ($summary.Count -gt 0) {
        $summary |
            Sort-Object container |
            Export-Csv -Path $SummaryOutputPath -NoTypeInformation -Encoding utf8
    }

    return [pscustomobject]@{
        Samples = $samples
        Summary = $summary
    }
}

function Parse-KafkaConsumerRows {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [switch]$WithTimestamps
    )

    $rows = @()
    if (-not (Test-Path $Path)) {
        return $rows
    }

    $timestamp = $null

    foreach ($line in Get-Content -Path $Path) {
        if ($WithTimestamps -and $line -match '^=====\s+(.+?)\s+=====$') {
            $timestamp = $Matches[1]
            continue
        }

        # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
        $match = [regex]::Match(
            $line,
            '^(?<group>\S+)\s+(?<topic>\S+)\s+(?<partition>\d+)\s+(?<current>-|\d+)\s+(?<end>\d+)\s+(?<lag>-|\d+)(?:\s+|$)'
        )

        if (-not $match.Success) {
            continue
        }

        if ($match.Groups["current"].Value -eq "-" -or $match.Groups["lag"].Value -eq "-") {
            continue
        }

        $rows += [pscustomobject]@{
            timestamp_utc  = $timestamp
            group          = $match.Groups["group"].Value
            topic          = $match.Groups["topic"].Value
            partition      = [int]$match.Groups["partition"].Value
            current_offset = [int64]$match.Groups["current"].Value
            log_end_offset = [int64]$match.Groups["end"].Value
            lag            = [int64]$match.Groups["lag"].Value
        }
    }

    return $rows
}

function Export-KafkaLagCsv {
    param(
        [Parameter(Mandatory = $true)]
        [string]$KafkaLagPath,

        [Parameter(Mandatory = $true)]
        [string]$OutputPath,

        [AllowNull()]
        [string]$MeasurementStartUtc,

        [AllowNull()]
        [string]$MeasurementEndUtc
    )

    $rows = @(Parse-KafkaConsumerRows -Path $KafkaLagPath -WithTimestamps)

    if (-not [string]::IsNullOrWhiteSpace($MeasurementStartUtc)) {
        $startAt = [DateTimeOffset]::Parse(
            $MeasurementStartUtc,
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::RoundtripKind
        ).UtcDateTime

        $rows = @(
            $rows |
                Where-Object {
                    try {
                        $rowAt = [DateTimeOffset]::Parse(
                            $_.timestamp_utc,
                            [System.Globalization.CultureInfo]::InvariantCulture,
                            [System.Globalization.DateTimeStyles]::RoundtripKind
                        ).UtcDateTime
                        $rowAt -ge $startAt
                    }
                    catch {
                        $false
                    }
                }
        )
    }

    if (-not [string]::IsNullOrWhiteSpace($MeasurementEndUtc)) {
        $endAt = [DateTimeOffset]::Parse(
            $MeasurementEndUtc,
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::RoundtripKind
        ).UtcDateTime

        $rows = @(
            $rows |
                Where-Object {
                    try {
                        $rowAt = [DateTimeOffset]::Parse(
                            $_.timestamp_utc,
                            [System.Globalization.CultureInfo]::InvariantCulture,
                            [System.Globalization.DateTimeStyles]::RoundtripKind
                        ).UtcDateTime
                        $rowAt -le $endAt
                    }
                    catch {
                        $false
                    }
                }
        )
    }

    if ($rows.Count -gt 0) {
        $rows |
            Sort-Object timestamp_utc, group, topic, partition |
            Export-Csv -Path $OutputPath -NoTypeInformation -Encoding utf8
    }

    return $rows
}

function Get-MaxTotalLag {
    param(
        [object[]]$Rows,
        [string]$Group
    )

    $groupRows = @($Rows | Where-Object { $_.group -eq $Group -and -not [string]::IsNullOrWhiteSpace($_.timestamp_utc) })
    if ($groupRows.Count -eq 0) {
        return $null
    }

    $totals = foreach ($timestampGroup in ($groupRows | Group-Object timestamp_utc)) {
        ($timestampGroup.Group | Measure-Object lag -Sum).Sum
    }

    return [int64](($totals | Measure-Object -Maximum).Maximum)
}

function Get-FinalGroupLag {
    param(
        [object[]]$Rows,
        [string]$Group
    )

    $groupRows = @($Rows | Where-Object { $_.group -eq $Group })
    if ($groupRows.Count -eq 0) {
        return $null
    }

    return [int64](($groupRows | Measure-Object lag -Sum).Sum)
}

function Get-FinalTopicRecords {
    param(
        [object[]]$Rows,
        [string]$Topic
    )

    $topicRows = @($Rows | Where-Object { $_.topic -eq $Topic })
    if ($topicRows.Count -eq 0) {
        return $null
    }

    return [int64](($topicRows | Measure-Object log_end_offset -Sum).Sum)
}

function Export-RunSummaryCsv {
    param(
        [Parameter(Mandatory = $true)]
        [string]$OutputPath,

        [Parameter(Mandatory = $true)]
        [string]$RunId,

        [Parameter(Mandatory = $true)]
        [string]$Status,

        [Parameter(Mandatory = $true)]
        [int]$Workers,

        [AllowNull()]
        [Nullable[double]]$ReplayElapsedSeconds,

        [Parameter(Mandatory = $true)]
        [double]$ElapsedSeconds,

        [object[]]$SimulatorRows,

        [object[]]$EdgeRows,

        [object[]]$KafkaRows,

        [object[]]$FinalKafkaRows
    )

    $simOffered = ($SimulatorRows | Measure-Object offered_events -Sum).Sum
    $simEnqueued = ($SimulatorRows | Measure-Object telemetry_enqueued -Sum).Sum
    $simDropped = ($SimulatorRows | Measure-Object telemetry_locally_dropped -Sum).Sum
    $simMqttErrors = ($SimulatorRows | Measure-Object mqtt_publish_errors -Sum).Sum
    $simEosFailures = ($SimulatorRows | Measure-Object eos_failures -Sum).Sum
    $simMaxSchedulingLag = ($SimulatorRows | Measure-Object max_scheduling_lag_ms -Maximum).Maximum

    $edgeReceived = ($EdgeRows | Measure-Object telemetry_received -Sum).Sum
    $edgeProcessed = ($EdgeRows | Measure-Object processed -Sum).Sum
    $edgeQueueDropped = ($EdgeRows | Measure-Object ingress_queue_dropped -Sum).Sum
    $edgeInvalid = ($EdgeRows | Measure-Object invalid_telemetry -Sum).Sum
    $edgeOutOfOrder = ($EdgeRows | Measure-Object out_of_order_dropped -Sum).Sum
    $edgePostEos = ($EdgeRows | Measure-Object post_eos_dropped -Sum).Sum
    $edgeAggregates = ($EdgeRows | Measure-Object aggregates_emitted -Sum).Sum
    $edgeMaxQueueUtil = ($EdgeRows | Measure-Object max_ingress_queue_utilization_pct -Maximum).Maximum

    $summary = [pscustomobject]@{
        run_id                              = $RunId
        status                              = $Status
        workers                             = $Workers
        replay_elapsed_seconds              = $ReplayElapsedSeconds
        elapsed_seconds                     = $ElapsedSeconds

        simulator_offered_total             = $simOffered
        simulator_enqueued_total            = $simEnqueued
        simulator_locally_dropped_total     = $simDropped
        simulator_mqtt_errors_total         = $simMqttErrors
        simulator_eos_failures_total        = $simEosFailures
        simulator_max_scheduling_lag_ms     = $simMaxSchedulingLag

        edge_received_total                 = $edgeReceived
        edge_processed_total                = $edgeProcessed
        edge_ingress_queue_dropped_total    = $edgeQueueDropped
        edge_invalid_total                  = $edgeInvalid
        edge_out_of_order_dropped_total     = $edgeOutOfOrder
        edge_post_eos_dropped_total         = $edgePostEos
        edge_aggregates_emitted_total       = $edgeAggregates
        edge_max_queue_utilization_pct      = $edgeMaxQueueUtil

        cloud_workers_max_total_lag         = Get-MaxTotalLag -Rows $KafkaRows -Group "cloud-workers"
        global_aggregator_max_total_lag     = Get-MaxTotalLag -Rows $KafkaRows -Group "global-aggregator"
        cloud_workers_final_lag             = Get-FinalGroupLag -Rows $FinalKafkaRows -Group "cloud-workers"
        global_aggregator_final_lag         = Get-FinalGroupLag -Rows $FinalKafkaRows -Group "global-aggregator"

        edge_aggregates_topic_records       = Get-FinalTopicRecords -Rows $FinalKafkaRows -Topic "edge-aggregates"
        cloud_partition_aggregates_topic_records = Get-FinalTopicRecords -Rows $FinalKafkaRows -Topic "cloud-partition-aggregates"
    }

    $summary | Export-Csv -Path $OutputPath -NoTypeInformation -Encoding utf8
    return $summary
}

function Export-ExperimentCsvArtifacts {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ArtifactDir,

        [Parameter(Mandatory = $true)]
        [string]$RawDir,

        [Parameter(Mandatory = $true)]
        [string]$RunId,

        [Parameter(Mandatory = $true)]
        [string]$Status,

        [Parameter(Mandatory = $true)]
        [int]$Workers,

        [AllowNull()]
        [Nullable[double]]$ReplayElapsedSeconds,

        [Parameter(Mandatory = $true)]
        [double]$ElapsedSeconds,

        [AllowNull()]
        [string]$MeasurementStartUtc,

        [AllowNull()]
        [string]$MeasurementEndUtc,

        [Parameter(Mandatory = $true)]
        [object]$ExpectedEdges
    )

    $composeLogPath = Join-Path $RawDir "compose.log"
    $dockerStatsPath = Join-Path $RawDir "docker-stats.jsonl"
    $kafkaLagPath = Join-Path $RawDir "kafka-lag.log"
    $finalKafkaPath = Join-Path $RawDir "kafka-consumer-groups-final.txt"

    $simulatorRows = @(
        Export-SimulatorStatsCsv `
            -ComposeLogPath $composeLogPath `
            -OutputPath (Join-Path $ArtifactDir "simulator-stats.csv") `
            -ExpectedEdges $ExpectedEdges
    )

    $edgeRows = @(
        Export-EdgeStatsCsv `
            -ComposeLogPath $composeLogPath `
            -OutputPath (Join-Path $ArtifactDir "edge-stats.csv") `
            -ExpectedEdges $ExpectedEdges
    )

    # I campioni puntuali Docker restano tra i raw.
    $dockerStats = Export-DockerStatsCsv `
        -JsonlPath $dockerStatsPath `
        -SamplesOutputPath (Join-Path $RawDir "container-stats-samples.csv") `
        -SummaryOutputPath (Join-Path $ArtifactDir "container-stats.csv") `
        -MeasurementStartUtc $MeasurementStartUtc `
        -MeasurementEndUtc $MeasurementEndUtc

    $kafkaRows = @(
        Export-KafkaLagCsv `
            -KafkaLagPath $kafkaLagPath `
            -OutputPath (Join-Path $ArtifactDir "kafka-lag.csv") `
            -MeasurementStartUtc $MeasurementStartUtc `
            -MeasurementEndUtc $MeasurementEndUtc
    )

    $finalKafkaRows = @(
        Parse-KafkaConsumerRows -Path $finalKafkaPath
    )

    $runSummary = Export-RunSummaryCsv `
        -OutputPath (Join-Path $ArtifactDir "run-summary.csv") `
        -RunId $RunId `
        -Status $Status `
        -Workers $Workers `
        -ReplayElapsedSeconds $ReplayElapsedSeconds `
        -ElapsedSeconds $ElapsedSeconds `
        -SimulatorRows $simulatorRows `
        -EdgeRows $edgeRows `
        -KafkaRows $kafkaRows `
        -FinalKafkaRows $finalKafkaRows

    return [pscustomobject]@{
        SimulatorRows  = $simulatorRows
        EdgeRows       = $edgeRows
        DockerStats    = $dockerStats
        KafkaRows      = $kafkaRows
        FinalKafkaRows = $finalKafkaRows
        RunSummary     = $runSummary
    }
}


$RepoRoot = Find-RepoRoot
Set-Location $RepoRoot

$ExperimentPath = Resolve-RepoPath $Experiment
if (-not (Test-Path $ExperimentPath)) {
    throw "Configurazione esperimento non trovata: $ExperimentPath"
}

$TopologyPath = Join-Path $RepoRoot "dataset/output/kmeans_topology.csv"
if (-not (Test-Path $TopologyPath)) {
    throw "Topologia non trovata: $TopologyPath"
}

$ReplayDir = Join-Path $RepoRoot "dataset/derived/replay_by_edge"
if (-not (Test-Path $ReplayDir)) {
    throw "Replay shard non trovati: $ReplayDir. Generarli prima tramite notebook."
}

$ReplayFiles = @(Get-ChildItem -Path $ReplayDir -Filter "edge-*.csv" -File)
if ($ReplayFiles.Count -eq 0) {
    throw "Nessun replay shard edge-*.csv trovato in $ReplayDir"
}

foreach ($command in @("go", "docker", "git")) {
    if ($null -eq (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "Comando richiesto non trovato nel PATH: $command"
    }
}

Write-Host "`nVerifica Docker..." -ForegroundColor Cyan

# Docker Desktop/WSL può emettere warning innocui su stderr (per esempio sul
# supporto blkio). In Windows PowerShell 5.1 non devono essere scambiati per
# un fallimento: la verifica reale è l'exit code del processo.
$previousErrorActionPreference = $ErrorActionPreference
try {
    $ErrorActionPreference = "SilentlyContinue"
    & docker info *> $null
    $dockerInfoExitCode = $LASTEXITCODE
}
finally {
    $ErrorActionPreference = $previousErrorActionPreference
}

if ($dockerInfoExitCode -ne 0) {
    throw "Docker non è raggiungibile. Avviare Docker Desktop prima della run."
}

$SourceYaml = Get-Content -Path $ExperimentPath -Raw
$ConfiguredWorkers = Get-ExperimentWorkers $SourceYaml
$ExperimentName = Get-ExperimentName $SourceYaml
$EffectiveWorkers = if ($Workers -gt 0) { $Workers } else { $ConfiguredWorkers }

$ScenarioName = [System.IO.Path]::GetFileNameWithoutExtension($ExperimentPath)

if ([string]::IsNullOrWhiteSpace($ScenarioName)) {
    throw "Impossibile ricavare il nome dello scenario dal file YAML: $ExperimentPath"
}

if ($ScenarioName -notmatch '^[A-Za-z0-9._-]+$') {
    throw "Il nome del file YAML può contenere soltanto lettere, numeri, punto, underscore e trattino."
}

if ([string]::IsNullOrWhiteSpace($RunId)) {
    $RunId = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
}

if ($RunId -notmatch '^[A-Za-z0-9._-]+$') {
    throw "RunId può contenere soltanto lettere, numeri, punto, underscore e trattino."
}

$ScenarioDir = Join-Path $RepoRoot "artifacts/local-runs/$ScenarioName"
$ArtifactDir = Join-Path $ScenarioDir $RunId
$RawDir = Join-Path $ArtifactDir "raw"

if (Test-Path $ArtifactDir) {
    throw "La directory della run esiste già: $ArtifactDir"
}

New-Item -ItemType Directory -Path $RawDir -Force | Out-Null

$RunExperimentPath = Join-Path $RawDir "input-experiment.yaml"
if ($Workers -gt 0) {
    $RunYaml = Set-ExperimentWorkers -Yaml $SourceYaml -WorkerCount $Workers
}
else {
    $RunYaml = $SourceYaml
}
Set-Content -Path $RunExperimentPath -Value $RunYaml -Encoding utf8

$ComposePath = Join-Path $RepoRoot "deploy/compose/continuum.generated.yml"
$MetricsPath = Join-Path $RawDir "docker-stats.jsonl"
$KafkaLagPath = Join-Path $RawDir "kafka-lag.log"
$StopMetricsFile = Join-Path $RawDir ".stop-metrics"

$GitCommit = (& git rev-parse HEAD).Trim()
$GitStatus = (& git status --short) -join "`n"
$GoVersion = (& go version) -join " "
$DockerVersion = (& docker version --format "{{.Server.Version}}" 2>$null) -join " "

$RunStartedAt = (Get-Date).ToUniversalTime()
$SimulatorsLaunchedAt = $null
$ReplayStartedAt = $null
$WorkloadCompletedAt = $null
$RunFinishedAt = $null
$Status = "failed"
$Failure = $null
$MetricsJob = $null
$CoordinatorStarted = $false

Write-Host ""
Write-Host "=============================================" -ForegroundColor Green
Write-Host " Local continuum experiment" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green
Write-Host "Scenario:     $ScenarioName"
Write-Host "Run ID:       $RunId"
Write-Host "Experiment:   $ExperimentName"
Write-Host "Workers:      $EffectiveWorkers"
Write-Host "Sink:         log"
Write-Host "Artifacts:    $ArtifactDir"
Write-Host "Git commit:   $GitCommit"
Write-Host ""

try {
    if (-not $SkipBuild) {
        Write-Host "`nBuild immagini applicative..." -ForegroundColor Yellow

        $builds = @(
            @("deploy/docker/simulator.Dockerfile", "continuum-simulator:local"),
            @("deploy/docker/edge.Dockerfile", "continuum-edge:local"),
            @("deploy/docker/cloud-worker.Dockerfile", "continuum-cloud-worker:local"),
            @("deploy/docker/global-aggregator.Dockerfile", "continuum-global-aggregator:local")
        )

        foreach ($build in $builds) {
            Invoke-External "docker" @(
                "build",
                "-f", $build[0],
                "-t", $build[1],
                "."
            )
        }
    }
    else {
        Write-Host "`nBuild immagini saltato (-SkipBuild)." -ForegroundColor Yellow
    }

    # Prima generazione: serve un Compose coerente con il numero di Worker scelto.
    Invoke-External "go" @(
        "run", "./cmd/deploygen",
        "-mode", "local",
        "-experiment", $RunExperimentPath
    )

    # Ogni run sperimentale parte da uno stack e da un Kafka volume puliti.
    # Evita che messaggi/topic/offset di una run precedente contaminino la successiva.
    Invoke-External "docker" @(
        "compose",
        "-f", $ComposePath,
        "--profile", "replay",
        "down",
        "-v",
        "--remove-orphans"
    )

    Write-Host "`nAvvio infrastruttura senza Simulator..." -ForegroundColor Yellow
    $CoordinatorStarted = $true
    Invoke-External "docker" @(
        "compose",
        "-f", $ComposePath,
        "up",
        "-d"
    )

    # The listener is healthy before /start; only activation enables empty EOS.
    Wait-CloudWorkerGroup -ExpectedWorkers $EffectiveWorkers
    Wait-RunContainerHealthy -Name "partition-coordinator"
    $services = @(& docker compose -f $ComposePath --profile replay config --services)
    if ($LASTEXITCODE -ne 0) { throw "Impossibile leggere i servizi Compose." }
    $edgeServices = @($services | Where-Object { $_ -match '^edge-\d+$' })
    $simulatorServices = @($services | Where-Object { $_ -match '^simulator-edge-\d+$' })
    if ($edgeServices.Count -eq 0 -or $edgeServices.Count -ne $simulatorServices.Count) {
        throw "Servizi Edge/Simulator mancanti o non allineati."
    }
    foreach ($edgeService in $edgeServices) { Wait-RunContainerHealthy -Name $edgeService }
    Invoke-External "docker" @("exec", "partition-coordinator", "wget", "-q", "-T", "10", "-O", "-", "--post-data=", "http://localhost:8081/start")

    # Rigenerazione intenzionale dopo l'avvio dell'infrastruttura:
    # deploygen calcola un nuovo REPLAY_START_AT usando start_lead_time.
    # In questo modo il tempo speso nel build e nel bootstrap non consuma il lead time.
    Write-Host "`nAggiornamento REPLAY_START_AT..." -ForegroundColor Yellow
    Invoke-External "go" @(
        "run", "./cmd/deploygen",
        "-mode", "local",
        "-experiment", $RunExperimentPath
    )

    Copy-Item -Path $ComposePath -Destination (Join-Path $RawDir "continuum.generated.yml") -Force

    $EffectiveConfigPath = Join-Path $RepoRoot "artifacts/experiments/$ExperimentName/effective-config.yaml"
    if (Test-Path $EffectiveConfigPath) {
        Copy-Item -Path $EffectiveConfigPath -Destination (Join-Path $ArtifactDir "effective-config.yaml") -Force
    }

    $MetricsJob = Start-MetricsCollector `
        -DockerStatsPath $MetricsPath `
        -KafkaLagPath $KafkaLagPath `
        -StopFile $StopMetricsFile `
        -IntervalSeconds $MetricsIntervalSeconds

    $SimulatorsLaunchedAt = (Get-Date).ToUniversalTime()

    Write-Host "`nAvvio dei Simulator..." -ForegroundColor Yellow
    Invoke-External "docker" (@(
        "compose",
        "-f", $ComposePath,
        "--profile", "replay",
        "up",
        "-d",
        "--no-deps"
    ) + $simulatorServices)

    # REPLAY_START_AT è il vero inizio del workload. Il compose up dei Simulator
    # avviene prima e include intenzionalmente il lead time.
    $ReplayStartedAt = Get-ActualReplayStartAt -ContainerName "simulator-edge-0"

    Write-Host "Workload effettivo da: $($ReplayStartedAt.ToString('o'))" -ForegroundColor DarkGray
    Write-Host "`nReplay avviato. Attendo GLOBAL_REPLAY_COMPLETED..." -ForegroundColor Yellow

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $globalExited = $false

    while ((Get-Date) -lt $deadline) {
        $coordinatorStatus = Get-PartitionCoordinatorStatus
        $workloadComplete = Test-WorkloadContainersCompleted -Names ($edgeServices + $simulatorServices)
        $state = (& docker inspect --format "{{.State.Status}}" global-aggregator 2>$null)
        if ($LASTEXITCODE -ne 0) {
            throw "Container global-aggregator non trovato durante la run."
        }

        $state = ($state | Out-String).Trim()

        if ($state -eq "exited") {
            $globalState = Get-RunContainerState -Name "global-aggregator"
            if ($globalState.ExitCode -ne 0) { throw "Global Aggregator terminato con exit code $($globalState.ExitCode)." }
            if ($workloadComplete -and $coordinatorStatus.complete) {
                $globalExited = $true
                break
            }
        }
        elseif ($state -ne "running") {
            throw "Global Aggregator in stato non valido: $state"
        }

        Start-Sleep -Seconds 2
    }

    if (-not $globalExited) {
        throw "Timeout di $TimeoutSeconds secondi: il Global Aggregator non ha terminato."
    }

    $GlobalExitCode = [int]((& docker inspect --format "{{.State.ExitCode}}" global-aggregator) | Out-String).Trim()
    $GlobalLogs = (& docker logs global-aggregator 2>&1) -join "`n"

    if ($GlobalExitCode -ne 0) {
        throw "Global Aggregator terminato con exit code $GlobalExitCode."
    }

    if ($GlobalLogs -notmatch 'GLOBAL_REPLAY_COMPLETED') {
        throw "Global Aggregator terminato senza GLOBAL_REPLAY_COMPLETED."
    }

    $WorkloadCompletedAt = (Get-Date).ToUniversalTime()
    $Status = "success"
    Write-Host "`nGLOBAL_REPLAY_COMPLETED rilevato." -ForegroundColor Green
}
catch {
    $Failure = $_.Exception.Message
    Write-Host "`nRUN FALLITA: $Failure" -ForegroundColor Red
}
finally {
    $RunFinishedAt = (Get-Date).ToUniversalTime()

    # A failed run must not keep emitting completion records, including when
    # -KeepContainers preserves the other containers for diagnosis.
    if ($CoordinatorStarted -and $Status -ne "success") {
        & docker stop --time 10 partition-coordinator 2>&1 | Out-Host
    }

    Stop-MetricsCollector -Job $MetricsJob -StopFile $StopMetricsFile
    Remove-Item $StopMetricsFile -Force -ErrorAction SilentlyContinue

    Collect-RunArtifacts -ComposePath $ComposePath -RawDir $RawDir

    $elapsedSeconds = [math]::Round(
        ($RunFinishedAt - $RunStartedAt).TotalSeconds,
        3
    )

    $MeasurementEndAt = if ($null -ne $WorkloadCompletedAt) {
        $WorkloadCompletedAt
    }
    else {
        $RunFinishedAt
    }

    $replayElapsedSeconds = $null
    if ($null -ne $ReplayStartedAt -and $MeasurementEndAt -ge $ReplayStartedAt) {
        $replayElapsedSeconds = [math]::Round(
            ($MeasurementEndAt - $ReplayStartedAt).TotalSeconds,
            3
        )
    }

    $MeasurementStartUtc = if ($null -ne $ReplayStartedAt) {
        $ReplayStartedAt.ToString("o")
    }
    else {
        $null
    }

    $MeasurementEndUtc = if ($null -ne $MeasurementEndAt) {
        $MeasurementEndAt.ToString("o")
    }
    else {
        $null
    }

    $PostprocessStatus = "success"
    $PostprocessError = $null

    try {
        Write-Host "`nGenerazione CSV sperimentali..." -ForegroundColor Yellow

        $ExpectedEdgeIDs = @(
            $ReplayFiles |
                ForEach-Object { $_.BaseName } |
                Sort-Object { if ($_ -match '^edge-(\d+)$') { [int]$Matches[1] } else { 999999 } }
        )

        $null = Export-ExperimentCsvArtifacts `
            -ArtifactDir $ArtifactDir `
            -RawDir $RawDir `
            -RunId $RunId `
            -Status $Status `
            -Workers $EffectiveWorkers `
            -ReplayElapsedSeconds $replayElapsedSeconds `
            -ElapsedSeconds $elapsedSeconds `
            -MeasurementStartUtc $MeasurementStartUtc `
            -MeasurementEndUtc $MeasurementEndUtc `
            -ExpectedEdges $ExpectedEdgeIDs
    }
    catch {
        $PostprocessStatus = "failed"
        $PostprocessError = $_.Exception.Message

        $PostprocessError |
            Set-Content -Path (Join-Path $RawDir "csv-export-error.txt") -Encoding utf8

        Write-Host "Generazione CSV fallita: $PostprocessError" -ForegroundColor DarkYellow
    }

    $artifactMetadata = [ordered]@{
        run_summary      = "run-summary.csv"
        simulator_stats  = "simulator-stats.csv"
        edge_stats       = "edge-stats.csv"
        container_stats  = "container-stats.csv"
        kafka_lag        = "kafka-lag.csv"
        effective_config = "effective-config.yaml"
    }

    # Se qualcosa va male teniamo automaticamente i raw per poter capire il motivo.
    $KeepRawForThisRun = $KeepRawArtifacts -or $Status -ne "success" -or $PostprocessStatus -ne "success"
    if ($KeepRawForThisRun) {
        $artifactMetadata.raw = "raw/"
    }

    $metadata = [ordered]@{
        scenario_name            = $ScenarioName
        run_id                   = $RunId
        status                   = $Status
        failure                  = $Failure
        experiment_name          = $ExperimentName
        experiment_source        = $Experiment
        workers                  = $EffectiveWorkers
        global_sink_type         = "log"
        git_commit_sha           = $GitCommit
        git_status_short         = $GitStatus
        go_version               = $GoVersion
        docker_server_version    = $DockerVersion
        metrics_interval_seconds = $MetricsIntervalSeconds
        timeout_seconds          = $TimeoutSeconds
        run_started_at           = $RunStartedAt.ToString("o")
        simulators_launched_at    = if ($null -ne $SimulatorsLaunchedAt) { $SimulatorsLaunchedAt.ToString("o") } else { $null }
        replay_started_at         = if ($null -ne $ReplayStartedAt) { $ReplayStartedAt.ToString("o") } else { $null }
        workload_completed_at     = if ($null -ne $WorkloadCompletedAt) { $WorkloadCompletedAt.ToString("o") } else { $null }
        run_finished_at           = $RunFinishedAt.ToString("o")
        measurement_start_at      = $MeasurementStartUtc
        measurement_end_at        = $MeasurementEndUtc
        elapsed_seconds           = $elapsedSeconds
        replay_elapsed_seconds    = $replayElapsedSeconds
        replay_shards_found      = $ReplayFiles.Count
        postprocess_status       = $PostprocessStatus
        postprocess_error        = $PostprocessError
        raw_artifacts_kept       = $KeepRawForThisRun
        artifacts                = $artifactMetadata
    }

    $metadata |
        ConvertTo-Json -Depth 6 |
        Set-Content -Path (Join-Path $ArtifactDir "run-metadata.json") -Encoding utf8

    if (-not $KeepRawForThisRun -and (Test-Path $RawDir)) {
        Remove-Item -Path $RawDir -Recurse -Force
        Write-Host "Artifact grezzi rimossi (usa -KeepRawArtifacts per conservarli)." -ForegroundColor DarkGray
    }
    elseif (Test-Path $RawDir) {
        Write-Host "Artifact grezzi conservati in: $RawDir" -ForegroundColor DarkYellow
    }

    if (-not $KeepContainers) {
        try {
            Write-Host "`nPulizia stack e volumi..." -ForegroundColor Yellow
            & docker compose `
                -f $ComposePath `
                --profile replay `
                down `
                -v `
                --remove-orphans | Out-Host
        }
        catch {
            Write-Host "Pulizia Docker non completata: $($_.Exception.Message)" -ForegroundColor DarkYellow
        }
    }
    else {
        Write-Host "`nContainer lasciati attivi (-KeepContainers)." -ForegroundColor Yellow
    }

    Write-Host ""
    Write-Host "=============================================" -ForegroundColor Green
    Write-Host " Risultato run: $Status" -ForegroundColor Green
    Write-Host "=============================================" -ForegroundColor Green
    Write-Host "Scenario: $ScenarioName"
    Write-Host "Artifacts: $ArtifactDir"
    Write-Host "Summary CSV: $(Join-Path $ArtifactDir 'run-summary.csv')"
    Write-Host "Durata totale: $elapsedSeconds s"
    if ($null -ne $replayElapsedSeconds) {
        Write-Host "Durata dal lancio replay: $replayElapsedSeconds s"
    }

    if ($null -ne $Failure) {
        Write-Host "Errore esecuzione: $Failure" -ForegroundColor Red
    }
    if ($PostprocessStatus -ne "success" -and $null -ne $PostprocessError) {
        Write-Host "Errore post-processing: $PostprocessError" -ForegroundColor Red
    }
}

if ($Status -ne "success" -or $PostprocessStatus -ne "success") {
    exit 1
}

exit 0
