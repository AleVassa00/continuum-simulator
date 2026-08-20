[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[A-Za-z0-9.-]+$")]
    [string]$HostName,

    [Parameter(Mandatory = $true)]
    [string]$KeyPath,

    [ValidatePattern("^edge-m[0-9]+-[0-9]+$")]
    [string]$NodeId = "edge-m2-0",

    [ValidatePattern("^[a-z_][a-z0-9_-]*$")]
    [string]$UserName = "ec2-user"
)

$ErrorActionPreference = "Stop"

$sensorRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$projectRoot = (Resolve-Path (Join-Path $sensorRoot "..")).Path
$topologyPath = Join-Path $projectRoot "artifacts\eda\kmeans_topology.csv"
$buildDirectory = Join-Path $sensorRoot ".build\ec2"
$archivePath = Join-Path $buildDirectory "continuum-deploy.tar.gz"

if (-not (Test-Path -LiteralPath $KeyPath -PathType Leaf)) {
    throw "EC2 private key not found: $KeyPath"
}
if (-not (Test-Path -LiteralPath $topologyPath -PathType Leaf)) {
    throw "Topology not found: $topologyPath"
}

New-Item -ItemType Directory -Force -Path $buildDirectory | Out-Null

$archiveInputs = @(
    "Sensor/.dockerignore",
    "Sensor/cmd",
    "Sensor/internal",
    "Sensor/go.mod",
    "Sensor/go.sum",
    "Sensor/config/project.yml",
    "Sensor/deploy/docker/Dockerfile",
    "Sensor/deploy/compose/continuum.yml",
    "Sensor/deploy/mosquitto/mosquitto.conf",
    "artifacts/eda/kmeans_topology.csv"
)

Write-Host "Creating the deployment archive..."
Push-Location $projectRoot
try {
    & tar -czf $archivePath @archiveInputs
    if ($LASTEXITCODE -ne 0) {
        throw "deployment archive creation failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}

$destination = "${UserName}@${HostName}"
$sshArguments = @("-i", $KeyPath)

Write-Host "Uploading the Compose deployment archive..."
& scp @sshArguments $archivePath "${destination}:/tmp/continuum-deploy.tar.gz"
if ($LASTEXITCODE -ne 0) {
    throw "deployment archive upload failed"
}

$remoteCommand = @"
set -eu
sudo cloud-init status --wait
sudo install -d -o ec2-user -g ec2-user -m 0755 /opt/continuum
sudo tar -xzf /tmp/continuum-deploy.tar.gz -C /opt/continuum
sudo chown -R ec2-user:ec2-user /opt/continuum
cd /opt/continuum/Sensor
sudo env EDGE_NODE_ID='$NodeId' docker compose -f deploy/compose/continuum.yml config --quiet
sudo env EDGE_NODE_ID='$NodeId' docker compose -f deploy/compose/continuum.yml up -d --build --remove-orphans
sudo docker compose -f deploy/compose/continuum.yml ps
sudo docker compose -f deploy/compose/continuum.yml logs --tail=50 edge
"@

Write-Host "Building and starting the Docker Compose application on EC2..."
& ssh @sshArguments $destination $remoteCommand
if ($LASTEXITCODE -ne 0) {
    throw "remote Compose deployment failed"
}

Write-Host "Deployment completed. Follow the Edge logs with:"
Write-Host "ssh -i `"$KeyPath`" $destination sudo docker compose -f /opt/continuum/Sensor/deploy/compose/continuum.yml logs -f edge"
