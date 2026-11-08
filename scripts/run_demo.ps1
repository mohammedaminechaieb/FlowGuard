<#
.SYNOPSIS
  FlowGuard - full end-to-end demo in one command.

.DESCRIPTION
  Runs the whole pipeline and prints every command it executes:
    1. Docker Desktop
    2. Hadoop cluster (class containers) + HDFS
    3. Kafka, Cassandra (+ automatic schema), Grafana
    4. Clean start: empty Kafka topic, Cassandra tables and Spark checkpoints
    5. Kafka topic
    6. Spark inference job (-> Cassandra) and raw archive job (-> HDFS)
    7. Grafana ready (link printed, open it yourself)
    8. Replays CICIDS2017 flows into Kafka (producer)
    9. Waits until Spark has scored every flow
   10. Latency / throughput report (+ opens the figure)
   11. Shows the raw stream archived in HDFS
   12. Summary

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\run_demo.ps1
  powershell -ExecutionPolicy Bypass -File scripts\run_demo.ps1 -Dataset "tuesday,wednesday,friday" -Events 5000
  powershell -ExecutionPolicy Bypass -File scripts\run_demo.ps1 -Dataset wednesday -Events 20000 -Delay 0.01 -SkipHdfs
#>
param(
    [string]$Dataset = "demo",     # one day or several: "tuesday,wednesday,friday"
    [int]$Events = 5000,           # max flows per day
    [double]$Delay = 0.02,         # seconds between two flows (0.02 = 50 flows/s)
    [switch]$NoReset,
    [switch]$SkipHdfs,
    [string]$Python = "py"
)

# Owner : Amine
$ErrorActionPreference = "Continue"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"

Set-Location (Split-Path -Parent $PSScriptRoot)   # project root
$Compose = "docker compose -f docker/docker-compose.yml"
$StartTime = Get-Date
$HadoopNodes = "hadoop-master hadoop-slave1 hadoop-slave2"
$DelayText = $Delay.ToString([Globalization.CultureInfo]::InvariantCulture)  # 0.02, not 0,02

# Demo-friendly defaults (only if not already set in the terminal)
if (-not $env:MAX_OFFSETS_PER_TRIGGER)  { $env:MAX_OFFSETS_PER_TRIGGER  = "2000" }        # Spark keeps up easily
if (-not $env:ARCHIVE_TRIGGER_INTERVAL) { $env:ARCHIVE_TRIGGER_INTERVAL = "10 seconds" }  # faster HDFS writes

# ------------------------------------------------------------------ helpers
function Step([string]$n, [string]$title) {
    Write-Host ""
    Write-Host ("=" * 72) -ForegroundColor DarkGray
    Write-Host " [$n] $title" -ForegroundColor Yellow
    Write-Host ("=" * 72) -ForegroundColor DarkGray
}

function Run([string]$cmd, [switch]$AllowFail) {
    Write-Host "PS> $cmd" -ForegroundColor Cyan
    $global:LASTEXITCODE = 0
    Invoke-Expression $cmd
    if (-not $AllowFail -and $LASTEXITCODE -ne 0) {
        Write-Host "FAILED (exit $LASTEXITCODE): $cmd" -ForegroundColor Red
        exit 1
    }
}

function Wait-Until([string]$what, [scriptblock]$test, [int]$timeoutSec = 300, [int]$every = 5) {
    Write-Host "   waiting for $what " -NoNewline -ForegroundColor DarkGray
    $sw = [Diagnostics.Stopwatch]::StartNew()
    while (-not (& $test)) {
        if ($sw.Elapsed.TotalSeconds -gt $timeoutSec) {
            Write-Host " TIMEOUT" -ForegroundColor Red
            exit 1
        }
        Start-Sleep -Seconds $every
        Write-Host "." -NoNewline -ForegroundColor DarkGray
    }
    Write-Host " OK ($([int]$sw.Elapsed.TotalSeconds) s)" -ForegroundColor Green
}

function Get-CassandraCount([string]$table, [string]$where) {
    $out = docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM flowguard.$table WHERE $where;" 2>$null | Out-String
    if ($out -match "(?m)^\s*(\d+)\s*$") { return [int]$Matches[1] }
    return -1
}

function Get-HdfsLiveNodes {
    $r = docker exec hadoop-master hdfs dfsadmin -report 2>$null | Out-String
    if ($r -match "Live datanodes \((\d+)\)") { return [int]$Matches[1] }
    return 0
}

# ------------------------------------------------------------------ 0. checks
$Datasets = $Dataset -split "," | ForEach-Object { $_.Trim().ToLower() } | Where-Object { $_ }
$valid = @("demo", "tuesday", "wednesday", "thursday", "friday", "train", "validation")
$expected = 0
foreach ($d in $Datasets) {
    if ($d -notin $valid) {
        Write-Host "Unknown dataset '$d'. Use: $($valid -join ', ')" -ForegroundColor Red
        exit 1
    }
    $parquet = if ($d -in @("train", "validation")) { "$d.parquet" } else { "test_$d.parquet" }
    if (-not (Test-Path "data/parquet/$parquet")) {
        $hint = if ($d -eq "demo") { "py scripts/make_demo_dataset.py" } else { "py ai_module/prepare_data.py" }
        Write-Host "Missing data/parquet/$parquet - run: $hint" -ForegroundColor Red
        exit 1
    }
    $rows = [int](& $Python -c "import pyarrow.parquet as pq; print(pq.ParquetFile(r'data/parquet/$parquet').metadata.num_rows)")
    $expected += [Math]::Min($Events, $rows)
}
$DatasetIn   = ($Datasets | ForEach-Object { "'$_'" }) -join ","   # for Cassandra: 'tuesday','friday'
$DatasetText = $Datasets -join ", "

# ------------------------------------------------------------------ 1. Docker
Step 1 "Docker Desktop"
docker info *> $null
if ($LASTEXITCODE -ne 0) {
    $dockerDesktop = "$env:ProgramFiles\Docker\Docker\Docker Desktop.exe"
    Write-Host "PS> Start-Process `"$dockerDesktop`"" -ForegroundColor Cyan
    Start-Process $dockerDesktop
    Wait-Until "Docker engine" { docker info *> $null; $LASTEXITCODE -eq 0 } 240
} else {
    Write-Host "   Docker engine is running" -ForegroundColor Green
}

# ------------------------------------------------------------------ 2. Hadoop / HDFS
if (-not $SkipHdfs) {
    Step 2 "Hadoop cluster + HDFS (raw stream archive)"
    Run "docker start $HadoopNodes"
    if ((Get-HdfsLiveNodes) -lt 2) {
        Run 'docker exec hadoop-master bash -c "cd /root && ./start-hadoop.sh"'
    }
    Wait-Until "2 live HDFS DataNodes" { (Get-HdfsLiveNodes) -ge 2 } 180
    Run "docker exec hadoop-master hdfs dfsadmin -safemode wait"
    Run "docker exec hadoop-master hdfs dfs -mkdir -p /flowguard/archive /flowguard/checkpoints"
    Run "docker exec hadoop-master hdfs dfs -chmod -R 777 /flowguard"
}

# ------------------------------------------------------------------ 3. Infrastructure
Step 3 "Kafka, Cassandra (+ automatic schema), Grafana"
Run "$Compose up -d kafka cassandra grafana cassandra-init"
Wait-Until "Kafka healthy" { (docker inspect -f "{{.State.Health.Status}}" kafka 2>$null) -eq "healthy" } 180
Write-Host "PS> docker wait flowguard-cassandra-init" -ForegroundColor Cyan
$initCode = "$(docker wait flowguard-cassandra-init)".Trim()
Run "docker logs flowguard-cassandra-init"
if ($initCode -ne "0") { Write-Host "Cassandra schema setup failed (exit $initCode)" -ForegroundColor Red; exit 1 }

# ------------------------------------------------------------------ 4. Clean start
if (-not $NoReset) {
    Step 4 "Clean start: empty Kafka topic, Cassandra tables, Spark checkpoints"
    Run "$Compose --profile archive stop spark spark-archive" -AllowFail
    Run "docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --if-exists --topic network-telemetry" -AllowFail
    Wait-Until "old topic deleted" {
        -not ((docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>$null) -contains "network-telemetry")
    } 60 2
    Run 'docker exec cassandra cqlsh -e "TRUNCATE flowguard.all_flows; TRUNCATE flowguard.alerts; TRUNCATE flowguard.batch_metrics;"'
    Run "Remove-Item -Recurse -Force checkpoints -ErrorAction SilentlyContinue"
    if (-not $SkipHdfs) {
        Run "docker exec hadoop-master hdfs dfs -rm -r -f -skipTrash /flowguard/checkpoints/archive_raw" -AllowFail
    }
}

# ------------------------------------------------------------------ 5. Topic
Step 5 "Kafka topic network-telemetry"
Run "docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic network-telemetry --partitions 1 --replication-factor 1"

# ------------------------------------------------------------------ 6. Spark jobs
Step 6 "Spark jobs: inference (-> Cassandra) and raw archive (-> HDFS)"
$sparkSince = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
if ($SkipHdfs) {
    Run "$Compose up -d --no-deps --force-recreate spark"
} else {
    Run "$Compose --profile archive up -d --no-deps --force-recreate spark spark-archive"
}
Wait-Until "Spark inference job started" {
    (docker logs --since $sparkSince flowguard-spark 2>&1 | Out-String) -match "FlowGuard - D"
} 300
if (-not $SkipHdfs) {
    Wait-Until "Spark archive job started" {
        (docker logs --since $sparkSince flowguard-spark-archive 2>&1 | Out-String) -match "Archiving"
    } 300
}
Start-Sleep -Seconds 10   # let the streaming queries subscribe to Kafka

# ------------------------------------------------------------------ 7. Grafana
Step 7 "Grafana dashboard"
Wait-Until "Grafana" {
    try { (Invoke-WebRequest -UseBasicParsing "http://localhost:3000/api/health" -TimeoutSec 3).StatusCode -eq 200 } catch { $false }
} 120
$grafanaUrl = "http://localhost:3000/d/flowguard-main?orgId=1&var-dataset=$($Datasets[0])&from=now-15m&to=now&refresh=5s"
Write-Host "   Grafana is ready - open it yourself when you want:" -ForegroundColor Green
Write-Host "   $grafanaUrl" -ForegroundColor Magenta
Write-Host "   Login: admin / admin (click 'Skip' on the password change)" -ForegroundColor Magenta

# ------------------------------------------------------------------ 8. Producer
Step 8 "Replay $expected flows ($DatasetText) into Kafka - watch Grafana now"
Run "$Python -m pip install -q -r streaming/requirements_streaming.txt" -AllowFail
foreach ($d in $Datasets) {
    Run "$Python streaming/kafka_producer.py --dataset $d --max-events $Events --delay $DelayText"
}

# ------------------------------------------------------------------ 9. Wait for Spark
Step 9 "Waiting until Spark has scored every flow"
$target = [int]($expected * 0.98)   # a few rows with invalid values are skipped
$previous = -1; $stable = 0
$sw = [Diagnostics.Stopwatch]::StartNew()
while ($true) {
    $count = Get-CassandraCount "all_flows" "dataset IN ($DatasetIn)"
    Write-Host "   flows scored and stored in Cassandra: $count / $expected"
    if ($count -ge $target) { break }
    if ($count -gt 0 -and $count -eq $previous) { $stable++ } else { $stable = 0 }
    if ($stable -ge 4) { Write-Host "   (no new rows for 20 s - continuing)" -ForegroundColor DarkYellow; break }
    if ($sw.Elapsed.TotalSeconds -gt 900) { Write-Host "   timeout - continuing" -ForegroundColor DarkYellow; break }
    $previous = $count
    Start-Sleep -Seconds 5
}
Start-Sleep -Seconds 6   # last batch_metrics row

# ------------------------------------------------------------------ 10. Latency report
Step 10 "Latency and throughput report"
$minutes = [int][Math]::Ceiling(((Get-Date) - $StartTime).TotalMinutes) + 1
Run "$Python streaming/latency_report.py --minutes $minutes"
Run "Start-Process results\latency_distribution.png" -AllowFail

# ------------------------------------------------------------------ 11. HDFS archive
if (-not $SkipHdfs) {
    Step 11 "Raw stream archived in HDFS"
    Write-Host "   waiting 12 s for the archive job's next write (every 10 s)..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 12
    Run "docker exec hadoop-master hdfs dfs -ls -R /flowguard/archive"
    Run "docker exec hadoop-master hdfs dfs -du -s -h /flowguard/archive"
}

# ------------------------------------------------------------------ 12. Summary
Step 12 "Summary"
$flows   = Get-CassandraCount "all_flows" "dataset IN ($DatasetIn)"
$alerts  = Get-CassandraCount "alerts" "dataset IN ($DatasetIn)"
$batches = Get-CassandraCount "batch_metrics" "pipeline='spark'"
$elapsed = (Get-Date) - $StartTime
Write-Host ("   Datasets            : {0}" -f $DatasetText)
Write-Host ("   Flows in Cassandra  : {0}" -f $flows)
Write-Host ("   Anomalies (alerts)  : {0}" -f $alerts) -ForegroundColor Red
Write-Host ("   Spark batches       : {0}" -f $batches)
Write-Host ("   Total demo time     : {0:mm\:ss}" -f $elapsed)
Write-Host ""
Write-Host "   Grafana      : $grafanaUrl"
Write-Host "   Spark UI     : http://localhost:4040"
Write-Host "   HDFS UI      : http://localhost:50070  (or 9870 on Hadoop 3)"
Write-Host "   Report       : results\latency_report.csv, results\latency_distribution.png"
Write-Host ""
Write-Host "   Everything is still running. To stop it:" -ForegroundColor DarkGray
Write-Host "   $Compose --profile archive down" -ForegroundColor DarkGray