# Shared CLI plumbing only. This file never starts, stops or deletes resources.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script:DemoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$script:DemoProject = 'tlcn-local'
$script:DemoEnv = '.env.example'
if (Test-Path -LiteralPath (Join-Path $script:DemoRoot '.env')) { $script:DemoEnv = '.env' }
$script:DemoCompose = @('compose', '--project-directory', $script:DemoRoot,
    '--env-file', (Join-Path $script:DemoRoot $script:DemoEnv), '-p', $script:DemoProject,
    '-f', (Join-Path $script:DemoRoot 'compose.yaml'))

function Invoke-DemoDocker {
    param([string[]]$Arguments, [switch]$Capture)
    $result = & docker @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker failed (exit $LASTEXITCODE): $($Arguments[0])" }
    if ($Capture) { return ($result -join "`n") }
    $result | Out-Host
}

function Invoke-DemoPython {
    param([string]$ContainerId, [string]$Code, [string[]]$Environment = @())
    # stdin avoids Windows PowerShell 5 native-argument quote stripping in -c.
    $arguments = @('exec', '-i')
    foreach ($value in $Environment) { $arguments += @('-e', $value) }
    $Code | & docker @arguments $ContainerId python -
    if ($LASTEXITCODE -ne 0) { throw "Read-only Python probe failed (exit $LASTEXITCODE)." }
}

function Get-DemoConfig {
    $config = Invoke-DemoDocker -Arguments ($script:DemoCompose + @('config', '--format', 'json')) -Capture | ConvertFrom-Json
    $web = $config.services.'airflow-webserver'
    if ($web.ports[0].host_ip -ne '127.0.0.1' -or [int]$web.ports[0].published -ne 8085) { throw 'Airflow UI must publish 127.0.0.1:8085.' }
    $minio = $config.services.'minio'
    if ($minio.ports[0].host_ip -ne '127.0.0.1' -or [int]$minio.ports[0].published -ne 9001) { throw 'MinIO Console must publish 127.0.0.1:9001.' }
    $trino = $config.services.'trino'
    if ($trino.ports[0].host_ip -ne '127.0.0.1' -or [int]$trino.ports[0].published -ne 18080) { throw 'Trino must publish 127.0.0.1:18080.' }
    if ($config.networks.default.PSObject.Properties['internal'] -and $config.networks.default.internal) {
        throw 'Internal-only bridge prevents the verified localhost publication.'
    }
    foreach ($service in @('runner', 'airflow-scheduler', 'airflow-webserver')) {
        $env = $config.services.$service.environment
        if ($env.SILVER_LOCAL_PROFILE -ne 'local_nine' -or $env.SILVER_LOCAL_ENABLED -ne '1' -or
            $env.SILVER_PRODUCTION_ENABLED -ne '0' -or $env.SILVER_ACCEPTED_RELEASE -or $env.SILVER_BRONZE_SCHEDULE) {
            throw "Unsafe local routing: $service"
        }
    }
    return $config
}

function Test-DemoRaw {
    param($Config)
    $raw = @($Config.services.'airflow-scheduler'.volumes | Where-Object { $_.target -eq '/workspace/data/raw' })
    if ($raw.Count -ne 1 -or -not $raw[0].read_only) { throw 'One readonly Windows Raw bind is required.' }
    $root = (Resolve-Path -LiteralPath $raw[0].source).Path
    $manifest = Get-Content -LiteralPath (Join-Path $script:DemoRoot 'config/local_raw_manifest.json') -Raw | ConvertFrom-Json
    if ($manifest.files.Count -ne 21) { throw 'Raw manifest must contain 21 files.' }
    foreach ($entry in @($manifest.files) + @($manifest.support_files)) {
        $path = [IO.Path]::GetFullPath((Join-Path $root $entry.path))
        if (-not $path.StartsWith($root.TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase)) { throw 'Raw manifest path escapes root.' }
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Missing Raw: $($entry.path)" }
        if ((Get-Item -LiteralPath $path).Length -ne $entry.bytes -or
            (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant() -ne $entry.sha256) {
            throw "Raw size/SHA-256 mismatch: $($entry.path)"
        }
    }
    Write-Host 'Raw PASS: 21 source files + nso/metadata.txt, Windows bind, sizes/SHA-256.'
}

function Get-AllDemoContainers {
    $ids = @(& docker ps -aq)
    if ($LASTEXITCODE -ne 0) { throw 'Cannot list Docker containers.' }
    if ($ids.Count) {
        # Windows PowerShell 5 emits a JSON root array as one pipeline object.
        # Emit each decoded container explicitly before Where-Object filtering.
        $decoded = Invoke-DemoDocker -Arguments (@('inspect') + $ids) -Capture | ConvertFrom-Json
        foreach ($container in $decoded) { Write-Output $container }
    }
}

function Get-DemoContainers {
    return @(Get-AllDemoContainers | Where-Object {
        $_.Config.Labels -and $_.Config.Labels.PSObject.Properties['com.docker.compose.project'] -and
        $_.Config.Labels.'com.docker.compose.project' -eq $script:DemoProject
    })
}

function Wait-DemoContainer {
    param([string]$Id)
    $deadline = (Get-Date).AddMinutes(3)
    do {
        $c = @(Invoke-DemoDocker -Arguments @('inspect', $Id) -Capture | ConvertFrom-Json)[0]
        if (-not $c.State.Running) { throw "Container exited during startup: $($c.Name)" }
        if (-not $c.State.PSObject.Properties['Health'] -or $c.State.Health.Status -eq 'healthy') { return }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)
    throw "Health timeout: $($c.Name). No automatic recreate."
}

function Assert-DemoPersistedPause {
    param([string]$PostgresId)
    # Runs before the scheduler starts. Unknown persisted runs/tasks block reopening.
    $sql = @'
BEGIN READ ONLY;
SELECT CASE WHEN
 (SELECT count(*) FROM dag WHERE dag_id IN ('rice_lakehouse_ingestion','rice_lakehouse_silver_pipeline') AND is_paused)=2
 AND NOT EXISTS (SELECT 1 FROM dag_run WHERE state IN ('running','queued'))
 AND NOT EXISTS (SELECT 1 FROM task_instance WHERE state IN ('running','queued','scheduled','restarting'))
 AND NOT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory')
 THEN 'SAFE_PAUSED' ELSE 'BLOCKED' END;
ROLLBACK;
'@
    $shell = 'exec psql -X -v ON_ERROR_STOP=1 -At -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<''TLCN_SQL''' + "`n" + $sql + "`nTLCN_SQL`n"
    $output = $shell | & docker exec -i $PostgresId sh -s
    if ($LASTEXITCODE -ne 0) { throw 'Persisted PostgreSQL probe failed.' }
    $result = $output -join "`n"
    if (($result -split "`n") -notcontains 'SAFE_PAUSED') { throw 'Persisted metadata is not quiescent/paused. Inspect commits before resuming.' }
}

function Assert-NoCompetingStacks {
    foreach ($c in @(Get-AllDemoContainers)) {
        $labels = $c.Config.Labels
        $project = ''
        if ($labels -and $labels.PSObject.Properties['com.docker.compose.project']) { $project = $labels.'com.docker.compose.project' }
        $repoMount = @($c.Mounts | Where-Object { $_.Type -eq 'bind' -and $_.Source -eq $script:DemoRoot }).Count -gt 0
        if ($c.State.Running -and $project -ne $script:DemoProject -and
            ($project -eq 'tlcn' -or $project.StartsWith('tlcn-') -or $repoMount)) {
            throw "Another TLCN stack is running: $($c.Name). Stop/reconcile it separately; this script will not stop it."
        }
    }
}

function Get-DemoMemory {
    $os = Get-CimInstance Win32_OperatingSystem
    $windows = [math]::Round($os.FreePhysicalMemory / 1MB, 3)
    $meminfo = & wsl -e cat /proc/meminfo
    if ($LASTEXITCODE -ne 0) { throw 'Cannot measure WSL memory.' }
    $match = [regex]::Match(($meminfo -join "`n"), '(?m)^MemAvailable:\s+(\d+)')
    if (-not $match.Success) { throw 'Missing WSL MemAvailable.' }
    return [pscustomobject]@{ WindowsAvailableGiB = $windows; WslAvailableGiB = [math]::Round([double]$match.Groups[1].Value / 1MB, 3) }
}

function Assert-DemoIdle {
    param([object[]]$Containers)
    $scheduler = @($Containers | Where-Object { $_.Config.Labels.'com.docker.compose.service' -eq 'airflow-scheduler' })
    if ($scheduler.Count -ne 1 -or -not $scheduler[0].State.Running) {
        throw 'A running scheduler container is required for the read-only DB/Trino safety probe. Reconcile offline state separately.'
    }
    $probe = @'
import json,os,urllib.request
import psycopg2
url=os.environ['INGESTION_DB_URL']
with psycopg2.connect(url) as db:
 db.set_session(readonly=True)
 with db.cursor() as c:
  c.execute("SELECT count(*) FROM dag_run WHERE state IN ('running','queued')"); runs=c.fetchone()[0]
  c.execute("SELECT count(*) FROM task_instance WHERE state IN ('running','queued','scheduled','restarting')"); tasks=c.fetchone()[0]
  c.execute("SELECT count(*) FROM pg_locks WHERE locktype='advisory'"); locks=c.fetchone()[0]
  c.execute("SELECT to_regclass('ingestion.ingestion_runs')"); exists=c.fetchone()[0]
  in_progress=0
  if exists:
   c.execute("SELECT count(*) FROM ingestion.ingestion_runs WHERE status='IN_PROGRESS'"); in_progress=c.fetchone()[0]
with urllib.request.urlopen(urllib.request.Request('http://trino:8080/v1/query',headers={'X-Trino-User':'local-demo-readonly'}),timeout=10) as r:
 active=[q for q in json.load(r) if q['state'] not in ('FINISHED','FAILED')]
out={'active_dag_runs':runs,'active_tasks':tasks,'advisory_locks':locks,'ingestion_in_progress':in_progress,'active_trino_queries':len(active)}
print(json.dumps(out));assert not any(out.values()),'Not idle; inspect commit/catalog state before stopping or resetting'
'@
    Invoke-DemoPython -ContainerId $scheduler[0].Id -Code $probe
    $runner = @($Containers | Where-Object { $_.Config.Labels.'com.docker.compose.service' -eq 'runner' })
    if ($runner.Count -ne 1 -or -not $runner[0].State.Running) { throw 'Cannot prove runner is idle.' }
    # Docker top needs a PID column even if the guard only inspects stat/comm.
    $processes = Invoke-DemoDocker -Arguments @('top', $runner[0].Id, '-eo', 'pid,stat,comm') -Capture
    if ($processes -match '(?m)^\s*\d+\s+[^Z\s]\S*\s+java\s*$') { throw 'Spark JVM still exists; reconcile it first.' }
}

function Assert-DemoReady {
    param([switch]$Fresh)
    $containers = @(Get-DemoContainers)
    $web = @($containers | Where-Object { $_.Config.Labels.'com.docker.compose.service' -eq 'airflow-webserver' })[0]
    $ports = Invoke-DemoDocker -Arguments @('port', $web.Id, '8080/tcp') -Capture
    if ($ports.Trim() -ne '127.0.0.1:8085') { throw "Actual Airflow port is missing/wrong: $ports" }
    $minio = @($containers | Where-Object { $_.Config.Labels.'com.docker.compose.service' -eq 'minio' })[0]
    $minioPorts = Invoke-DemoDocker -Arguments @('port', $minio.Id, '9001/tcp') -Capture
    if ($minioPorts.Trim() -ne '127.0.0.1:9001') { throw "Actual MinIO Console port is missing/wrong: $minioPorts" }
    $trino = @($containers | Where-Object { $_.Config.Labels.'com.docker.compose.service' -eq 'trino' })[0]
    $trinoPorts = Invoke-DemoDocker -Arguments @('port', $trino.Id, '8080/tcp') -Capture
    if ($trinoPorts.Trim() -ne '127.0.0.1:18080') { throw "Actual Trino port is missing/wrong: $trinoPorts" }
    $health = Invoke-RestMethod 'http://127.0.0.1:8085/health' -TimeoutSec 10
    if ($health.metadatabase.status -ne 'healthy' -or $health.scheduler.status -ne 'healthy') { throw 'Airflow DB/scheduler is unhealthy.' }
    $minioResp = Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:9001' -TimeoutSec 10
    if ($minioResp.StatusCode -ne 200) { throw 'MinIO Console is unreachable on 127.0.0.1:9001.' }
    $trinoInfo = Invoke-RestMethod 'http://127.0.0.1:18080/v1/info' -TimeoutSec 10
    if ($trinoInfo.starting) { throw 'Trino coordinator is still starting.' }
    $scheduler = @($containers | Where-Object { $_.Config.Labels.'com.docker.compose.service' -eq 'airflow-scheduler' })[0]
    $webDb = @($web.Config.Env | Where-Object { $_.StartsWith('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=') })
    $schedulerDb = @($scheduler.Config.Env | Where-Object { $_.StartsWith('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=') })
    if ($webDb.Count -ne 1 -or $schedulerDb.Count -ne 1 -or $webDb[0] -ne $schedulerDb[0]) { throw 'Webserver/scheduler database mismatch.' }
    foreach ($c in @($containers | Where-Object { $_.Config.Labels.'com.docker.compose.service' -in @('runner', 'airflow-scheduler', 'airflow-webserver') })) {
        if ($c.Config.Env -notcontains 'SILVER_LOCAL_PROFILE=local_nine' -or $c.Config.Env -notcontains 'SILVER_LOCAL_ENABLED=1' -or
            $c.Config.Env -notcontains 'SILVER_PRODUCTION_ENABLED=0' -or $c.Config.Env -notcontains 'SILVER_CANARY_ONLY=0' -or
            $c.Config.Env -notcontains 'SILVER_ACCEPTED_RELEASE=' -or $c.Config.Env -notcontains 'SILVER_BRONZE_SCHEDULE=') {
            throw "Actual scope flags mismatch: $($c.Name)"
        }
    }
    $probe = @'
import json,os,urllib.request,hashlib
from pathlib import Path
from airflow.models import DagBag
from airflow import settings
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from silver.local_profile import BRONZE_SOURCES,DATASETS,REGISTRY_SHA256,local_contract
from silver.contract.loader import SilverContractLoader
bag=DagBag('dags/',include_examples=False);assert not bag.import_errors,bag.import_errors
assert set(bag.dags)=={'rice_lakehouse_ingestion','rice_lakehouse_silver_pipeline'}
b=bag.dags['rice_lakehouse_ingestion'];s=bag.dags['rice_lakehouse_silver_pipeline']
assert {t.task_id[7:] for t in b.tasks if t.task_id.startswith('ingest_')}==set(BRONZE_SOURCES)
assert {t.task_id[10:] for t in s.tasks if t.task_id.startswith('transform_')}==set(DATASETS)
assert len(s.tasks)==14 and 'transform_nso' in s.task_ids
assert all(d.schedule_interval is None and not d.catchup and d.max_active_runs==1 and all(t.retries==0 for t in d.tasks) for d in [b,s])
assert all(local_contract(SilverContractLoader(),d).silver_output.startswith('iceberg.silver_local.') for d in DATASETS)
assert hashlib.sha256(Path('config/ingestion/source_registry.yaml').read_bytes()).hexdigest()==REGISTRY_SHA256
assert SilverContractLoader().load_contract('nso').status=='READY'
with settings.Session() as session:
 dags=session.query(DagModel).filter(DagModel.dag_id.in_([b.dag_id,s.dag_id])).all()
 assert len(dags)==2 and all(d.is_paused for d in dags),'Pause both DAGs before readiness check'
 runs=session.query(DagRun).count()
 if os.environ.get('DEMO_EXPECT_EMPTY')=='1':assert runs==0,'Unexpected automatic DagRun'
for url in ['http://runner:5005/health','http://iceberg-rest:8181/v1/config','http://trino:8080/v1/info']:
 with urllib.request.urlopen(url,timeout=10) as r:assert r.status==200
print(json.dumps({'dag_parse':'PASS','bronze_sources':9,'silver_tasks':14,'silver_sources':9,'paused':True,'schedule':None,'dag_runs':runs,'services_http':'PASS','nso_contract':'READY'}))
'@
    $empty = '0'; if ($Fresh) { $empty = '1' }
    Invoke-DemoPython -ContainerId $scheduler.Id -Code $probe -Environment @("DEMO_EXPECT_EMPTY=$empty")
    Write-Host 'READINESS PASS (smoke only; Bronze/Silver E2E has NOT been run by this script).'
}
