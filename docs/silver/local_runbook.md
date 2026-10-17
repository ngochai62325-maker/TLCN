# Local startup and handoff

For the thesis demo use the [demo guide](demo_guide.md) and one command:
`powershell -NoProfile -ExecutionPolicy Bypass -File scripts/local_demo/start.ps1`.
Canonical project: `tlcn-local`, Airflow `http://localhost:8085`.
Fresh demo runtime smoke and stop/start persistence PASS; see
[Phase B evidence](evidence/demo_phase_b_20261017_131025/README.md).
The owner waived old-volume backups and approved the targeted cleanup.
The fresh project has zero DagRuns; the owner will trigger its first pipeline.

This is a **fresh-laptop workflow**, not a recovery command for an existing
clean-room project. Do not run it against an existing project whose
writer outcome is unresolved. Current evidence is indexed [here](evidence/index.md).

## Prerequisites

Windows with WSL2, Docker Desktop using Linux **amd64** containers, PowerShell,
and Docker Compose supporting `!reset` and `!override`. The verified versions
are Docker client/server28.5.1 and Compose2.40.3-desktop.1.
The demonstrated host has 16 GB RAM and roughly 8 GB allocated to Docker/WSL.
Allow at least 15 GB free disk for images/build cache, datasets and volumes; this
is a planning allowance, not a measured minimum. First image builds require
network access. No published private execution images or existing volumes are
required: Dockerfiles pin base digests and hash-locked dependency additions.

Use one lakehouse stack at a time on a 16 GB laptop. Before workloads, measure
Windows available RAM at least three times, 30–60 seconds apart: require about
2 GiB or more throughout, plus WSL headroom for runner growth, a 1 GiB reserve
and infrastructure growth. Actual usage and OOM counters matter, not cap totals.
Do not close Docker Desktop while writers are active.

## Raw bundle

Transfer the **21 input files separately**, preserving paths below `data/raw`.
The pinned files total **183,728,724 bytes**. Also include `nso/metadata.txt`;
it is support metadata in addition to the 21 observations files.
The full Trade CSV is **167,990,400 bytes**, SHA-256
`697bdc393bc0283b93a3fd90c51e7801c5363a089ea71fc1a7c6aed29a260168`.
Never substitute the validation fixture or add this CSV to normal Git history.

The [Raw manifest](../../config/local_raw_manifest.json) records all paths,
sizes and hashes. From the repository root, verify the transferred bundle:

```powershell
$manifest = Get-Content config/local_raw_manifest.json -Raw | ConvertFrom-Json
$rawRoot = (Resolve-Path ./data/raw).Path
foreach ($entry in @($manifest.files) + @($manifest.support_files)) {
    $path = Join-Path $rawRoot $entry.path
    if ((Get-Item -LiteralPath $path).Length -ne $entry.bytes) { throw "Size: $path" }
    if ((Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLower() -ne $entry.sha256) {
        throw "Checksum: $path"
    }
}
```

## Compose and image preparation

From the repository root:

```powershell
Copy-Item .env.local.example .env.local
# Edit .env.local if Raw is elsewhere or the default host ports are occupied.
$dc = @('compose', '--env-file', '.env.local', '-p', 'tlcn-local',
    '-f', 'compose.silver-test.yml', '-f', 'compose.local-nine-test.yml',
    '-f', 'compose.local.yml')
docker @dc config --quiet
docker @dc build runner airflow-webserver
```

Only two images are built; all three Airflow services share the same local image.
Builds use pinned Linux base images and `requirements/*.lock`; no pip install or
`.pytest_cache` mount is needed at service startup. Newly built local tags do not
claim the registry manifest digests of historical accepted images. Record actual
image IDs when validating a new laptop. Local test Compose uses isolated demo
authentication; no personal `.env`, database dump or secret backup is transferred.

## Fresh empty project startup

Use a new `-p` value for every independent empty-storage rebuild. Never combine
these files with `docker-compose.yml` or reuse shared/accepted volume names.

```powershell
docker @dc up -d --wait postgres-test minio-test rest-test
docker @dc up --no-deps airflow-init
# Require initializer exit 0; run it only for the fresh project.
docker @dc up -d --wait --no-deps trino-test runner
docker @dc up -d --no-deps airflow-webserver airflow-scheduler
docker @dc ps -a
docker @dc exec -T airflow-scheduler airflow dags list-import-errors
```

Both DAGs start paused and manual-only. Airflow is fixed at `http://localhost:8085`
and Trino at `http://localhost:18080` by default; override Trino in `.env.local`.
The isolated demo user's creation command is in `compose.local-nine-test.yml`.
The Trino web UI monitors queries; use its CLI below to execute SQL.

The historical, retired clean-room acceptance used its preserved
[port overlay](evidence/nso_v2/phase3_cleanroom_20261016_154548/compose.cleanroom.yml)
and original immutable image references. Those runtime resources were retired
under owner approval; the overlay remains audit evidence. The active demo uses
the three root Compose files shown above, with locally built execution images.

The canonical last overlay sets the project bridge to `internal: false`. The
test base's `internal: true` left requested port bindings unpublished on the
observed Docker Desktop runtime, even while container-local HTTP was healthy.
There is no host/shared-container network namespace. Verify actual `docker port`
and Windows HTTP after fresh startup, not just `docker compose config`.

## One manual Bronze → automatic Silver

After resource and readiness checks, unpause Silver then Bronze in Airflow UI.
Trigger **only** `rice_lakehouse_ingestion` with `{"force_reprocess":false}`.
Its batch gate automatically creates the correct Silver child. Do not manually
trigger `rice_lakehouse_silver_pipeline`.

For a CLI-only low-memory run, stop the idle webserver gracefully first and use:

```powershell
docker @dc exec -T airflow-scheduler airflow dags unpause rice_lakehouse_silver_pipeline
docker @dc exec -T airflow-scheduler airflow dags unpause rice_lakehouse_ingestion
$runId = 'local_manual__' + (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
docker @dc exec -T airflow-scheduler airflow dags trigger rice_lakehouse_ingestion --run-id $runId --conf '{"force_reprocess":false}'
docker @dc exec -T airflow-scheduler airflow dags list-runs -d rice_lakehouse_ingestion
docker @dc exec -T airflow-scheduler airflow dags list-runs -d rice_lakehouse_silver_pipeline
```

CLI and scheduler use PostgreSQL directly and do not need the webserver API.
Unchanged inputs can yield ingestion `SKIPPED` while the Airflow task is SUCCESS;
the commit/readback proof is mandatory. Pause both DAGs after reconciliation.
For full/incremental verification, use the existing common runner interfaces and
real batch source pins; see [validation](validation.md). Do not invent parent
configuration flags: the current automatic Silver child uses full mode.

## Trino validation

```powershell
docker @dc exec -T trino-test trino --server http://localhost:8080 --user local --execute 'SHOW TABLES FROM iceberg.silver_local'
docker @dc exec -T trino-test trino --server http://localhost:8080 --user local --execute 'SELECT count(*) FROM iceberg.silver_local.nso_rice_statistics_v2'
docker @dc exec -T trino-test trino --server http://localhost:8080 --user local --execute 'SELECT source_dataset, count(*) FROM iceberg.silver_local.dead_letters GROUP BY source_dataset'
```

Reconcile all nine tables against [the catalog](data_catalog.md), including business
keys, units, missing reasons and source lineage. Counts depend on the pinned Raw
bundle; they are acceptance measurements, not pipeline constants.

## Restart, safe stop and interruption

Pause both DAGs and verify no running/queued DagRun, task, source lock, Spark job
or Trino query before shutdown. Keep volumes. On a normal subsequent session,
start **existing containers** only after read-only state reconciliation; do not
rerun the initializer or a fresh-project `up` sequence blindly.

If stopping services, use graceful signals and wait for each actual exit. For an
idle Python Spark API, the demonstrated shutdown uses one SIGINT; never escalate
automatically after a timeout. Stop execution before Trino/catalog/MinIO/PostgreSQL.
Never use `down -v`, prune, clear all tasks, reset snapshots or force reprocess as
a default recovery step. Exit 137 during Docker Desktop shutdown alone does not
prove OOM or absence of a commit.

After interruption, keep scheduler stopped and inspect PostgreSQL, catalog
pointers, active files, task logs/XCom and quarantine independently. A RUNNING task
may already have committed. Resume only the exact unfinished task after a known
outcome and explicit approval for any state mutation; preserve successful tasks.

## Troubleshooting and limits

- Missing Raw/checksum mismatch: fix the transferred bundle; never enable fixture.
- UI unavailable: check the exact project/ports and webserver state. CLI-only runs
  intentionally leave it stopped; its availability is not needed by the pipeline.
- Low RAM/OOM: stop before new work, preserve committed evidence, run one stack and
  remove idle UI overhead. Do not raise caps or kill applications automatically.
- Schema/source revision: fail closed; do not auto-ALTER or overwrite accepted data.
- Image unavailable: build from the supplied Dockerfiles/locks; do not silently
  substitute `latest` for an immutable reference.

A second-laptop execution has not yet been performed. Portable Compose resolution, Raw checksums, imports/DAG parsing and regression
have passed locally; image rebuild and actual second-laptop execution were not run
in this completion task.
NSO production DRAFT, production scheduling and Gold remain outside this workflow.
