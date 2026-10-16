# Phase 2 — local nine-source integration, stopped before Airflow E2E

Status: **BLOCKED_RESOURCE_HEADROOM — isolated Airflow webserver cgroup OOM**.
No manual Bronze run or automatic Silver run was triggered. Phase 2 is not
accepted; do not infer Phase 3 clean-room PASS.

Implemented routing/orchestration: [local_nine.md](../../../local_nine.md).
Machine-readable summary: [report.json](report.json).

## Validation completed

| Validation | Observed result |
|---|---|
| Initial DAG/profile/batch + existing release metadata regressions | 67 PASS, exit0 |
| New local Airflow metadata tests | 6 PASS, exit0 |
| Existing + local API/runner/lock regressions | 24 PASS, exit0 |
| Final affected DAG/local metadata regressions after local source chains | 18 PASS, exit0 |
| Native read context, local readiness/NOOP and API arguments | 6 PASS, exit0, 65.46s |
| Actual runtime DagBag + serialized scopes | PASS: exact nine sources/datasets, both paused/manual-only/retries0 |
| Actual spark-submit child `--help` import | PASS; no SparkSession/data write |
| Full nine-source Airflow E2E | NOT_RUN: webserver failed pre-trigger admission |

Counts above are executions of overlapping focused suites, not a claim of 121
unique tests. Native proof includes actual Iceberg concurrent snapshot pinning,
missing/invalid context rejection, unchanged eight-source opt-in behavior and a
new local NSO eight-observation fixture with full/incremental NOOP. It does not
substitute for the unexecuted Airflow run with full Raw inputs.

An initial native attempt failed during collection because a fixture imported
ingestion dependencies without boto3. It ran no writer. Its exit2/log is retained;
the separate corrected test container used existing offline boto3==1.35.99 and
passed. Images and production dependency versions were not changed.

Runtime configuration also exposed an explicitly empty release environment value.
Only the activated local DAG branches normalize it to no release; accepted/default
behavior is unchanged. The actual paused/runtime/serialized DAG checks passed
with that exact Compose environment. A plain `python` import probe lacked
spark-submit's PySpark path; the actual spark-submit child `--help` subsequently
passed without creating a data job.

Source checks confirmed 21 pinned Raw files/183,728,724 bytes, including full Trade
and all 13 approved NSO CSVs. NSO contract/transformer/metadata hashes and all Phase 1
evidence hashes were preserved. Eight CLOSED source business files and registry
were not changed. Git HEAD remains `5c571693f90101c55d00ff84ceb5138092a1263c`.

## Actual failure and persisted state

Project `tlcn-local-nine-airflow-20261016` was unused before creation. Compose
created its own internal network and four persistent volumes; no shared volumes
or container names, no tmpfs, Raw/workspace readonly. The initializer completed
Airflow metadata migration, local test-user/bootstrap and warehouse-bucket setup.
Both DAGs were paused; there was no ingestion schema or Iceberg data table.

Failed webserver:

- Container: `d2b68f6bcdd28d1fd1e3d82b507cacf251a42b376d5ce1740073a1c9aedc69c6`.
- Immutable Airflow image: `tlcn/silver-airflow@sha256:6748e6464ed7231f5ff35bf6fe10c6e0929dc2016b9687b28c5056b85ed4cb96`.
- Memory limit: 536,870,912 bytes; final exit1; OOMKilled=true; restart count0.
- Kernel: `constraint=CONSTRAINT_MEMCG`, matching this exact container cgroup,
  followed by `Memory cgroup out of memory: Killed process ... (python)`.
- Docker event query returned no retained matching events; the conclusion relies
  on inspect and matching kernel records, not reconstructed events.

See [runtime-failure.json](runtime-failure.json) and [airflow-webserver.log.gz](airflow-webserver.log.gz).
This was a container-limit OOM, not evidence of another global WSL OOM. No writer
commit is unknown: [airflow-post-failure.json](airflow-post-failure.json) verifies
zero DagRuns, zero TaskInstances, zero ingestion tables and zero advisory locks.
[Catalog readback](empty-catalog-readback.json) verifies zero Iceberg tables and
healthy Trino. Thus no source task, batch gate or target writer has executed.

Both DAGs remain paused, schedule=None. Six other isolated services remain
running for inspection; webserver remains failed/exited, initializer exited0.
No restart/recreation/retry was attempted after the OOM. Volumes and logs remain.
The Spark API has only its idle Python process, no writer JVM.

All seven shared containers remain stopped with original IDs/stop timestamps;
their volumes and historical 29-table/29-ingestion-run data baseline were not
opened by this project or modified. The Phase 1 and historical interrupted REST
projects remain untouched. No shared restart or production-parity restoration.

At the last sample Windows available RAM was approximately 3.60 GiB and WSL
MemAvailable 5.94 GiB, swap0. This supports the matching cgroup-limit diagnosis;
it does not prove a proposed larger container limit will pass. See
[resources-after-failure.json](resources-after-failure.json).

## Remaining work requiring owner review

The proposed un-applied change is isolated webserver512 MiB → 1 GiB and
Spark runner3 GiB → 2.5 GiB, retaining driver768 MiB/local[2] and the aggregate
7.1875 GiB steady-service cap. Recreate only those two isolated services after
fresh gates; preserve the failure evidence. Do not start shared or rerun init.

Only after actual health/parse/resource gates pass should the approved one Bronze
manual run occur and automatically trigger one Silver run. Actual run IDs,
source outcomes, target counts/keys/DQ/lineage/Trino and full-input replay evidence
are **not available yet**. Expected total1,414,527 is not an observed result.

NSO production contract remains DRAFT. No READY approval is requested at this
blocked stage; the existing local-only overlay suffices for the future test.
No Phase 3–5, data reset/deletion, Gold, consumer change, commit or push occurred.
Stopping here follows the owner's Phase 2 section8 instruction to stop/report
when resource failure occurs and retain logs/failed state.
