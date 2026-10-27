# Reproducible test environments

Python 3.10–3.13 is supported for host unit tests. The verified Docker Spark
runtime uses Python 3.10, Spark 3.5.5 and Java 17. Python 3.13 is **not** a
supported Spark JVM test environment. Airflow stays in its existing Linux image.

From the repository root, create a virtual environment and install declared dev dependencies:

```powershell
python -m venv .venv-foundation
.venv-foundation/Scripts/python -m pip install -r requirements/test-host.txt
.venv-foundation/Scripts/python -m pytest --collect-only -q
.venv-foundation/Scripts/python -m pytest -q
```

Default collection contains unit tests without JVM/Airflow/catalog services.
PySpark's Python types/expressions are a declared dev dependency; these imports
do not start a JVM. Runtime files are excluded **before import** by the explicit
list in `tests/conftest.py`. No failure is converted to a blanket skip. Enable
runtime collection with `--with-runtime` and select the intended test files.

Windows: run from a normal user PowerShell. Pytest creates mode-700 temp paths;
the managed Codex Windows sandbox cannot re-open these paths on this host.
The same focused suites pass outside that sandbox. Do not change system ACLs.
Use `--basetemp=.test-tmp/<unique-run>` with an existing `.test-tmp` parent if
desired; never share a basetemp between concurrent runs. Linux containers use
`/tmp`, independent of host ACLs.

Isolated Spark/Iceberg fixtures (read-only repository mount, no network, ephemeral
warehouse; no live catalog credentials):

```powershell
powershell -NoProfile -File scripts/test_silver_isolated.ps1
```

This uses the existing local Spark image; it does not test the nine live sources.
Airflow DAG parsing must use the existing Airflow image, with test-only pytest
dependencies in `/tmp`, and `--with-runtime tests/unit/test_dag_structure.py
tests/unit/test_local_nine_dags.py`. Parsing is not a DagRun acceptance test.

Prefer module entrypoints for Silver tools, for example:

```powershell
python -m jobs.silver.verify_silver_trino --help
python -m pytest tests/unit/gold -q
python scripts/validate_silver_contracts.py
```

`validate_silver_e2e`, `run_all_silver` and migration tools can write data. Their
`--help`/import smoke tests are safe; do not execute real workloads for unit tests.

Runtime tests require local demo memory/admission checks first (Windows >=2 GiB
available). Do not start them at the final 0.62 GiB resource sample.

Owned REST/PostgreSQL fixtures use the existing Airflow image and test-only
dependencies in `/tmp`. Every created namespace has a random owner marker:

```powershell
docker exec -e PYTHONPATH=/tmp/foundation-test-deps:/workspace/src:/workspace tlcn-local-airflow-webserver-1 python -m pip install --no-index --no-deps --find-links /workspace/.pytest_cache/silver_wheels --target /tmp/foundation-test-deps pytest==8.4.2 pluggy==1.6.0 iniconfig==2.3.1 pygments==2.21.0
docker exec -w /workspace -e FOUNDATION_OWNED_FIXTURES=1 -e PYTHONPATH=/tmp/foundation-test-deps:/workspace/src:/workspace tlcn-local-airflow-webserver-1 python -m pytest --with-runtime tests/integration/test_foundation_runtime.py -q -p no:cacheprovider --basetemp=/tmp/foundation-owned-local
```

These are writer tests on **owned disposable resources**, not the nine real
targets. Do not run arbitrary old integration suites against the active stack or
use a legacy full-input rebuild just to prove idempotency.

The first Airflow pytest bootstrap used a `/tmp` target containing a newer
`packaging`, producing a dependency warning. Installed Airflow packages and running
processes were not changed. The command above uses `--no-deps` to retain the image's
existing compatible `packaging`.

Host originally had PySpark 4.1.2. A second complete unit run uses approved 3.5.5
Python libraries from the existing runner, without JVM or global package changes:

```powershell
New-Item -ItemType Directory -Force .test-tmp | Out-Null
docker cp tlcn-local-runner-1:/opt/spark/python/lib/pyspark.zip .test-tmp/pyspark-3.5.5.zip
docker cp tlcn-local-runner-1:/opt/spark/python/lib/py4j-0.10.9.7-src.zip .test-tmp/py4j-0.10.9.7-src.zip
$env:PYTHONPATH='.test-tmp/pyspark-3.5.5.zip;.test-tmp/py4j-0.10.9.7-src.zip;src;.'
python -m pytest tests/unit -q
Remove-Item Env:PYTHONPATH
```

Host baseline: **489 PASS**; final approved-3.5.5 API run: **490 PASS**, including the added stale-report CLI regression. The zip overlay is a compatibility check, not a
replacement for installing the declared dev environment. A separate fresh-venv
install was not executed during acceptance; no global Spark downgrade or image
upgrade was performed. PyArrow/lxml and pinned test tools are now declared explicitly.
