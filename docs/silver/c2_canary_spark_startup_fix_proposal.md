# C2 Spark startup fix proposal - NOT APPLIED

Current canary status BLOCKED. Conditional approval authorized startup using the existing Compose configuration and shared write only after a complete preflight PASS. The existing configuration is incompatible with the image entrypoint, so no repair or alternate startup is executed in this C2 run. This document gives a concrete repair for review.

## Diagnosed behavior

[Container evidence](c2_canary_spark_failure_evidence.json) shows `Cmd=[bash,-c,script]` and entrypoint `./entrypoint.sh`. The [actual image entrypoint](c2_spark_entrypoint_evidence.txt) starts cluster services and then evaluates only its first argument. It therefore evaluates `bash`, ignoring the remaining `-c` and API/Jupyter script. Noninteractive bash exits, API is never started, and the container exits 0 after about eleven seconds. This is not evidence that the Spark job succeeded. HistoryServer additionally reports a missing `file:/tmp/spark-events` directory. Container did not OOM.

## Recommended exact configuration change for review

Change only the `spark-iceberg` startup stanza in docker-compose.yml; preserve current image, environment, ports, network and mounts. Run an outer shell that prepares the local event directory, starts the existing API, then invokes the image's entrypoint with its supported single `notebook` argument. This preserves the intended notebook and cluster service startup alongside the API.

```yaml
    entrypoint: ["/bin/bash", "-lc"]
    command:
      - |
        set -e
        mkdir -p /tmp/spark-events
        python3 /home/iceberg/jobs/silver/spark_api_server.py &
        exec /opt/spark/entrypoint.sh notebook
```

This is a proposal, not a verified/applied patch. Validate the exact argv and startup first in an isolated container, then restart only Spark with `docker compose up -d --no-deps spark-iceberg` after approval of the startup change. Do not recreate any other service. Check API health from scheduler and mounted contract/code hashes, and rerun the entire native and Spark read-only preflight before namespace or table creation. Runtime master local[2], driver 2g and shuffle partitions 4 still need live confirmation.

Use the scheduler-held Production source lock for every API write call; record its PostgreSQL backend/lock throughout the job and inspect uncertain commit outcomes before releasing/retrying. The standalone API has no implicit source lock. No Airflow task/status update is part of the canary. Existing approval remains limited to Production; seven other business sources and NSO remain out of write scope.
