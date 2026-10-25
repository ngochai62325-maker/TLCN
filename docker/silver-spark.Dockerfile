FROM tabulario/spark-iceberg@sha256:790a72e744e0d09f161e1b4c7c2f79625cc598cf0476da4703e62f1f4e023460
USER root
COPY requirements/silver-spark.lock /opt/silver-runtime/requirements.lock
RUN python3 -m pip install --no-cache-dir --disable-pip-version-check --no-deps --require-hashes -r /opt/silver-runtime/requirements.lock \
    && python3 -c "import importlib.metadata as m; import psycopg2, pyiceberg, pyarrow, pandas, yaml; expected={'psycopg2-binary':'2.9.10','pyiceberg':'0.7.1','pyarrow':'17.0.0','pandas':'2.2.3','PyYAML':'6.0.2'}; assert all(m.version(k)==v for k,v in expected.items()), expected"
ENV PYTHONPATH=/home/iceberg/src:/home/iceberg/jobs:/home/iceberg/jobs/silver
LABEL org.opencontainers.image.description="Silver V2 Spark execution dependencies; no credentials or startup installation"
