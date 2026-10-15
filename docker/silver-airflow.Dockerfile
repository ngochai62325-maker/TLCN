FROM apache/airflow@sha256:5afa55b7c6087112770605f9b9851afd665f5c7f3719b390fcb4bd28a47a7f9a
USER airflow
COPY requirements/silver-airflow.lock /opt/silver-runtime/requirements.lock
RUN python3 -m pip install --no-cache-dir --disable-pip-version-check --no-deps --require-hashes -r /opt/silver-runtime/requirements.lock \
    && python3 -c "import importlib.metadata as m; import airflow, psycopg2, pyiceberg, pyarrow, pandas, yaml, openpyxl, lxml.etree; expected={'apache-airflow':'2.10.4','psycopg2-binary':'2.9.10','pyiceberg':'0.12.0','pyarrow':'16.1.0','pandas':'2.1.4','PyYAML':'6.0.2','openpyxl':'3.1.5','lxml':'5.3.0'}; assert all(m.version(k)==v for k,v in expected.items()), expected"
ENV _PIP_ADDITIONAL_REQUIREMENTS=""
LABEL org.opencontainers.image.description="Silver V2 Airflow execution dependencies; no credentials or startup installation"
