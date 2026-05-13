FROM apache/superset:4.0.2

USER root
RUN pip install --no-cache-dir sqlalchemy-trino==0.5.0 trino==0.329.0
USER superset
