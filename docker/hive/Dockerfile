FROM apache/hive:4.0.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fSL https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -o /opt/hive/lib/postgresql-42.7.3.jar \
    && chmod 644 /opt/hive/lib/postgresql-42.7.3.jar

COPY docker/hive/hive-site.xml /opt/hive/conf/hive-site.xml
COPY docker/hive/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

USER root

ENTRYPOINT ["/entrypoint.sh"]
