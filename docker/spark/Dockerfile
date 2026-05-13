FROM spark:3.5.8-scala2.12-java11-python3-ubuntu

USER root
COPY requirements/base.txt /tmp/requirements.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && pip3 install --no-cache-dir -r /tmp/requirements.txt

ENV PYTHONPATH=/opt/project
