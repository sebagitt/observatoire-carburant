# AIRFLOW_VERSION doit correspondre à l'image du docker-compose.yml (3.0.6 au J1)
ARG AIRFLOW_VERSION=3.0.6
FROM apache/airflow:${AIRFLOW_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION=3.12
COPY airflow/requirements.txt /tmp/requirements.txt
# Le fichier de contraintes d'Airflow évite qu'un pip install ne casse ses dépendances
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
# Venv séparé pour dbt (G1 de J10) : les contraintes d'Airflow 3.0.6 ne s'y appliquent pas
USER root
RUN mkdir -p /opt/dbt-venv && chown airflow:0 /opt/dbt-venv
USER airflow
RUN python -m venv /opt/dbt-venv \
    && PIP_USER=false /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
