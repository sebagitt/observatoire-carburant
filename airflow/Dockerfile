# AIRFLOW_VERSION doit correspondre à l'image du docker-compose.yml (3.0.6 au J1)
ARG AIRFLOW_VERSION=3.0.6
FROM apache/airflow:${AIRFLOW_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION=3.12
COPY airflow/requirements.txt /tmp/requirements.txt
# Le fichier de contraintes d'Airflow évite qu'un pip install ne casse ses dépendances
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
