FROM apache/airflow:2.8.1-python3.10

USER root

# Instala o Java (Obrigatório para o PySpark rodar)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Instala o PySpark, requests (para a API) e o pytest
RUN pip install --no-cache-dir pyspark==3.5.0 pytest==8.0.0 requests==2.31.0