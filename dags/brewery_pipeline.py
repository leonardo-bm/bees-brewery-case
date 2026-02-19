from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configurações padrão mostrando boas práticas de engenharia (como retries em caso de falha)
default_args = {
    'owner': 'leonardo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3, # Tenta novamente 3 vezes se a API cair
    'retry_delay': timedelta(minutes=2), # Espera 2 minutos entre as tentativas
}

# Definição da DAG
with DAG(
    'brewery_medallion_pipeline',
    default_args=default_args,
    description='Pipeline ETL - BEES Data Engineering Case',
    schedule=timedelta(days=1), # Atualizado para o padrão novo do Airflow 3
    start_date=datetime(2026, 2, 18),
    catchup=False,
    tags=['bees', 'medallion', 'pyspark'],
) as dag:

    # Tarefa 1: Extração da API (Camada Bronze)
    extract_bronze = BashOperator(
        task_id='extract_api_to_bronze',
        bash_command='python /opt/airflow/scripts/bronze.py',
    )

    # Tarefa 2: Transformação com PySpark (Camada Silver)
    transform_silver = BashOperator(
        task_id='transform_bronze_to_silver',
        bash_command='python /opt/airflow/scripts/silver.py',
    )

    # Tarefa 3: Agregação final com PySpark (Camada Gold)
    transform_gold = BashOperator(
        task_id='transform_silver_to_gold',
        bash_command='python /opt/airflow/scripts/gold.py',
    )

    # Definindo a ordem do pipeline (Dependências)
    extract_bronze >> transform_silver >> transform_gold