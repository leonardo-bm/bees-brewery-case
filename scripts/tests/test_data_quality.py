import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Caminho da nossa camada final
GOLD_DIR = "/opt/airflow/data/gold"

@pytest.fixture(scope="session")
def spark():
    """Cria uma sessão do Spark apenas para os testes"""
    return SparkSession.builder \
        .appName("Brewery_Test") \
        .master("local[*]") \
        .getOrCreate()

def test_gold_layer_is_not_empty(spark):
    """Testa se a camada Gold gerou dados e não está vazia"""
    df_gold = spark.read.parquet(GOLD_DIR)
    
    # Verifica se o total de linhas é maior que zero
    assert df_gold.count() > 0, "A tabela da camada Gold está vazia!"

def test_gold_layer_no_null_counts(spark):
    """Testa se a agregação de contagem gerou valores nulos"""
    df_gold = spark.read.parquet(GOLD_DIR)
    
    # Filtra para ver se existe alguma linha com a contagem nula
    null_counts = df_gold.filter(col("brewery_count").isNull()).count()
    
    # O teste passa se o número de nulos for exatamente zero
    assert null_counts == 0, f"Encontrados {null_counts} valores nulos na coluna brewery_count!"