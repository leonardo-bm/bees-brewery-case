import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

SILVER_DIR = "/opt/airflow/data/silver"
GOLD_DIR = "/opt/airflow/data/gold"

def process_gold():
    print("Iniciando processamento da camada Gold...")
    
    spark = SparkSession.builder \
        .appName("Brewery_Gold_Layer") \
        .master("local[*]") \
        .getOrCreate()
    
    # Lê os dados limpos e tipados da camada Silver
    df_silver = spark.read.parquet(SILVER_DIR)
    
    if os.path.exists(GOLD_DIR):
        print(f"Limpando o diretório {GOLD_DIR}...")
        shutil.rmtree(GOLD_DIR)
        
    print("Criando a view agregada: quantidade de cervejarias por tipo e local...")
    
    # A agregação exigida pelo case da Ambev/BEES
    df_gold = df_silver.groupBy("country", "state", "brewery_type") \
        .agg(count("*").alias("brewery_count"))
        
    # Salvando a tabela final que seria consumida por um dashboard
    df_gold.repartition(1).write \
        .mode("overwrite") \
        .parquet(GOLD_DIR)
        
    print("Processamento da camada Gold concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    process_gold()