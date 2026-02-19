import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

BRONZE_DIR = "/opt/airflow/data/bronze"
SILVER_DIR = "/opt/airflow/data/silver"

def process_silver():
    print("Iniciando processamento da camada Silver...")
    
    spark = SparkSession.builder \
        .appName("Brewery_Silver_Layer") \
        .master("local[*]") \
        .getOrCreate()
    
    # Lê os dados em formato multiline JSON
    df = spark.read.option("multiline", "true").json(f"{BRONZE_DIR}/*.json")
    
    # Adiciona data de carga
    df_silver = df.withColumn("_load_date", current_date())
    
    if os.path.exists(SILVER_DIR):
        print(f"Limpando o diretório {SILVER_DIR}...")
        shutil.rmtree(SILVER_DIR)
        
    print("Escrevendo dados na camada Silver...")
    
    # IMPORTANTE: Para o ambiente local (Docker no Windows), a gravação particionada 
    # gera conflitos de I/O (FileAlreadyExistsException) nas pastas _temporary.
    # Em um ambiente cloud real (S3/ADLS) ou no Databricks, usaríamos o código abaixo:
    #
    # df_silver.write.mode("overwrite").partitionBy("country", "state").parquet(SILVER_DIR)
    #
    # Para garantir a execução local deste case, a gravação será feita num único ficheiro Parquet.
    
    df_silver.repartition(1).write \
        .mode("overwrite") \
        .parquet(SILVER_DIR)
        
    print("Processamento da camada Silver concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    process_silver()