import requests
import json
import os
from datetime import datetime

# URL base da API
BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
# Caminho da pasta Bronze mapeada dentro do container
BRONZE_DIR = "/opt/airflow/data/bronze"

def fetch_and_save_breweries():
    print("Iniciando a extração de dados da API Open Brewery DB...")
    all_breweries = []
    page = 1
    per_page = 50 

    while True:
        print(f"Buscando página {page}...")
        response = requests.get(f"{BASE_URL}?per_page={per_page}&page={page}")
        
        # O raise_for_status é vital: se a API cair (ex: erro 500), 
        # ele quebra o script e avisa o Airflow para acionar o "retry".
        response.raise_for_status() 
        
        data = response.json()
        
        # A API retorna uma lista vazia quando a paginação acaba
        if not data:
            print("Nenhum dado retornado. Fim da paginação.")
            break
        
        all_breweries.extend(data)
        page += 1

    # Garante que a pasta existe
    os.makedirs(BRONZE_DIR, exist_ok=True)
    
    # Salva com a data atual para garantir a idempotência diária
    date_str = datetime.now().strftime("%Y%m%d")
    file_path = os.path.join(BRONZE_DIR, f"breweries_{date_str}.json")
    
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(all_breweries, f, ensure_ascii=False, indent=4)
        
    print(f"Extração concluída com sucesso! {len(all_breweries)} registros salvos em {file_path}")

if __name__ == "__main__":
    fetch_and_save_breweries()