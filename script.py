import os
import time
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import json
from datetime import datetime
import sys

def run():
    # Carregar credenciais da conta de serviço a partir de uma variável de ambiente
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    with open(credentials_path) as f:
        service_account_info = json.load(f)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Nome do dataset e tabela
    dataset_id = 'flyon_airbyte'
    table_id = 'pesquisa_tipo_lead'

    # Função para apagar dados da tabela
    def delete_table_data(dataset_id, table_id):
        query = f"DELETE FROM `{dataset_id}.{table_id}` WHERE TRUE"
        query_job = client.query(query)
        query_job.result()  # Aguarda a conclusão da query
        print(f"Dados apagados da tabela {dataset_id}.{table_id}")
        sys.stdout.flush()

    # Função para buscar dados do endpoint
    def fetch_data_from_endpoint(initial_url, pagination_url, headers):
        all_data = []
        
        # Primeira chamada para obter os primeiros 1000 dados e o token
        response = requests.get(initial_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        all_data.extend(data)
        token = response.headers.get('X-VTEX-MD-TOKEN')  # Ajuste conforme necessário para obter o token correto
        
        # Loop para continuar buscando dados usando o token
        while token:
            paginated_response = requests.get(pagination_url.format(token=token), headers=headers)
            paginated_response.raise_for_status()
            paginated_data = paginated_response.json()
            if not paginated_data:
                break
            all_data.extend(paginated_data)
            token = paginated_response.headers.get('X-VTEX-MD-TOKEN')
        
        return all_data

    # Função para adicionar o campo emitedAt aos dados
    def add_emitedAt_field(data):
        emited_at = datetime.utcnow().isoformat() + 'Z'  # Formato UTC com sufixo 'Z'
        for record in data:
            record['emitedAt'] = emited_at
        return data

    # Função para inserir dados na tabela
    def insert_data_to_table(dataset_id, table_id, data):
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        errors = client.insert_rows_json(table, data)
        if errors:
            raise Exception(f"Erro ao inserir dados: {errors}")
        print(f"Dados inseridos na tabela {dataset_id}.{table_id}")
        sys.stdout.flush()

    # URLs do endpoint
    initial_endpoint_url = 'https://eutbem.myvtex.com/api/dataentities/QO/scroll?_fields=OpcaoConsulta,id,createdIn&_sort=createdIn%20ASC&_size=1000'
    pagination_endpoint_url = 'https://eutbem.myvtex.com/api/dataentities/QO/scroll?_token={token}'

    # Headers de autenticação
    headers = {
        'X-VTEX-API-AppKey': 'vtexappkey-eutbem-CBLDIW',
        'X-VTEX-API-AppToken': 'BTFHHAZEATMXGZZGZYRGWUWQNEEABHYAVCCRWLQGVTLCHMMLKQAUHOMSLVSEIORYKZLNSUEXIAHFBVKMTFOXIXCEUMVYGAQQDISTGQTYDBPYGOATQDQFOGZKYBFXUFML'
    }

    # Processo principal
    try:
        # Apagar dados da tabela
        delete_table_data(dataset_id, table_id)
        
        # Buscar dados do endpoint
        data = fetch_data_from_endpoint(initial_endpoint_url, pagination_endpoint_url, headers)
        
        # Adicionar o campo emitedAt aos dados
        data = add_emitedAt_field(data)
        
        # Inserir dados na tabela
        insert_data_to_table(dataset_id, table_id, data)
        
        print("Processo concluído com sucesso!")
        sys.stdout.flush()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        sys.stdout.flush()

if __name__ == "__main__":
    print("Iniciando o loop infinito para executar o script a cada 3 horas.")
    sys.stdout.flush()  # Garante que a saída é escrita no log imediatamente
    while True:
        run()
        print("Aguardando 3 horas para a próxima execução...")
        sys.stdout.flush()  # Garante que a saída é escrita no log imediatamente
        time.sleep(10800)  # Espera 3 horas (10800 segundos) antes de executar novamente
