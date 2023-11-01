from urllib.parse import urljoin
from os.path import join
from datetime import datetime
from config import HEADERS, KEY_API
import requests
import json

class SancoesAPI:
    def __init__(self, name_sancao):
        self.type_sancao = {"ceis":1, "cnep":2, "cepim":5}
        self.base_url = f'https://portaldatransparencia.gov.br/sancoes/consulta/resultado?paginacaoSimples=true&tamanhoPagina=100000&offset=0&direcaoOrdenacao=asc&colunaOrdenacao=nomeSancionado&cadastro={self.type_sancao.get(name_sancao)}&colunasSelecionadas=linkDetalhamento%2Ccadastro%2CcpfCnpj%2CnomeSancionado%2CufSancionado%2Corgao%2CcategoriaSancao%2CdataPublicacao%2CvalorMulta%2Cquantidade&_=1695667939978'
        self.headers = HEADERS
        self.datetime_now = datetime.now().strftime("%m%Y")
    
    def get_sancoes(self):
        response = requests.get(self.base_url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Falha na solicitação. Código de status: {response.status_code}")
            return None
    
    def save_json_file(self, dataset, path_dir, file_name):
        self.file_name = f"{file_name}_{self.datetime_now}.json"
        path_full = join(path_dir, self.file_name)
        with open(path_full, 'w', encoding='utf-8') as file_json:
            json.dump(dataset, file_json, ensure_ascii=False, indent=4)
        print(f"Create file Json in {path_full}")

class PortalTransparenciaAPI():
    def __init__(self, type_database):
        self.type_database = type_database
        self.url = f"https://api.portaldatransparencia.gov.br/api-de-dados/{self.type_database}"
        self.datetime_now = datetime.now().strftime("%m%Y")

    def get_all_data(self):
        number_page = 1
        datas_full = []
        
        while True:
            url_full_all_data = urljoin(self.url, f"?pagina={number_page}")
            print(f"Url will be connect: {url_full_all_data}")
            response = requests.get(url_full_all_data, headers={"chave-api-dados":KEY_API})
            if response.status_code == 200:
                datas = response.json()
                if len(datas) == 0:
                    break
                else:
                    datas_full.append(datas[0])
                    number_page += 1
            else:
                print(f"Falha na solicitação. Código de status: {response.status_code}")
                break
        return datas_full

    def save_json_file(self, dataset, path_dir, file_name):
        self.file_name = f"{file_name}_{self.datetime_now}.json"
        path_full = join(path_dir, self.file_name)
        with open(path_full, 'w', encoding='utf-8') as file_json:
            json.dump(dataset, file_json, ensure_ascii=False, indent=4)
        print(f"Create file Json in {path_full}")
        