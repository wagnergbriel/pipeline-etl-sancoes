import config
import requests

class SancoesAPI:
    def __init__(self):
        self.base_url = 'https://portaldatransparencia.gov.br/sancoes/consulta/resultado?paginacaoSimples=true&tamanhoPagina=100000&offset=0&direcaoOrdenacao=asc&colunaOrdenacao=nomeSancionado&cadastro=2&colunasSelecionadas=linkDetalhamento%2Ccadastro%2CcpfCnpj%2CnomeSancionado%2CufSancionado%2Corgao%2CcategoriaSancao%2CdataPublicacao%2CvalorMulta%2Cquantidade&_=1695667939978'
        self.headers = config.HEADERS
    
    def get_sancoes(self):
        response = requests.get(self.base_url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Falha na solicitação. Código de status: {response.status_code}")
            return None

# Exemplo de uso:
api = SancoesAPI()
sancoes = api.get_sancoes()
if sancoes:
    print(sancoes)
