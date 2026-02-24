import requests
import pprint
import os
from dotenv import load_dotenv #busca a chave salva no arquivo .env

load_dotenv(override=True)

chave_api= os.getenv("chave_api") # chave protegida em .env
link_api = "http://api.weatherapi.com/v1/current.json"

parametros = {
    "key": chave_api,
    "q" : "Manaus",
    "lang": "pt"
}

retorno = requests.get(link_api,params=parametros)

if retorno.status_code == 200:
    dados_retorno = retorno.json()
    pprint.pprint(dados_retorno)
    temp = dados_retorno["current"]["temp_c"]
    print(f"Em Manaus, a temperatura atual é de:{temp}")

else:
    print(f"Erro na requisição: {retorno.status_code}")
    print(retorno.text) # Mostra o motivo do erro (ex: chave inválida)
