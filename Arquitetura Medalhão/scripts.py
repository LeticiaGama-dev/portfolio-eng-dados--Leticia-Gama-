#docker-compose.yml
services:
  db:
    image: postgres:16
    restart: always
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: '1234'
      POSTGRES_HOST_AUTH_METHOD: trust
    ports:
      - "5444:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:


#populate.db carrega os dados processados para o banco de dados

from db import DB
import pandas as pd
import os

db = DB(host="localhost", port=5444, database="postgres", user="postgres", password="1234")

# Primeiro guardamos a lista de arquivos nesta variável 'files'
files = os.listdir("02-silver-validated")
print(f"Arquivos encontrados na pasta: {files}")

print(f"Eu estou trabalhando nesta pasta: {os.getcwd()}")
print(f"Conteúdo total desta pasta: {os.listdir('.')}")

for file in files:
    print(f"Processando agora o arquivo: {file}")
    df = pd.read_parquet(f"02-silver-validated/{file}")

    db.create_table(
        file.replace(".parquet", ""),
        df.columns.tolist()
    )

    db.insert_data(
        file.replace(".parquet", ""),
        df
    )


#get_data.py


import requests
import pandas as pd

def get_data(cep):
    endpoint =f'https://viacep.com.br/ws/{cep}/json/'

    response = requests.get(endpoint)
    cep_info = response.json()

    return cep_info


users_path = 'exercicio/01-bronze-raw/users.csv'
users_df = pd.read_csv(users_path)

cep_lists = users_df['cep'].tolist() #to list é um metodo do panda que transforma uma coluna e lista

cep_info_list = []

for cep in cep_lists:
    cep_info = get_data(cep.replace("-",""))
    print(cep_info)
    if "erro" in cep_info:
        continue
    cep_info_list.append(cep_info)

cep_info_df = pd.DataFrame(cep_info_list)

cep_info_df.to_csv('exercicio/01-bronze-raw/users.csv', index = False)


#normalize_data.py

import os
import pandas as pd

class NormalizeData:
    def __init__(self,input_dir,output_dir):
        self.input_dir = input_dir
        self.output_dir = outtput_dir
        os.makedirs(output_dir,exist_ok=True)#garante que a pasta de saída existe

    def normalize_data(self):
        for file in os.listdir(self.input_dir):
            input_path = os.path.join(self.input_dir,file)
            name,ext = os.path.join(self.output_dir,f'{name}.parquet')

            if ext.lower() == '.csv':
                df = pd.read_csv(input_path)
            elif ext.lower() == '.json':
                #tenta ler como lista de objetos
                try:
                    df = pd.read_json(input_path)
                except ValueError:
                    df = pd.read_json(input_path, lines+True)
            else:
                print(f'Arquivo {file} ignorado (formato não suportado)')
                continue

        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x,list)).any():
                df[col].apply(lambda x: str(x) if isinstance(x,list) else x)

        df = df.drop_duplicates().reset_index(drop=True)
    
        df.to_parquet(output_path, index=False)
        print(f'Arquivo {file} normalizado e salvo como {output_path}')


#db.py

import os
import pandas as pd
import psycopg2

class DB:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        # A conexão usa as variáveis que definimos acima
        self.conn = psycopg2.connect(
            host=self.host, 
            port=self.port, 
            database=self.database, 
            user=self.user, 
            password=self.password
        )

    def create_table(self, table_name, columns):
        cursor = self.conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
       
        cols_with_types = ", ".join([f"{col} TEXT" for col in columns])
        cursor.execute(f"CREATE TABLE {table_name} ({cols_with_types})")
        
        self.conn.commit()
        cursor.close()

    def insert_data(self, table_name, df):
        cursor = self.conn.cursor()
        
        # Cria a lista de placeholders (%s) baseada no número de colunas
        placeholders = ", ".join(["%s"] * len(df.columns))
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        
        # Converte o DataFrame em uma lista de tuplas para o banco entender
        data_to_insert = [tuple(x) for x in df.values]
        
        # Insere tudo de uma vez de forma segura
        cursor.executemany(query, data_to_insert)
        
        self.conn.commit()
        cursor.close()

    def execute_query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        res = cursor.fetchall()
        cursor.close()
        return res

    def select_all_data_from_table(self, table_name):
        query = f"SELECT * FROM {table_name}"
        return self.execute_query(query)

    def close(self):
        self.conn.close()

if __name__ == "__main__":    
    db = DB(host="localhost", port=5444, database="postgres", user="postgres", password="1234")

    df = pd.DataFrame({"id": [1, 2, 3], "name": ["John", "Jane", "Jim"]})

    db.create_table("test", df.columns.tolist())
    db.insert_data("test", df)

    print(db.select_all_data_from_table("test"))
    db.close()
