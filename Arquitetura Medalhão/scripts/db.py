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
