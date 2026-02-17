#normalize-data

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
