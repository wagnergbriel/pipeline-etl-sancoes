from os import listdir
from os.path import join
from datetime import datetime
import pandas as pd
import os
import re

class TransformDataSancoes():
    def __init__(self, name_folder_data_raw, name_folder_inserts):
        self.folder_path = f"./data_raw/{name_folder_data_raw}"
        self.folder_inserts = f"./sql/inserts/{name_folder_inserts}"
        self.url_format_link = "https://portaldatransparencia.gov.br"
        self.date_time = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    
    def get_file_datas(self):
        file = listdir(self.folder_path)
        latest_file = max(file, key=lambda f: os.path.getmtime(os.path.join(self.folder_path, f)))
        path_full_file = join(self.folder_path, latest_file)
        return path_full_file, latest_file
    
    def create_insert_datas(self, df):
        file_name = f"{self.name_folder_inserts}_{self.datetime_now}.json"
        path_inserts_full = join(self.folder_inserts, file_name)
        with open(path_inserts_full, 'w') as file_sql:
            for index, line in df.iterrows():
                #---- criar condicao para cada sancao -------
                # Construir a instrução SQL de inserção
                if name_folder_inserts == 'acordo_leniencia':
                    sql_insert_datas = f"INSERT INTO {tabela} (id, nome, idade)\
                                        VALUES ({line['id']}, '{line['nome']}', {line['idade']})\
                                        ON CONFLICT (id) DO UPDATE\
                                        SET nome = EXCLUDED.nome,\
                                        idade = EXCLUDED.idade;\n"
                    #Escrever a instrução SQL no arquivo
                    file_sql.write(sql_insert_datas)
                
                elif name_folder_inserts == '':
                    pass
                
                elif name_folder_inserts == '':
                    pass
                
                elif name_folder_inserts == '':
                    pass

        print(f"As inserções SQL foram escritas em '{file_name}'.")
    
    def verifier_cpf_or_cnpj(self, value):
        cpf_pattern = re.compile(r'\b\d{3}\.\d{3}\.\d{3}-\d{2}\b')
        cpfs_find = cpf_pattern.findall(value)
        if len(cpfs_find) > 0:
          return True
        else:
          return False

    def transform_datas_ceis(self):
        path_full_file, file = self.get_file_datas()
        df_ceis_raw = pd.read_json(path_full_file, encoding="utf-8").to_dict("records")
        data_ceis = list(map(lambda x: x["data"], df_ceis_raw))
        df_ceis = pd.DataFrame(data_ceis)

        df_ceis = df_ceis[['skSancao', 'cadastro','cpfCnpj', 'nomeSancionado', 'ufSancionado', 'categoriaSancao', 'orgao','dataPublicacao', 'linkDetalhamento']]

        df_ceis["TipoDePessoa"] = df_ceis['cpfCnpj'].apply(lambda x: self.verifier_cpf_or_cnpj(x))
        df_ceis["TipoDePessoa"] = df_ceis["TipoDePessoa"].map({True: "F", False: "J"})
        df_ceis['cpfCnpjFormatado'] = df_ceis['cpfCnpj'].apply(lambda x: str(re.sub(r'\D', '', x)))
        df_ceis['linkDetalhamento'] = df_ceis['linkDetalhamento'].apply(lambda x: self.url_format_link + x)
        df_ceis["DataDaCarga"] = self.date_time
        df_ceis["ArquivoFonte"] = file
        print(df_ceis)

    def transform_datas_cepim(self):
        path_full_file, file = self.get_file_datas()
        df_cepim_raw = pd.read_json(path_full_file, encoding="utf-8").to_dict("records")
        data_cepim = list(map(lambda x: x["data"], df_cepim_raw))
        df_cepim = pd.DataFrame(data_cepim)

        df_cepim = df_cepim[['skFatCepim', 'cadastro','cpfCnpj', 'nomeSancionado', 'ufSancionado', 'categoriaSancao', 'orgao','dataPublicacao', 'linkDetalhamento']]
        df_cepim["TipoDePessoa"] = df_cepim['cpfCnpj'].apply(lambda x: self.verifier_cpf_or_cnpj(x))
        df_cepim["TipoDePessoa"] = df_cepim["TipoDePessoa"].map({True: "F", False: "J"})
        df_cepim['cpfCnpjFormatado'] = df_cepim['cpfCnpj'].apply(lambda x: str(re.sub(r'\D', '', x)))
        df_cepim['linkDetalhamento'] = df_cepim['linkDetalhamento'].apply(lambda x: self.url_format_link + x)
        df_cepim["DataDaCarga"] = self.date_time
        df_cepim["ArquivoFonte"] = file
        print(df_cepim)

    def transform_datas_cnep(self):
        path_full_file, file = self.get_file_datas()
        df_cnep_raw = pd.read_json(path_full_file, encoding="utf-8").to_dict("records")
        data_cnep = list(map(lambda x: x["data"], df_cnep_raw))
        df_cnep = pd.DataFrame(data_cnep)

        df_cnep = df_cnep[['skSancao', 'cadastro','cpfCnpj', 'nomeSancionado', 'ufSancionado', 'categoriaSancao', 'orgao','dataPublicacao', 'linkDetalhamento']]
        df_cnep["TipoDePessoa"] = df_cnep['cpfCnpj'].apply(lambda x: self.verifier_cpf_or_cnpj(x))
        df_cnep["TipoDePessoa"] = df_cnep["TipoDePessoa"].map({True: "F", False: "J"})
        df_cnep['cpfCnpjFormatado'] = df_cnep['cpfCnpj'].apply(lambda x: str(re.sub(r'\D', '', x)))
        df_cnep['linkDetalhamento'] = df_cnep['linkDetalhamento'].apply(lambda x: self.url_format_link + x)
        df_cnep["DataDaCarga"] = self.date_time
        df_cnep["ArquivoFonte"] = file
        print(df_cnep)

    def transform_datas_acordo_leniencia(self):
        path_full_file, file = self.get_file_datas()
        df = pd.read_json(path_full_file).to_dict("records")
        datas_format = []
        for data in df:
            new_dict = {}

            # Copie os campos fora da lista
            new_dict["id"] = data["id"]
            new_dict["dataInicioAcordo"] = data["dataInicioAcordo"]
            new_dict["dataFimAcordo"] = data["dataFimAcordo"]
            new_dict["orgaoResponsavel"] = data["orgaoResponsavel"]
            new_dict["situacaoAcordo"] = data["situacaoAcordo"]

            # Crie uma lista de dicionários para armazenar os dados da lista "sancoes"
            sancoes = data.get("sancoes")
            for data_sancoes in sancoes:
              dict_extend = dict(new_dict, **data_sancoes)
              datas_format.append(dict_extend)

        new_df = pd.DataFrame(datas_format)
        new_df["DataDaCarga"] = self.date_time
        new_df["ArquivoFonte"] = file
        print(new_df)



path_datas_ceis = 'acordo_leniencia'
test = TransformDataSancoes(name_folder_data_raw=path_datas_ceis, name_folder_inserts=path_datas_ceis)
test.transform_datas_acordo_leniencia()