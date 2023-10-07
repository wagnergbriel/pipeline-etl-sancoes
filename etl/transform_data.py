from os import listdir
from os.path import join
from datetime import datetime
import pandas as pd
import os
import re

class TransformDataSancoes():
    def __init__(self, name_folder_data_raw, name_folder_inserts):
        self.name_folder_inserts = name_folder_inserts
        self.folder_path = f"./data_raw/{name_folder_data_raw}"
        self.folder_inserts = f"./sql/inserts/{self.name_folder_inserts}"
        self.url_format_link = "https://portaldatransparencia.gov.br"
        self.date_time = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.month_year = datetime.now().strftime("%m%Y")
    
    def get_file_datas(self):
        file = listdir(self.folder_path)
        latest_file = max(file, key=lambda f: os.path.getmtime(os.path.join(self.folder_path, f)))
        path_full_file = join(self.folder_path, latest_file)
        return path_full_file, latest_file
    
    def create_insert_datas(self, df):
        list_data_insert = []
        file_name = f"{self.name_folder_inserts}_{self.month_year}.sql"
        path_inserts_full = join(self.folder_inserts, file_name)
        for line in df.to_dict("records"):
            if self.name_folder_inserts == 'acordo_leniencia':
                insert = f"""INSERT INTO sancoes.acordo_leniencia (id, dataInicioAcordo, dataFimAcordo, orgaoResponsavel, situacaoAcordo, nomeInformadoOrgaoResponsavel, razaoSocial, nomeFantasia, cnpj, cnpjFormatado, DataDaCarga, ArquivoFonte) VALUES\
                    ({line['id']},'{line['dataInicioAcordo']}', '{line['dataFimAcordo']}', '{line['orgaoResponsavel']}', '{line['situacaoAcordo']}', '{line['nomeInformadoOrgaoResponsavel']}', '{line['razaoSocial']}', '{line['nomeFantasia']}', '{line['cnpj']}', '{line['cnpjFormatado']}', '{line['DataDaCarga']}', '{line['ArquivoFonte']}')\
                        ON CONFLICT (skAcordoLeniencia) DO UPDATE
                                    SET dataInicioAcordo = EXCLUDED.dataInicioAcordo,
                                    dataFimAcordo = EXCLUDED.dataFimAcordo,
                                    orgaoResponsavel = EXCLUDED.orgaoResponsavel,
                                    situacaoAcordo = EXCLUDED.situacaoAcordo,
                                    nomeInformadoOrgaoResponsavel = EXCLUDED.nomeInformadoOrgaoResponsavel,
                                    razaoSocial = EXCLUDED.razaoSocial,
                                    nomeFantasia = EXCLUDED.nomeFantasia,
                                    cnpj = EXCLUDED.cnpj,
                                    cnpjFormatado = EXCLUDED.cnpjFormatado,
                                    DataDaCarga = EXCLUDED.DataDaCarga,
                                    ArquivoFonte = EXCLUDED.ArquivoFonte;\n"""
                list_data_insert.append(insert)
            elif self.name_folder_inserts in ['ceis', 'cnep']:
                insert = f"""INSERT INTO sancoes.{self.name_folder_inserts} (skSancao, cadastro, cpfCnpj, nomeSancionado, ufSancionado, categoriaSancao, orgao, dataPublicacao, linkDetalhamento, TipoDePessoa, cpfCnpjFormatado, DataDaCarga, ArquivoFonte) VALUES\
                    ({line['skSancao']},'{line['cadastro']}', '{line['cpfCnpj']}', '{line['nomeSancionado']}', '{line['ufSancionado']}', '{line['categoriaSancao']}', '{line['orgao']}', '{line['dataPublicacao']}', '{line['linkDetalhamento']}', '{line['TipoDePessoa']}', '{line['cpfCnpjFormatado']}', '{line['DataDaCarga']}', '{line['ArquivoFonte']}')\
                        ON CONFLICT (skSancao) DO UPDATE
                                SET cadastro = EXCLUDED.cadastro,
                                cpfCnpj = EXCLUDED.cpfCnpj,
                                nomeSancionado = EXCLUDED.nomeSancionado,
                                ufSancionado = EXCLUDED.ufSancionado,
                                categoriaSancao = EXCLUDED.categoriaSancao,
                                orgao = EXCLUDED.orgao,
                                dataPublicacao = EXCLUDED.dataPublicacao,
                                linkDetalhamento = EXCLUDED.linkDetalhamento,
                                TipoDePessoa = EXCLUDED.TipoDePessoa,
                                cpfCnpjFormatado = EXCLUDED.cpfCnpjFormatado,
                                DataDaCarga = EXCLUDED.DataDaCarga,
                                ArquivoFonte = EXCLUDED.ArquivoFonte;\n"""
                list_data_insert.append(insert)

            elif self.name_folder_inserts == 'cepim':
                insert = f"""INSERT INTO sancoes.{self.name_folder_inserts} (skSancao, cadastro, cpfCnpj, nomeSancionado, ufSancionado, categoriaSancao, orgao, dataPublicacao, linkDetalhamento, TipoDePessoa, cpfCnpjFormatado, DataDaCarga, ArquivoFonte) VALUES\
                    ({line['skFatCepim']},'{line['cadastro']}', '{line['cpfCnpj']}', '{line['nomeSancionado']}', '{line['ufSancionado']}', '{line['categoriaSancao']}', '{line['orgao']}', '{line['dataPublicacao']}', '{line['linkDetalhamento']}', '{line['TipoDePessoa']}', '{line['cpfCnpjFormatado']}', '{line['DataDaCarga']}', '{line['ArquivoFonte']}')\
                        ON CONFLICT (skSancao) DO UPDATE
                                SET cadastro = EXCLUDED.cadastro,
                                cpfCnpj = EXCLUDED.cpfCnpj,
                                nomeSancionado = EXCLUDED.nomeSancionado,
                                ufSancionado = EXCLUDED.ufSancionado,
                                categoriaSancao = EXCLUDED.categoriaSancao,
                                orgao = EXCLUDED.orgao,
                                dataPublicacao = EXCLUDED.dataPublicacao,
                                linkDetalhamento = EXCLUDED.linkDetalhamento,
                                TipoDePessoa = EXCLUDED.TipoDePessoa,
                                cpfCnpjFormatado = EXCLUDED.cpfCnpjFormatado,
                                DataDaCarga = EXCLUDED.DataDaCarga,
                                ArquivoFonte = EXCLUDED.ArquivoFonte;\n"""
                list_data_insert.append(insert)
            else:
                print("The insert file can't be generate!")
        
        if self.name_folder_inserts == 'acordo_leniencia':
            with open(path_inserts_full, 'w') as file_sql:
                file_sql.writelines(list_data_insert)
        else:
            with open(path_inserts_full, 'w') as file_sql:
                file_sql.writelines(list_data_insert)
        
        print(f"The SQL inserts were written in '{file_name}'.")
    
    def verifier_cpf_or_cnpj(self, value):
        cpf_pattern = re.compile(r'\b\d{3}\.\d{3}\.\d{3}-\d{2}\b')
        cpfs_find = cpf_pattern.findall(value)
        if len(cpfs_find) > 0:
          return True
        else:
          return False

    def transform_datas_sancoes(self):
        path_full_file, file = self.get_file_datas()
        df_raw = pd.read_json(path_full_file, encoding="utf-8").to_dict("records")
        data_sancoes = list(map(lambda x: x["data"], df_raw))
        df_sancoes = pd.DataFrame(data_sancoes)
        list_columns = ['skSancao', 'cadastro','cpfCnpj', 'nomeSancionado', 'ufSancionado', 'categoriaSancao', 'orgao','dataPublicacao', 'linkDetalhamento']
        if df_sancoes['skSancao'][0] == None:
            list_columns[0] = 'skFatCepim'
        df_sancoes = df_sancoes[list_columns]

        df_sancoes['nomeSancionado'] = df_sancoes['nomeSancionado'].apply(lambda x: x.replace("'", " "))
        df_sancoes['categoriaSancao'] = df_sancoes['categoriaSancao'].apply(lambda x: x.replace("'", " "))
        df_sancoes['orgao'] = df_sancoes['orgao'].apply(lambda x: x.replace("'", " "))
        df_sancoes['ufSancionado'] = df_sancoes['ufSancionado'].apply(lambda x: x.replace("-1", " "))
        df_sancoes["TipoDePessoa"] = df_sancoes['cpfCnpj'].apply(lambda x: self.verifier_cpf_or_cnpj(x))
        df_sancoes["TipoDePessoa"] = df_sancoes["TipoDePessoa"].map({True: "F", False: "J"})
        df_sancoes['cpfCnpjFormatado'] = df_sancoes['cpfCnpj'].apply(lambda x: str(re.sub(r'\D', '', x)))
        df_sancoes['linkDetalhamento'] = df_sancoes['linkDetalhamento'].apply(lambda x: self.url_format_link + x)
        df_sancoes["DataDaCarga"] = self.date_time
        df_sancoes["ArquivoFonte"] = file
        self.create_insert_datas(df_sancoes)

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
        new_df['orgaoResponsavel'] = new_df['orgaoResponsavel'].apply(lambda x: x.replace("'", " "))
        new_df['situacaoAcordo'] = new_df['situacaoAcordo'].apply(lambda x: x.replace("'", " "))
        new_df['nomeInformadoOrgaoResponsavel'] = new_df['nomeInformadoOrgaoResponsavel'].apply(lambda x: x.replace("'", " "))
        new_df['razaoSocial'] = new_df['razaoSocial'].apply(lambda x: x.replace("'", " "))
        new_df['nomeFantasia'] = new_df['nomeFantasia'].apply(lambda x: x.replace("'", " "))
        new_df["DataDaCarga"] = self.date_time
        new_df["ArquivoFonte"] = file
        self.create_insert_datas(new_df)



path_datas_ceis = 'cepim'
test = TransformDataSancoes(name_folder_data_raw=path_datas_ceis, name_folder_inserts=path_datas_ceis)
test.transform_datas_sancoes()