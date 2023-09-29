from os import listdir
from os.path import join
import pandas as pd
import os

class TransformDataSancoes():
    def __init__(self, folder_path):
        self.folder_path = folder_path
    
    def get_file_datas(self):
        file = listdir(self.folder_path)
        latest_file = max(file, key=lambda f: os.path.getmtime(os.path.join(self.folder_path, f)))
        path_full_file = join(self.folder_path, latest_file)
        return path_full_file

    def transform_datas_ceis(self):
        pass
    
    def transform_datas_cepim(self):
        pass
    
    def transform_datas_cnep(self):
        pass

    def transform_datas_acordo_leniencia(self):
        pass




path_datas_ceis = './data_raw/ceis'
test = TransformDataSancoes(folder_path=path_datas_ceis)
test.get_file_datas()