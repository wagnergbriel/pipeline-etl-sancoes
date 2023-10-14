from os import listdir
from os.path import join
import os

class LoadDataSancoes():
    def __init__(self, name_folder_inserts):
        self.name_folder_inserts = name_folder_inserts
        self.folder_inserts = f"./sql/inserts/{self.name_folder_inserts}"
    
    def get_file_datas(self):
        file = listdir(self.folder_inserts)
        latest_file = max(file, key=lambda f: os.path.getmtime(os.path.join(self.folder_inserts, f)))
        path_full_file = join(self.folder_inserts, latest_file)
        return path_full_file

x = LoadDataSancoes(name_folder_inserts="ceis")
path = x.get_file_datas()
print(path)