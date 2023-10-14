from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from etl.extract_data import SancoesAPI, PortalTransparenciaAPI
from etl.transform_data import TransformDataSancoes
from etl.load_data import LoadDataSancoes

# Defina as funções para suas tarefas

def extract_ceis_data():
    path_datas_ceis = './data_raw/ceis'
    name_sancao = "ceis"
    api_ceis = SancoesAPI(name_sancao=name_sancao)
    data_ceis = api_ceis.get_sancoes()
    api_ceis.save_json_file(dataset=data_ceis,
                            path_dir=path_datas_ceis,
                            file_name="ceis")

def transform_ceis_data():
    path_datas_ceis = './data_raw/ceis'  # Isso parece um erro, pois você já definiu 'path_datas_ceis' anteriormente
    data_t = TransformDataSancoes(name_folder_data_raw=path_datas_ceis, name_folder_inserts=path_datas_ceis)
    data_t.transform_datas_sancoes()
    # data_t.transform_datas_acordo_leniencia()

def load_ceis_data():
    file_sql_to_load = LoadDataSancoes(name_folder_inserts="ceis")
    file_sql_to_load.get_file_datas()

# Defina o fluxo de trabalho Airflow

with DAG(dag_id="pipeline_etl_sancoes",
         start_date=datetime(2023, 9, 10),
         schedule_interval="@monthly",
         is_active=True
) as dag:

    extract_task = PythonOperator(
        task_id="extract_ceis_data",
        python_callable=extract_ceis_data
    )

    transform_task = PythonOperator(
        task_id="transform_ceis_data",
        python_callable=transform_ceis_data
    )

    load_task = PythonOperator(
        task_id="load_ceis_data",
        python_callable=load_ceis_data
    )

    extract_task >> transform_task >> load_task
