from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from extract_data import SancoesAPI, PortalTransparenciaAPI
from transform_data import TransformDataSancoes
from load_data import LoadDataSancoes

name_folder = "cepim"
def extract_data():
    path_datas = './data_raw/cepim'
    name_sancao = "cepim"
    api_cepim = SancoesAPI(name_sancao=name_sancao)
    data_cepim = api_cepim.get_sancoes()
    api_cepim.save_json_file(dataset=data_cepim,
                            path_dir=path_datas,
                            file_name="cepim")

def transform_data():
    data_t = TransformDataSancoes(name_folder_data_raw=name_folder, name_folder_inserts=name_folder)
    data_t.transform_datas_sancoes()
    # data_t.transform_datas_acordo_leniencia()

file_sql_to_load = LoadDataSancoes(name_folder_inserts="cepim")
path_file = file_sql_to_load.get_file_datas()
with open(path_file, "r") as sql_file:
        sql_query = sql_file.read()


with DAG(dag_id="pipeline_etl_cepim",
         start_date=datetime(2023, 9, 20),
         schedule_interval="@monthly",
         catchup=False,
         template_searchpath="./sql/tables/" #path of folder files sql
) as dag:

    extract_task = PythonOperator(
        task_id="extract_cepim_data",
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id="transform_cepim_data",
        python_callable=transform_data
    )

    with TaskGroup("task_sql") as task_sql_group:
        create_table = PostgresOperator(
            task_id="create_table",
            postgres_conn_id="conection_to_sancoes_datas",
            sql=f"create_table_{name_folder}.sql",
            autocommit=True
        )

        insert_or_upadate_data = PostgresOperator(
            task_id="insert_or_update_data",
            postgres_conn_id="conection_to_sancoes_datas",
            sql=sql_query,
            autocommit=True
        )

        create_table >> insert_or_upadate_data

    extract_task >> transform_task >> task_sql_group
