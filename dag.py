from airflow import DAG
from datetime import datetime

with DAG(dag_id="pipeline_etl_sancoes",
            start_date=datetime(2023, 9, 10),
            schedule_interval="@monthly"
        ) as dag:

    pass
