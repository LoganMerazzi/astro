from datetime import datetime
from airflow import DAG
from datetime import datetime, timedelta

with DAG("my_dag", description="dag in charge of processing whatever it has to do",
         start_date=datetime(2022,1,1),
         schedule_interval="@daily",
         dagrun_timeout=timedelta(minutes=10), 
         tags=["data_science"],
         catchup=False) as dag:
    None