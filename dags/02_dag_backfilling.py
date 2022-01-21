# Para executar o backfilling, rodar na CLI:
# $ airflow dags backfill -s 2020-01-01 -e 2021-01-01 dag_id
# https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html#backfill

from airflow import DAG
from datetime import datetime, timedelta

with DAG( "02_backfilling",
          description="Para executar o backfilling, deve ser realizado via CLI (ver o comentário no código)", 
          start_date=datetime(2022,1,1), 
          schedule_interval="@daily",
          dagrun_timeout=timedelta(minutes=10),
          tags=["certificacao","backfilling", "astronomer"],
          catchup=False,
          max_active_runs=1 # Indica que, caso ele seja chamado mais de uma vez, apenas uma execução por vez acontecerá.
          ) as dag:
    None