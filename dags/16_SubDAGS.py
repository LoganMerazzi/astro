# SubDAGs é uma forma (antiga) para voce agrupar as suas tasks correlacionadas.
# Está aqui para fins de aprendizado

# Para uso, são necessários 2 componentes:
#   - SubdagOperator
#   - Factory function -> Responsável por controlar e retornar as chamadas usando o SubdagOperator.

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from airflow.operators.subdag import SubDagOperator

from datetime import datetime, timedelta
from typing import Dict
from subdag.subdag_factory import subdag_factory

@task.python(task_id="extract_partners", do_xcom_push=False, multiple_outputs=True)
def extract():
    partner_name="netflix"
    partner_path="/partners/netflix"
    return {"partner_name": partner_name, "partner_path": partner_path}

default_args = {
    "start_date": datetime(2022,1,1)
}

@dag(
    description="DAG para realizar o processamento de dados",
    default_args=default_args, # Feito desta forma para compartilhar o mesmo valor de start_date para a dag como para a subdag
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["data_science", "partners"],
    catchup=False,
    max_active_runs=1
)

def _16_Subdag():

    process_tasks = SubDagOperator(
        task_id="process_tasks",
        # O parâmetro sub_dag_id DEVE ser o nome da task_id da SubDagOprator.
        subdag=subdag_factory("_16_Subdag","process_tasks", default_args) #Aqui é onde você chama a factory function
    )

    extract()>> process_tasks

dag = _16_Subdag()