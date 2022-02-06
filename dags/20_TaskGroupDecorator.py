# Taskgroup é a forma ideal para agrupar as tasks.
# 

# o código fica muito mais limpo e visualmente na UI, fica muito melhor vendo agrupado.

# Nesta V1, o objetivo é limpar o código e mostrar o funcionamento.

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag

from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
from typing import Dict
from subdag.subdag_factory import subdag_factory

from groups.process_taskgroup_decorator import process_taskgroup_decorator

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

def _20_TaskGroupsDecorator():

    partner_settings = extract()

    process_taskgroup_decorator(partner_settings)

dag = _20_TaskGroupsDecorator()