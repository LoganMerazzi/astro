# TaskFlow API pode ser dividido em 2:
# - Decorators
#   - Você não precisa mais instanciar o Operator (PythonOperator, por exemplo), o decorator fará isto para você.
#   - Para isto, basta colocar o decorator @task.python, @task.virtualenv ou @task_group acima da função desejada


# - XCOMS Args
#   - Irá mostrar na UI a dependência de uso do XCOM entre as tasks. 
#     Até então, só era possível ver a dependências entre as tasks, mas não que os dados seriam reaproveitados.
#     A API agora deixará essa dependência explícita.
#     


from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.decorators import task, dag

from datetime import datetime, timedelta

@task.python
def extract():
    nome = 'Logan'
    idade = 40
    return {"nome": nome, "idade": idade}

# Se usar apenas o @task, será utilizado o pythonOperator por baixo dos panos.
# Boa prática: use sempre completo (@task.python), como na funcao acima.
@task
def process():
    print("process")

# Existe um decorator para a DAG tb, podendo ficar desta forma:
@dag( description="Usando os decorators do TaskFlow API. Olhar os comentários no início do código.", 
      start_date=datetime(2022,1,1), 
      schedule_interval="@daily",
      dagrun_timeout=timedelta(minutes=10),
      tags=["certificacao","xcoms", "astronomer"],
      catchup=False,
      max_active_runs=1
      )

# Comentado apenas para mostrar que não é mais necessário o uso
#    extract = PythonOperator(
#        task_id="extract",
#        python_callable= _extract,
#    )

#    process = PythonOperator(
#        task_id = "process",
#        python_callable= _process
#    )

# A chamada agora passar a ser como uma chamada de função. O nome da função é o dag_id
def _13_TaskFlowAPI_SemXCOM():
    extract() >> process()


TaskFlowAPI_SemXCOM = _13_TaskFlowAPI_SemXCOM()