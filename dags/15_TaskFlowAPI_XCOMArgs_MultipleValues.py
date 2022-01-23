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

# O decorator aceita parametros, para que seja possível passar multiplos valores sem precisar fazer o push do xcom 'n' vezes
# Uma outra forma de realizar:
#   - Remover o multiple_outputs=True abaixo
#   - Inserir: from typing import Dict
#   - Alterar a definição da função para: 
#       - def extract() -> Dict['str', 'str']

#  Para não realizar o push do json na base e manter apenas os parametros passados:
#    - do_xcom_push=False

@task.python(task_id = "extract_pessoa", do_xcom_push=False, multiple_outputs=True)
def extract():
    nome = 'Logan'
    idade = 40
    return {"nome": nome, "idade": idade}

@task.python
def process(nome, idade):
    print(nome)
    print(idade)

# Existe um decorator para a DAG tb, podendo ficar desta forma:
@dag( description="Usando os decorators do TaskFlow API. Olhar os comentários no início do código.", 
      start_date=datetime(2022,1,1), 
      schedule_interval="@daily",
      dagrun_timeout=timedelta(minutes=10),
      tags=["certificacao","xcoms", "astronomer"],
      catchup=False,
      max_active_runs=1
      )
def TaskFlowAPI_XCOMArgs_MultipleValues():
    dadosPessoa = extract()
    process(dadosPessoa['nome'], dadosPessoa['idade'])

TaskFlowAPI_XCOMArgs_MultipleValues = TaskFlowAPI_XCOMArgs_MultipleValues()