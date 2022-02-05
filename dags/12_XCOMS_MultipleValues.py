# Para compartilhar dados entre tasks, é necessário utilizar as XCOMS (Cross Communications)
# 
# o parâmetro 'ti'  é um objeto que representa a task_instance

# Para validar:
# Na UI, dê um start na DAG
# Após terminar, vá em Admin -> XCOMS
# os dados devem aparecer na tela.


from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

def _extract(ti):
    nome = 'Logan'
    idade = 40
    # Para retornar multiplos valores, colocar em um json, para realizar apenas uma única conexão no banco.
    return {"nome": nome, "idade": idade}

def _process(ti):
    pessoa = ti.xcom_pull(task_ids="extract")
    print(pessoa["nome"])

with DAG( "_12_XCOMS_MultipleValues",
          description="Usando XCOMs. Olhar os comentários no início do código.", 
          start_date=datetime(2022,1,1), 
          schedule_interval="@daily",
          dagrun_timeout=timedelta(minutes=10),
          tags=["certificacao","xcoms", "astronomer"],
          catchup=False,
          max_active_runs=1
          ) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable= _extract,
    )

    process = PythonOperator(
        task_id = "process",
        python_callable= _process
    )

    extract >> process