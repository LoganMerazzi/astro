# Para compartilhar dados entre tasks, é necessário utilizar as XCOMS (Cross Communications)
# 
# o parâmetro 'ti'  é um objeto que representa a task_instance

# Para validar:
# $ airflow tasks test 10_XCOMS extract 2022-01-01
# $ airflow tasks test 10_XCOMS process 2022-01-01

# Veja que a a variável nome trafegou entre as duas tarefas

# Mas, cuidado!
# Uma das limitações no uso de Xcoms é o tamanho do xcom a ser enviado:
#   - O limite quando estiver usando:
#      - SQLite: 4GB
#      - Postgres: 1GB
#      - MySQL: 64KB (yep!)


from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

def _extract(ti):
    nome = 'Logan'
    return nome

def _process(ti):
    # Ambos os modos funcionam
    #nome = ti.xcom_pull(key="return_value", task_ids="extract")
    nome = ti.xcom_pull(task_ids="extract")
    print(nome)

with DAG( "11_XCOMS_Method2",
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