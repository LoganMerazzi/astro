# Para criar uma variável:
# Via UI: Admin -> Variables

# key  - value
# nome - Logan

# Para testar via CLI 
# (lembrar de se conectar no scheduler se estiver rodando via docker)
# $ docker exec -it astrob4c2e3_scheduler_1 "/bin/bash"
# $ airflow tasks test 03_variables extract 2022-01-01

# Se usar password, secret, etc, no nome da variável, o valor é automaticamente escondido, tanto via UI quanto via logs de execução

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def _extract():
    nome = Variable.get("nome")
    print(nome)

with DAG( "_03_variables",
          description="Testes com variáveis. Olhar os comentários no início do código.", 
          start_date=datetime(2022,1,1), 
          schedule_interval="@daily",
          dagrun_timeout=timedelta(minutes=10),
          tags=["certificacao","variables", "astronomer"],
          catchup=False,
          max_active_runs=1
          ) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable= _extract
    )