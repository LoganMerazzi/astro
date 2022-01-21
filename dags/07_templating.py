# Para usar o template, basta usar as {{  }} no local.
# Em tempo de execução, o conteúdo será substituído
# Para ver onde e como o operador suporta como template, acessar:
# https://registry.astronomer.io/
# e pesquisar sobre o operador
# https://registry.astronomer.io/providers/postgres/modules/postgresoperator
# 
# Importante:
# {{ ds }} substitui pela data de execução da Dag.

# Para ver o resultado, entrar na Dag -> graph -> selecionar a task e clicar em 'rendered'
# o valor deve aparecer na query

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

def _extract(pessoa_nome, pessoa_idade):
    print(pessoa_nome)
    print(pessoa_idade)

with DAG( "07_templating",
          description="Testes com templating. Olhar os comentários no início do código.", 
          start_date=datetime(2022,1,1), 
          schedule_interval="@daily",
          dagrun_timeout=timedelta(minutes=10),
          tags=["certificacao","variables", "astronomer"],
          catchup=False,
          max_active_runs=1
          ) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable= _extract,
        op_args=["{{ var.json.pessoa.nome }}","{{ var.json.pessoa.idade }}"]
    )

    fetching_data = PostgresOperator(
        task_id = "fetching_data",
        sql="select * from pessoas where data = '{{ ds }}'"
    )