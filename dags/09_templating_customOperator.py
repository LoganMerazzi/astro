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

# Para buscar o valor a partir de um arquivo:
# Criar a pasta e o arquivo dentro da pasta dag
# alterar a query para o caminho do arquivo
# Validar novamente

# Para ver a identificação do parametro .sql neste caso, ir em:
# https://github.com/apache/airflow/blob/main/airflow/providers/postgres/operators/postgres.py
# e pesquisar pelo template_ext: Sequence[str] = ('.sql',)

# Se quiser fazer com que algo que não possua um template, permita, é preciso criar um CustomOperator:
# 

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

from typing import Sequence

class CustomPostgresOperator(PostgresOperator):
    template_fields: Sequence[str] = ('sql','parameters')

def _extract(pessoa_nome, pessoa_idade):
    print(pessoa_nome)
    print(pessoa_idade)

with DAG( "_09_templating_customOperator",
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

    fetching_data = CustomPostgresOperator(
        task_id = "fetching_data",
        sql="sql/request.sql",
        parameters={
            'next_ds': '{{ next_ds }}',
            'prev_ds': '{{ prev_ds }}',
            'nome':'{{ var.json.pessoa.nome }}'
        }
    )