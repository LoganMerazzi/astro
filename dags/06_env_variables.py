# No arquivo Dockerfile, inserir:
#   - ENV AIRFLOW_VAR_<NomeDaVariavel>=valor 
# ( 
#     ENV AIRFLOW_VAR_PESSOA='{"nome":"Logan", "idade":"40", "api_secret":"MySecret"}'
#  neste caso)
# Desta forma, o airflow identificará as variáveis que devem ser utilizadas

# key    - value
# pessoa - {"nome":"Logan", "idade":"40", "api_secret":"MySecret"}

# Remover a variável da UI
# Entrar no CLI e reiniciar o Airflow:
#   $ docker exec -it astrob4c2e3_scheduler_1 "/bin/bash"
#   $ astro dev stop
#   $ astro dev start
# Testar via CLI
# (lembrar de se conectar no scheduler se estiver rodando via docker)
#   $ docker exec -it astrob4c2e3_scheduler_1 "/bin/bash"
# Validar se a variável de ambiente está lá (é para estar)
#   $ env | grep AIRFLOW_VAR 
# $ airflow tasks test 06_env_variables extract 2022-01-01

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def _extract(pessoa_nome, pessoa_idade):
    print(pessoa_nome)
    print(pessoa_idade)

with DAG( "_06_env_variables",
          description="Testes com variáveis de ambiente. Olhar os comentários no início do código.", 
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