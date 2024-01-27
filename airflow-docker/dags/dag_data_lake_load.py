# Importando as bibliotecas que vamos usar nesse exemplo
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

from etl_scripts import etl_atendimentos_dl, load_ddd_cidades_uf, etl_dim_cidade, etl_dim_agente, etl_dim_departamento, etl_dim_tempo_calendario, elt_fato_atendimento


# Nomeando a DAG e definindo quando ela vai ser executada (você pode usar argumentos em Crontab também caso queira que a DAG execute por exemplo todos os dias as 8 da manhã)
dag = DAG(
    'etl_atendimento',
    schedule_interval=None,
    start_date=datetime(2024, 1, 24),
    catchup=False,
)

# Cargas Data Lake
# Definindo as tarefas que a DAG vai executar, nesse caso a execução de dois programas Python, chamando sua execução por comandos bash
t1 = PythonOperator(
    task_id='Carregar Cidades',
    python_callable=load_ddd_cidades_uf,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id='Carregar Atendimentos',
    python_callable=etl_atendimentos_dl,
    provide_context=True,
    dag=dag,
)

# Definindo o padrão de execução, nesse caso executamos t1 e depois t2
[t2,  t1]
