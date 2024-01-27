# Importando as bibliotecas que vamos usar nesse exemplo
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

from etl_scripts import etl_dim_cidade, etl_dim_agente, etl_dim_departamento, etl_dim_tempo_calendario, elt_fato_atendimento


# Nomeando a DAG e definindo quando ela vai ser executada (você pode usar argumentos em Crontab também caso queira que a DAG execute por exemplo todos os dias as 8 da manhã)
dag = DAG(
    'ETL data mart da fato Atendimento',
    schedule_interval=None,
    start_date=datetime(2024, 1, 24),
    catchup=False,
)

# Cargas Data Lake
# Definindo as tarefas que a DAG vai executar, nesse caso a execução de dois programas Python, chamando sua execução por comandos bash
t1 = PythonOperator(
    task_id='ETL Dimensão Cidades',
    python_callable=etl_dim_cidade,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id='ETL Dimensão Agente ',
    python_callable=etl_dim_agente,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='ETL Dimensão Departamento ',
    python_callable=etl_dim_departamento,
    provide_context=True,
    dag=dag,
)

t4 = PythonOperator(
    task_id='ETL Dimensão Calendário e Tempo(Horas) ',
    python_callable=etl_dim_tempo_calendario,
    provide_context=True,
    dag=dag,
)

t5 = PythonOperator(
    task_id='ETL da Fato Atendimentos ',
    python_callable=elt_fato_atendimento,
    provide_context=True,
    dag=dag,
)

# Definindo o padrão de execução
[t2,  t1, t3, t4] >> t5
