# Importando as bibliotecas que vamos usar nesse exemplo
from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator


# Nomeando a DAG e definindo quando ela vai ser executada (você pode usar argumentos em Crontab também caso queira que a DAG execute por exemplo todos os dias as 8 da manhã)
dag =  DAG(
   'etl_atendimento',
   schedule_interval=None,
   start_date=datetime(2024,1,24),
   catchup=False,
   )
    
    
    
# Definindo as tarefas que a DAG vai executar, nesse caso a execução de dois programas Python, chamando sua execução por comandos bash
t3 = BashOperator(
    task_id='first_etl',
    bash_command="""
    cd $AIRFLOW_HOME/dags/etl_scripts/
    python3 my_first_etl_script.py
    """,
    dag=dag)
        
t2 = BashOperator(
    task_id='second_etl',
    bash_command="""
    cd $AIRFLOW_HOME/dags/etl_scripts/
    python3 my_second_etl_script.py
    """,
    dag=dag)

# Definindo o padrão de execução, nesse caso executamos t1 e depois t2
t3 >> t2