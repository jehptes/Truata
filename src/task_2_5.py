from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from random import randint


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 8, 1)}



dag = DAG(
    default_args=default_args,
    schedule_interval='* * * * *',
    catchup=False)


task_1 = DummyOperator(dag=dag, task_id='task_1')
task_2 = DummyOperator(dag=dag, task_id='task_2')
task_3 = DummyOperator(dag=dag, task_id='task_3')
task_4 = DummyOperator(dag=dag, task_id='task_4')
task_5 = DummyOperator(dag=dag, task_id='task_5')
task_6 = DummyOperator(dag=dag, task_id='task_6')

first_part = DummyOperator(dag=dag, task_id='first')
second_part = DummyOperator(dag=dag, task_id='second')


task_1 >> task_2   >> first_part

task_1 >> task_3   >> first_part

task_1 >>  first_part >> task_4

task_1 >>  first_part >> task_5

task_1 >>  first_part >> task_6

