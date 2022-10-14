from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

import os

path = os.path.dirname(os.path.abspath(__file__))
path_get_latest_version=os.path.join(path, 'get_latest_version')
path_predict=os.path.join(path, 'prediction.py')

params = {'path_get_latest_version': path_get_latest_version,
          'path_predict': path_predict}

dag = DAG(
    'process_3_predict',
    description = 'Create features table -> train -> eval',
    #“At 13:30 on Friday.”    
    schedule_interval='30 13 * * 5',
    start_date = days_ago(1),
    tags=["football"])

t1 = BashOperator(
    task_id='get_latest_version',
    depends_on_past=False,
    params=params,
    bash_command='{{params.path_get_latest_version}} ',
    dag=dag)

t2 = BashOperator(
    task_id='predictions',
    depends_on_past=False,
    params=params,
    bash_command='python3 {{params.path_predict}}',
    dag=dag)

t1 >> t2