from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator

import os

path = os.path.dirname(os.path.abspath(__file__))
path_get_latest_version=os.path.join(path, 'get_latest_version')
path_predict=os.path.join(path, 'prediction.py')

params = {'path_get_latest_version': path_get_latest_version,
          'path_predict': path_predict}

with DAG(
    'process_3_predict',
    description = 'Create features table -> train -> eval',
    #“At 13:30 on Friday.”    
    schedule_interval='30 13 * * 5',
    start_date = days_ago(1),
    tags=["football"],
) as dag:

    alert = DiscordWebhookOperator(
        task_id= "discord_alert_start",
        http_conn_id = 'discord',
        webhook_endpoint ='webhooks/1030306654106951731/5MHkAQZMKDMUn30n1HjL-BHtDSVU5QkQFK7sZQmBXVhWtK4I-SzI97E0g2u85gjVzuNS', 
        message = 'DAG Predict started succesfully',)

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

alert2 = DiscordWebhookOperator(
    task_id= "discord_alert_finish",
    http_conn_id = 'discord',
    webhook_endpoint ='token', 
    message = 'DAG Predict finished succesfully',
    dag=dag)

t1 >> t2
t2 >> alert2
