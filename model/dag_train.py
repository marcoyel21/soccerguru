from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator

import os

path = os.path.dirname(os.path.abspath(__file__))
path_get_latest_model=os.path.join(path, 'get_latest_model')
path_train=os.path.join(path, 'train.py')
path_upload_model=os.path.join(path, 'upload_model')

params = {
    'path_get_latest_model' : path_get_latest_model,
    'path_train': path_train,
    'path_upload_model': path_upload_model}

dag = DAG(
    'process_2_train',
    description = 'Model training',
    #At 13:15 on day-of-month 1.
    schedule_interval='15 13 * * 5',
    start_date = days_ago(1),
    tags=["football"])
with DAG(
    'process_2_train',
    description = 'Model training',
    #At 13:15 on day-of-month 1.
    schedule_interval='15 13 * * 5',
    start_date = days_ago(1),
    tags=["football"],
) as dag:

    alert = DiscordWebhookOperator(
        task_id= "discord_alert_start",
        http_conn_id = 'discord',
        webhook_endpoint ='webhooks/1030306654106951731/5MHkAQZMKDMUn30n1HjL-BHtDSVU5QkQFK7sZQmBXVhWtK4I-SzI97E0g2u85gjVzuNS', 
        message = 'DAG Train started succesfully',)

t1 = BashOperator(
    task_id='get_latest_model',
    depends_on_past=False,
    params=params,
    bash_command='{{params.path_get_latest_model}} ',
    dag=dag)


t2 = BashOperator(
    task_id='train',
    depends_on_past=False,
    params=params,
    bash_command='python3 {{params.path_train}}',
    dag=dag)
    
t3 = BashOperator(
    task_id='upload_model',
    depends_on_past=False,
    params=params,
    bash_command='{{params.path_upload_model}} ',
    dag=dag)

alert2 = DiscordWebhookOperator(
    task_id= "discord_alert_finish",
    http_conn_id = 'discord',
    webhook_endpoint ='webhooks/1030306654106951731/5MHkAQZMKDMUn30n1HjL-BHtDSVU5QkQFK7sZQmBXVhWtK4I-SzI97E0g2u85gjVzuNS', 
    message = 'DAG Train finished succesfully',
    dag=dag)


t1 >> t2
t2 >> t3
t3 >> alert2
