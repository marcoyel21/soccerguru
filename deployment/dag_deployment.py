from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator


import os

path = os.path.dirname(os.path.abspath(__file__))
path_html_factory=os.path.join(path, 'html_factory.py')
path_html_to_bucket=os.path.join(path, 'html_to_bucket')

params = {
    'path_html_factory': path_html_factory,
    'path_html_to_bucket': path_html_to_bucket}

with DAG(
    'process_4_deployment',
    description = '2 step deployment: create html + send it to bucket',
    #“At 13:30 on Friday.”    
    schedule_interval='45 13 * * 5',
    start_date = days_ago(1),
    tags=["football"],
) as dag:

    alert = DiscordWebhookOperator(
        task_id= "discord_alert_start",
        http_conn_id = 'discord',
        webhook_endpoint ='webhooks/1030306654106951731/5MHkAQZMKDMUn30n1HjL-BHtDSVU5QkQFK7sZQmBXVhWtK4I-SzI97E0g2u85gjVzuNS', 
        message = 'DAG Deploy started succesfully',)

t1 = BashOperator(
    task_id='html_factory',
    depends_on_past=False,
    params=params,
    bash_command='python3 {{params.path_html_factory}}',
    dag=dag)


t2 = BashOperator(
    task_id='html_to_bucket',
    depends_on_past=False,
    params=params,
    bash_command='{{params.path_html_to_bucket}} ',
    dag=dag)

alert2 = DiscordWebhookOperator(
    task_id= "discord_alert_finish",
    http_conn_id = 'discord',
    webhook_endpoint ='webhooks/1030306654106951731/5MHkAQZMKDMUn30n1HjL-BHtDSVU5QkQFK7sZQmBXVhWtK4I-SzI97E0g2u85gjVzuNS', 
    message = 'DAG Deploy finished succesfully',
    dag=dag)

t1 >> t2
t2 >> alert2
