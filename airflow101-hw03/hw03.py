from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from secur.hw03_credentials import ENV
from hw03_operators.tg_airtable import Bot, SendButtonOperator, CheckButtonPressSensor, ReportToAirtableOperator

# DAG

default_args = {
    'owner': 'maxim',
    'start_date': days_ago(2),
    'report_filename': '/tmp/last_update.txt',
    'token': ENV['TG_BOT_TOKEN'],
    'button_marker': ENV['TG_BUTTON_MARKER'],
    'chat_id': ENV['TG_BOT_CHAT_ID'],
    'airtable_token': ENV['AIRTABLE_TOKEN'],
    'airtable_url': ENV['AIRTABLE_URL'],
}

dag = DAG(dag_id='hw03', schedule_interval=None, default_args=default_args)

send_button = SendButtonOperator(task_id='send_button', dag=dag)
wait_button_press = CheckButtonPressSensor(task_id='wait_button_press', poke_interval=10, dag=dag)
report_to_airtable = ReportToAirtableOperator(task_id='report_to_airtable', dag=dag)

all_success = DummyOperator(task_id='all_success', dag=dag)

send_button >> wait_button_press >> report_to_airtable >> all_success