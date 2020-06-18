from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import timedelta
from time import sleep
from datetime import datetime
import requests
import psycopg2
import random

import secur.hw04_credentials as ENV

args = {
    'owner': 'maxim',
    'start_date': datetime(2020, 6, 18, 15, 20),
}


def pg_connection_test(force_insert_test: bool = False) -> bool:
    try:
        conn = PostgresHook(postgres_conn_id='postgre_hw02_trg').get_conn()
        cursor = conn.cursor()
        if force_insert_test:
            cursor.execute("INSERT INTO final_dataset (name) VALUES ('pgtest')")
            conn.rollback()
        cursor.close()
        conn.close()
        return True

    except psycopg2.OperationalError as ex:
        print(f'Connection failed: {ex}')
        return False


def bot_message(message_text: str, **kwargs) -> None:
    response = requests.post(
        url=f'https://api.telegram.org/bot{ENV.TG_BOT_TOKEN}/sendMessage',
        data={'chat_id': ENV.TG_BOT_CHAT_ID, 'text': message_text}
    ).json()
    print(response)


def canary_test() -> None:
    sleep_seconds = random.randint(1, 10)
    print('sleep: ', sleep_seconds)
    sleep(sleep_seconds)
    if not pg_connection_test(force_insert_test=True):
        print('Autokilling DAG')
        1 / 0


def on_dag_failure(*args, **kwargs):
    print(args, kwargs)
    bot_message('--- failure ---')
    bot_message(message_text=args)


def print_sla_miss(*args, **kwargs):
    alert_text = '--- SLA MISSED ---'
    print(alert_text)
    bot_message(message_text=alert_text)


# def just_send_to_tg(**kwargs) -> None:
#     bot_message('Hi')
#     pass


with DAG(dag_id='hw05_01_02',
         default_args=args,
         schedule_interval=timedelta(minutes=15),
         sla_miss_callback=print_sla_miss,
         on_failure_callback=on_dag_failure,
         ) as dag:
    entry_point = DummyOperator(task_id='entry_point')
    exit_point = DummyOperator(task_id='exit_point')

    task_canary = PythonOperator(
        task_id='canary_test',
        sla=timedelta(seconds=40),
        python_callable=canary_test,
    )

    # just_message = PythonOperator(
    #     task_id='just_message',
    #     python_callable=just_send_to_tg,
    # )
    entry_point >> task_canary >> exit_point
