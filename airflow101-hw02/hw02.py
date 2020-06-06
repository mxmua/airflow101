import os
# import datetime as dt
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

import requests
import csv

args = {
    'owner': 'maxim',
    'start_date': days_ago(2)
}

STAGE_DIR = os.path.join(os.path.expanduser('~'), 'stage')

ORDERS_SRC = 'https://airflow101.python-jitsu.club/orders.csv'
ORDERS_FILENAME = 'orders.csv'

PAYMENT_STATUS_SRC = 'https://api.jsonbin.io/b/5ed7391379382f568bd22822'
PAYMENT_STATUS_FILENAME = 'payment_status.csv'


def download_payment_status():
    data = requests.get(PAYMENT_STATUS_SRC).json()

    with open(os.path.join(STAGE_DIR, PAYMENT_STATUS_FILENAME), mode='w') as dest_file:
        payment_status_writer = csv.writer(dest_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        payment_status_writer.writerow(['order_uuid', 'payment_status'])

        for d in data.keys():
            order_uuid = d
            payment_status = data[d]['success']
            payment_status_writer.writerow([order_uuid, payment_status])


with DAG(dag_id='hw02', default_args=args, schedule_interval=None) as dag:
    # Entry point
    dummy_task = DummyOperator(task_id='dummy_task')

    # Download orders.csv
    download_orders = BashOperator(
        task_id='download_orders',
        bash_command='rm {dir}/{filename} ; wget -P {dir} -O {dir}/{filename} {src}'.format(dir=STAGE_DIR, filename=ORDERS_FILENAME, src=ORDERS_SRC)
    )

    # Save payment status to csv
    download_payment_status = PythonOperator(
        task_id = 'download_payment_status',
        python_callable = download_payment_status
    )

    dummy_task >> [download_orders, download_payment_status]