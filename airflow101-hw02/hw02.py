import os
# import datetime as dt
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.hooks.postgres_hook import PostgresHook
# from datetime import datetime, timedelta
from psycopg2.extras import execute_values



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


sql_create_stage_customers = """
    drop table if exists stage_customers;
    create table stage_customers
            (
                id integer,
                name varchar(512),
                birth_date date not null,
                gender varchar(1),
                email varchar(128)
            );
    """

sql_create_stage_goods = """
    drop table if exists stage_goods;
    create table stage_goods
        (
            id integer,
            name text,
            price numeric
        );
    """


sql_create_stage_orders = """
    drop table if exists stage_orders;
    create table stage_orders
    (
        id integer,
        uuid uuid,
        good_name varchar(512),
        order_date timestamp,
        amount integer,
        customer_name varchar(512),
        customer_email varchar(128)
    );
    """

sql_create_stage_payment_status = """
    drop table if exists stage_payment_status;
    create table stage_payment_status
    (
        order_uuid uuid,
        status boolean
    );
"""

# pg to pg customers
sql_get_src_customers = """
    select id,
            trim(name),
            birth_date,
            gender,
            trim(email)
    from customers
"""

sql_insert_target_customers = """
    insert into stage_customers values %s
"""

# pg to pg goods
sql_get_src_goods = """
    select id,
            trim(name),
            price
    from goods
"""

sql_insert_target_goods = """
    insert into stage_goods values %s
"""


def load_pg_to_pg_stage(sql_get_src, sql_insert_target):
    src_conn = PostgresHook(postgres_conn_id='postgre_hw02_src').get_conn()
    trg_conn = PostgresHook(postgres_conn_id='postgre_hw02_trg').get_conn()

    src_cursor = src_conn.cursor("serverCursor")
    src_cursor.execute(sql_get_src)
    trg_cursor = trg_conn.cursor()

    while True:
        records = src_cursor.fetchmany(size=100)
        if not records:
            break
        execute_values(trg_cursor,
                       sql_insert_target,
                       records)
        trg_conn.commit()

    src_cursor.close()
    trg_cursor.close()
    src_conn.close()
    trg_conn.close()



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

    create_stage_customers = PostgresOperator(
        task_id='create_stage_customers',
        postgres_conn_id = 'postgre_hw02_trg',
        sql=sql_create_stage_customers
    )

    create_stage_goods = PostgresOperator(
        task_id='create_stage_goods',
        postgres_conn_id = 'postgre_hw02_trg',
        sql=sql_create_stage_goods
    )

    create_stage_orders = PostgresOperator(
        task_id='create_stage_orders',
        postgres_conn_id = 'postgre_hw02_trg',
        sql=sql_create_stage_orders
    )

    create_stage_payment_status = PostgresOperator(
        task_id='create_stage_payment_status',
        postgres_conn_id = 'postgre_hw02_trg',
        sql=sql_create_stage_payment_status
    )

    load_stage_customers = PythonOperator(
        task_id='load_stage_customers',
        python_callable=load_pg_to_pg_stage,
        op_kwargs={'sql_get_src': sql_get_src_customers, 'sql_insert_target': sql_insert_target_customers}
    )

    load_stage_goods = PythonOperator(
        task_id='load_stage_goods',
        python_callable=load_pg_to_pg_stage,
        op_kwargs={'sql_get_src': sql_get_src_goods, 'sql_insert_target': sql_insert_target_goods}
    )
    

    dummy_task >> [download_orders,
                   download_payment_status,
                   create_stage_customers,
                   create_stage_goods,
                   create_stage_orders,
                   create_stage_payment_status]
    create_stage_customers >> load_stage_customers
    create_stage_goods >> load_stage_goods