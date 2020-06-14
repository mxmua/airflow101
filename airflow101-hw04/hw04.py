import os
# import datetime as dt
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

import requests
import csv

import hw04_utils
import secur.hw04_credentials as ENV
import hw04_sql_templates as sqltpl

args = {
    'owner': 'maxim',
    'start_date': days_ago(2)
}

STAGE_DIR = ENV.STAGE_DIR
ORDERS_SRC = ENV.ORDERS_SRC
ORDERS_FILENAME = ENV.ORDERS_FILENAME
ORDERS_FILENAME_FIXED = ENV.ORDERS_FILENAME_FIXED
PAYMENT_STATUS_SRC = ENV.PAYMENT_STATUS_SRC
PAYMENT_STATUS_FILENAME = ENV.PAYMENT_STATUS_FILENAME


# OLD HomeWork 2 executables 
# (for Homework 4 code see hw04_utils.py)

def fix_ordersfile_header():
    header = ['id', 'uuid', 'good_name', 'order_date', 'amount', 'customer_name', 'customer_email']
    with open(os.path.join(STAGE_DIR, ORDERS_FILENAME), mode='r') as src_file:
        reader = csv.DictReader(src_file, fieldnames=header)

        with open(os.path.join(STAGE_DIR, ORDERS_FILENAME_FIXED), mode='w', newline='') as dest_file: 
            print(dest_file.name)
            writer = csv.DictWriter(dest_file, fieldnames=reader.fieldnames)
            # writer.writeheader()
            # header_shift = next(reader)
            next(reader)
            writer.writerows(reader)


def download_payment_status():
    json_data = requests.get(PAYMENT_STATUS_SRC).json()

    with open(os.path.join(STAGE_DIR, PAYMENT_STATUS_FILENAME), mode='w') as dest_file:
        payment_status_writer = csv.writer(dest_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # payment_status_writer.writerow(['order_uuid', 'payment_status'])
        for d in json_data.keys():
            order_uuid = d
            payment_status = json_data[d]['success']
            payment_status_writer.writerow([order_uuid, payment_status])


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


def load_csv_to_pg_stage(csv_dir, csv_filename, pg_tablename):
    trg_conn = PostgresHook(postgres_conn_id='postgre_hw02_trg').get_conn()
    trg_cursor = trg_conn.cursor()
    src_csv = open(os.path.join(csv_dir, csv_filename), mode='r')
    src_csv.seek(0)
    trg_cursor.copy_from(src_csv, pg_tablename, sep=',')
    trg_conn.commit()
    src_csv.close()
    trg_conn.close()


# DAG

with DAG(dag_id='hw04', default_args=args, schedule_interval=None) as dag:
    
    # Homework 04 tasks

    start_task = DummyOperator(task_id='start_task')
    stop_execution = DummyOperator(task_id='stop_execution')

    sources_are_clear = DummyOperator(task_id='sources_are_clear')

    can_i_run = BranchPythonOperator(
        task_id='can_i_run',
        python_callable = hw04_utils.can_i_start_dag,
        #provide_context=True
    )

    source_sanity_check = PythonOperator(
        task_id='source_sanity_check',
        python_callable=hw04_utils.sources_sanity_check,
        provide_context=True,
    )

    branch_after_sanity_check = BranchPythonOperator(
        task_id='branch_after_sanity_check',
        python_callable=hw04_utils.chose_way_after_sanity_check,
        provide_context=True,
    )

    garbage_in_da_house_notifier = PythonOperator(
        task_id='garbage_in_da_house_notifier',
        python_callable=hw04_utils.bot_notification,
        provide_context=True,
    )

    # Homework 02 tasks below

    # Download orders.csv
    download_orders = BashOperator(
        task_id='download_orders',
        bash_command='rm {dir}/{filename} ; wget -P {dir} -O {dir}/{filename} {src}'.
            format(dir=STAGE_DIR, filename=ORDERS_FILENAME, src=ORDERS_SRC)
    )

    fix_orders_header = PythonOperator(
        task_id='fix_orders_header',
        python_callable=fix_ordersfile_header
    )

    # Save payment status to csv
    download_payment_status = PythonOperator(
        task_id='download_payment_status',
        python_callable=download_payment_status
    )

    create_stage_customers = PostgresOperator(
        task_id='create_stage_customers',
        postgres_conn_id='postgre_hw02_trg',
        sql=sqltpl.sql_create_stage_customers
    )

    create_stage_goods = PostgresOperator(
        task_id='create_stage_goods',
        postgres_conn_id='postgre_hw02_trg',
        sql=sqltpl.sql_create_stage_goods
    )

    create_stage_orders = PostgresOperator(
        task_id='create_stage_orders',
        postgres_conn_id='postgre_hw02_trg',
        sql=sqltpl.sql_create_stage_orders
    )

    create_stage_payment_status = PostgresOperator(
        task_id='create_stage_payment_status',
        postgres_conn_id='postgre_hw02_trg',
        sql=sqltpl.sql_create_stage_payment_status
    )

    load_stage_customers = PythonOperator(
        task_id='load_stage_customers',
        python_callable=load_pg_to_pg_stage,
        op_kwargs={'sql_get_src': sqltpl.sql_get_src_customers, 'sql_insert_target': sqltpl.sql_insert_target_customers}
    )

    load_stage_goods = PythonOperator(
        task_id='load_stage_goods',
        python_callable=load_pg_to_pg_stage,
        op_kwargs={'sql_get_src': sqltpl.sql_get_src_goods, 'sql_insert_target': sqltpl.sql_insert_target_goods}
    )
    
    load_stage_orders = PythonOperator(
        task_id='load_stage_orders',
        python_callable=load_csv_to_pg_stage,
        op_kwargs={'csv_dir': STAGE_DIR, 'csv_filename': ORDERS_FILENAME_FIXED, 'pg_tablename': 'stage_orders'}
    )

    load_stage_payment_status = PythonOperator(
        task_id='load_stage_payment_status',
        python_callable=load_csv_to_pg_stage,
        op_kwargs={'csv_dir': STAGE_DIR,
                   'csv_filename': PAYMENT_STATUS_FILENAME,
                   'pg_tablename': 'stage_payment_status'
                   }
    )

    load_final_dataset = PostgresOperator(
        task_id='load_final_dataset',
        postgres_conn_id='postgre_hw02_trg',
        sql=sqltpl.sql_load_final_dataset
    )

    can_i_run >> [start_task, stop_execution]

    start_task >> [download_orders,
                   download_payment_status] >> source_sanity_check >> branch_after_sanity_check

    branch_after_sanity_check >> [sources_are_clear, garbage_in_da_house_notifier]
    sources_are_clear >> [create_stage_customers,
                          create_stage_goods,
                          create_stage_orders,
                          create_stage_payment_status]

    create_stage_orders >> fix_orders_header >> load_stage_orders
    create_stage_customers >> load_stage_customers
    create_stage_goods >> load_stage_goods
    
    create_stage_payment_status >> load_stage_payment_status

    [load_stage_orders,
     load_stage_customers,
     load_stage_goods,
     load_stage_payment_status] >> load_final_dataset
