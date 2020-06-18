###
# HomeWork 4 code goes here
###

from airflow.hooks.postgres_hook import PostgresHook

import os
import csv
import psycopg2
import uuid
import requests

import secur.hw04_credentials as ENV


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


def can_i_start_dag() -> str:
    if pg_connection_test(force_insert_test=True):
        return 'start_task'
    else:
        return 'stop_execution'


def is_convertible_to_type(checked_value, checked_type: type) -> bool:
    try:
        _dummy = checked_type(checked_value)
        return True
    except TypeError:
        return False
    except ValueError:
        return False


def csv_column_to_list(filename: str, csv_column_name: str, header: list = None) -> list:
    with open(filename, mode='r') as csv_file:
        if header is None:
            reader = csv.DictReader(csv_file)
        else:
            reader = csv.DictReader(csv_file, fieldnames=header)
        column_list = []
        for row in reader:
            column_list.append(row[csv_column_name].strip().lower())
    return column_list


def postgres_column_to_list(table_name: str, column_name: str) -> list:
    table_name = table_name.lower()
    column_name = column_name.lower()
    try:
        # connect to source DB
        conn = PostgresHook(postgres_conn_id='postgre_hw02_src').get_conn()
        sql_get_type = f"""SELECT data_type 
                             FROM information_schema.columns
                            WHERE table_name = '{table_name}'
                              AND column_name = '{column_name}';"""
        cursor = conn.cursor()
        cursor.execute(sql_get_type)
        column_type = cursor.fetchone()[0]
        if column_type in ('"char"', 'text', 'character varying'):
            sql_get_column = f'SELECT lower(trim({column_name})) FROM {table_name};'
        else:
            sql_get_column = f'SELECT {column_name} FROM {table_name};'
        cursor.execute(sql_get_column)
        column_list = [x[0] for x in cursor.fetchall()]
        conn.close()
        return column_list

    except psycopg2.OperationalError as ex:
        print(f'Connection failed: {ex}')


def check_list_elements_type(input_list: list, checked_type: type) -> bool:
    is_type_correct = {is_convertible_to_type(x, checked_type) for x in input_list}
    return is_type_correct == {True}


def check_list_elements_unique(input_list: list) -> bool:
    compare_set = set(input_list)
    return len(compare_set) == len(input_list)


def sources_sanity_check(**kwargs) -> dict:
    task_id = kwargs['ti']

    # lists of key fields
    customers_email_list = postgres_column_to_list(table_name='customers', column_name='email')
    goods_name_list = postgres_column_to_list(table_name='goods', column_name='name')
    orders_uuid_list = csv_column_to_list(filename=os.path.join(ENV.STAGE_DIR, ENV.ORDERS_FILENAME), csv_column_name='uuid заказа')
    payment_status_uuid_list = csv_column_to_list(filename=os.path.join(ENV.STAGE_DIR, ENV.PAYMENT_STATUS_FILENAME),
                                                  csv_column_name='order_uuid',
                                                  header=['order_uuid', 'payment_status'])
    # lists of other important fields
    orders_amount_list = csv_column_to_list(filename=os.path.join(ENV.STAGE_DIR, ENV.ORDERS_FILENAME), csv_column_name='количество')
    goods_price_list = postgres_column_to_list(table_name='goods', column_name='price')

    checking_plan = {
        'uniqueness_test': {
            'executable': check_list_elements_unique,
            'queue': [
                {'src_list': customers_email_list, 'src_list_name': 'customers_email_list'},
                {'src_list': goods_name_list, 'src_list_name': 'goods_name_list'},
                {'src_list': orders_uuid_list, 'src_list_name': 'orders_uuid_list'},
            ],
        },
        'field_type_test': {
            'executable': check_list_elements_type,
            'queue': [
                {'src_list': orders_uuid_list, 'checked_type': uuid.UUID, 'src_list_name': 'orders_uuid_list'},
                {'src_list': payment_status_uuid_list, 'checked_type': uuid.UUID, \
                    'src_list_name': 'payment_status_uuid_list'},
                {'src_list': orders_amount_list, 'checked_type': int, 'src_list_name': 'orders_amount_list'},
                {'src_list': goods_price_list, 'checked_type': float, 'src_list_name': 'goods_price_list'},
            ],
        },
    }

    test_result = {'failed': 0}

    for test_type in checking_plan:
        test_run_result = {}
        for test_run in checking_plan[test_type]['queue']:
            num_failed = 0
            if test_type == 'uniqueness_test':
                _local_result = checking_plan[test_type]['executable'](test_run['src_list'])
                test_run_result[test_run['src_list_name']] = _local_result
                num_failed += 1 if _local_result is False else 0
            if test_type == 'field_type_test':
                _local_result = checking_plan[test_type]['executable'](test_run['src_list'], test_run['checked_type'])
                test_run_result[test_run['src_list_name']] = _local_result
                num_failed += 1 if _local_result is False else 0
            test_result[test_type] = test_run_result
            test_result['failed'] += num_failed
    
    test_result['task_id'] = task_id.task_id
    test_result['dag_id'] = task_id.dag_id

    print('Task_id below:')
    print(task_id.task_id)

    # xcom_value = {
    #     'test_result': test_result,
    #     'task_id': task_id,
    # }

    # task_id.xcom_push(key='xxcom', value=str(xcom_value))

    return test_result


def chose_way_after_sanity_check(**kwargs) -> str:
    task_id = kwargs['ti']
    xcom_value = task_id.xcom_pull(task_ids='source_sanity_check')
    print(f'My xcom value: {str(xcom_value)}')
    
    if xcom_value['failed'] == 0:
        return 'sources_are_clear'
    else:
        return 'garbage_in_da_house_notifier'


def bot_notification(**kwargs) -> None:
    task_id = kwargs['ti']
    xcom_value = task_id.xcom_pull(task_ids='source_sanity_check')
    print(f'My xcom value: {str(xcom_value)}')

    failed_task_id = xcom_value['task_id']
    failed_dag_id = xcom_value['dag_id']
    print(f'failed_task_id: {failed_task_id}, dag_id: {failed_dag_id}')

    # bot
    bot_message = f'Data quality issue in [dag_id] task_id: [{failed_dag_id}] {failed_task_id}. \nSanity check result: {xcom_value}'

    response = requests.post(
        url=f'https://api.telegram.org/bot{ENV.TG_BOT_TOKEN}/sendMessage',
        data={'chat_id': ENV.TG_BOT_CHAT_ID, 'text': bot_message}
    ).json()

    print(response)
