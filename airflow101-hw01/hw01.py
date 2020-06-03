import os
import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import requests
import csv

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 6, 03),
}

# filepath = '~'
filepath = '/var/www/html/stat'

FILENAME = os.path.join(os.path.expanduser(filepath), 'yandex_covid19_stat.csv')

def publish_stat():
    api_url = 'https://yastat.net/s3/milab/2020/covid19-stat/data/data_struct_10.json?v=timestamp'

    data = requests.get(api_url).json()

    dates = {}
    for i in range(0, 31):
        dates[i] = data['russia_stat_struct']['dates'][i]

    regions = {}
    for reg_id in data['russia_stat_struct']['data']:
        regions[reg_id] = data['russia_stat_struct']['data'][reg_id]['info']['name']

    with open(FILENAME, mode='w') as dest_file:
        employee_writer = csv.writer(dest_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        employee_writer.writerow(['date', 'region', 'infected', 'recovered', 'dead'])

        for cur_reg in regions.keys():
            for cur_dt in dates.keys():
                dt = dates[cur_dt]
                region = regions[cur_reg]
                try:
                    infected = data['russia_stat_struct']['data'][cur_reg]['cases'][cur_dt]['v']
                except:
                    infected = 0

                try:
                    recovered = data['russia_stat_struct']['data'][cur_reg]['cured'][cur_dt]['v']
                except:
                    recovered = 0

                try:
                    dead = data['russia_stat_struct']['data'][cur_reg]['deaths'][cur_dt]['v']
                except:
                    dead = 0

                employee_writer.writerow([dt, region, infected, recovered, dead])


with DAG(dag_id='hw01', default_args=args, schedule_interval='0 12 * * *') as dag:
    publish_stat = PythonOperator(
        task_id='publish_stat',
        python_callable=publish_stat,
        dag=dag
    )

publish_stat