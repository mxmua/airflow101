import requests
# import pandas as pd
import csv

api_url = 'https://yastat.net/s3/milab/2020/covid19-stat/data/data_struct_10.json?v=timestamp'

data = requests.get(api_url).json()

DATES = {}
for i in range(0, 31):
    DATES[i] = data['russia_stat_struct']['dates'][i]

REGIONS = {}
for reg_id in data['russia_stat_struct']['data']:
    REGIONS[reg_id] = data['russia_stat_struct']['data'][reg_id]['info']['name']


with open('yandex_covid19_stat.csv', mode='w') as dest_file:
    employee_writer = csv.writer(dest_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    employee_writer.writerow(['date', 'region', 'infected', 'recovered', 'dead'])

    for cur_reg in REGIONS.keys():
        for cur_dt in DATES.keys():
            dt = DATES[cur_dt]
            region = REGIONS[cur_reg]
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



