import requests
import csv
import os

src = 'https://api.jsonbin.io/b/5ed7391379382f568bd22822'
FILENAME = 'payment_status.csv'

data = requests.get(src).json()

with open(FILENAME, mode='w') as dest_file:
    payment_status_writer = csv.writer(dest_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    payment_status_writer.writerow(['order_uuid', 'payment_status'])

    for d in data.keys():
        order_uuid = d
        payment_status = data[d]['success']
        payment_status_writer.writerow([order_uuid, payment_status])


print(os.path.join(os.path.expanduser('~'), 'stage'))