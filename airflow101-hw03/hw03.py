import requests

import os
import json
from typing import List, Union
from datetime import datetime

from airflow.models import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.dates import days_ago

from secur.hw03_credentials import ENV

class Bot:
    BASE_URL = 'https://api.telegram.org'

    def __init__(self, token: str) -> None:
        self.token = token
        self.base_url = f'{self.BASE_URL}/bot{token}'

    def get_updates(self) -> None:
        payload = {
        }
        response = requests.post(
            f'{self.base_url}/getUpdates', json=payload, timeout=3
        )
        return response.json()

    def send_keyboard(
        self,
        chat_id: Union[str, int],
        text: str,
        reply_markup: dict,
        parse_mode: str = 'Markdown',
        disable_notification: bool = True,
        # remove_keyboard: bool = True,
    ) -> None:
        payload = {
            'chat_id': chat_id,
            'text': text,
            'reply_markup': reply_markup,
            'parse_mode': parse_mode,
            'disable_notification': disable_notification,
            # 'remove_keyboard': remove_keyboard,
        }
        response = requests.post(
            f'{self.base_url}/sendMessage', json=payload, timeout=5
        )
        return response.json()


class SendButtonOperator(BaseOperator):
    @apply_defaults
    def __init__(self, token: str, chat_id: int, button_marker: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = token
        self.bot = Bot(token=token)
        self.chat_id = chat_id
        self.button_marker = button_marker
        
    def execute(self, *args, **kwargs):
        button_text = 'Поехали'
        chat_text = 'Поехали?'
        keyboard_markup = {"inline_keyboard": [[{"text": button_text, "callback_data": self.button_marker}]]}
        
        button_result = self.bot.send_keyboard(chat_id=self.chat_id, text=chat_text, reply_markup=keyboard_markup)
        
        print(f'Button sent with result: {button_result}')


class CheckButtonPressSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, token: str, button_marker:str, report_filename: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = token
        self.bot = Bot(token=token)
        self.report_filename = report_filename
        self.updates_result = {}
        self.button_marker = button_marker

    def get_last_update(self) -> dict:
        updates = self.bot.get_updates()
        for update in updates['result']:
            update_id = update['update_id']
            username = update['callback_query']['from']['username']
            button_marker = update['callback_query']['data']
            triggered_at = datetime.fromtimestamp(update['callback_query']['message']['date'])

            if button_marker == self.button_marker:
                self.updates_result['update_id'] = update_id
                self.updates_result['chat_id'] = update_id
                self.updates_result['username'] = username
                self.updates_result['button_marker'] = button_marker
                self.updates_result['triggered_at'] = triggered_at

                print(f'updates: {updates}')

        # Return true if not empty otherwise return false
        return any(self.updates_result)


    def poke(self, *args, **kwargs) -> bool:
        self.get_last_update()
        
        # initial file creation kostyl
        if not (os.path.exists(self.report_filename) and os.stat(self.report_filename).st_size > 0):
            with open(self.report_filename, 'w') as writer:
                json.dump({"update_id": -1}, writer, default=str)
        
        with open(self.report_filename, 'r') as reader:
            previous_result = json.load(reader)
        
        previous_update_id = previous_result['update_id']
        print(f'Poking until {self.button_marker}')
        
        if self.updates_result and previous_update_id < self.updates_result['update_id']:

            with open(self.report_filename, 'w') as writer:
                json.dump(self.updates_result, writer, default=str)

            print(f'Dumped info as {self.report_filename}')
            
            return self.button_marker == self.updates_result['button_marker']
        else:
            return False


class ReportToAirtableOperator(BaseOperator):
    @apply_defaults
    def __init__(self, airtable_token: str, airtable_url: str, report_filename: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.airtable_token = airtable_token
        self.report_filename = report_filename
        self.airtable_url = airtable_url
        
    def execute(self, *args, **kwargs):
        with open(self.report_filename, "r") as reader:
            button_data = json.load(reader)

        headers = {'Authorization': f'Bearer {self.airtable_token}', 'Content-Type': 'application/json'}

        airtable_fields_json = {
            "fields": {
                "chat_id": str(button_data["chat_id"]),
                "username": button_data["username"],
                "triggered_at": button_data["triggered_at"],
                "event_type": "button_press",
                "reporter_name": button_data["button_marker"]
            }
            #"typecast": "true"
        }

        airtable_request = requests.post(self.airtable_url, json=airtable_fields_json, headers=headers)
        print(airtable_request.json())

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