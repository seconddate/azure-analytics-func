"""
Created By - ayaan@mz.co.kr
Created At - 2023.12.04
"""

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.eventhub import EventHubProducerClient, EventData
import logging
from dotenv import load_dotenv
from datetime import datetime
import os
import json
import random
import sys

# 경로 설정
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)

# 환경 변수 로드
load_dotenv()

# Function App 정의
app = func.FunctionApp()

# Event Hub 연결 설정
event_hub_connection_str = os.getenv('EVENT_HUB_CONNECTION_STRING')
event_hub_name = os.getenv('EVENT_HUB_NAME')


def generate_employee_access_data():
    employee_id = random.randint(1000, 9999)
    date = datetime.now().strftime("%Y-%m-%d")
    time = datetime.now().strftime("%H:%M:%S")
    entrance = random.choice(['정문', '후문', '사이드문'])
    access_type = random.choice(['출입', '퇴실'])

    return {
        "EmployeeID": employee_id,
        "Date": date,
        "Time": time,
        "Entrance": entrance,
        "AccessType": access_type
    }


def send_data_to_event_hub(data):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=event_hub_connection_str,
        eventhub_name=event_hub_name
    )
    with producer:
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(data))
        producer.send_batch(event_data_batch)

@app.function_name(name="http_trigger")
@app.route(route="hello")
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # 데이터 생성
    employee_data = generate_employee_access_data()

    try:
        # 데이터를 Event Hub로 전송
        send_data_to_event_hub(json.dumps(employee_data))

    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

    return func.HttpResponse("Data written to Azure Data Lake Storage successfully", status_code=200)
