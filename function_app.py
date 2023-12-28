"""
Created By - ayaan@mz.co.kr
Created At - 2023.12.04
"""

import azure.functions as func
from azure.eventhub import EventHubProducerClient, EventData
import logging
from datetime import datetime, timezone
import os
import sys

# 경로 설정
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)

# Function App 정의
app = func.FunctionApp()


@app.function_name(name="http_trigger")
@app.route(route="hello")
def main(req: func.HttpRequest) -> func.HttpResponse:
    utc_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    logging.info('HTTP trigger function executed successfully!')

    # Private Event Hub 연결 정보
    eventhub_name = os.environ['EVENTHUB_NAME']
    eventhub_connection = os.environ['EVENT_HUB_CONNECTION']

    # Private Event Hub에 연결
    print(eventhub_connection)
    client = EventHubProducerClient.from_connection_string(
        conn_str=eventhub_connection,
        eventhub_name=eventhub_name
    )

    # 새로운 SalesMemo 데이터 생성
    new_salesmemo_data = {
        'date': datetime.utcnow().isoformat(),
        'content': '새로운 이벤트',
        'status': '계획 중',
        'owner': '김영업'
    }

    # 데이터 전송
    with client:
        event_data_batch = client.create_batch()
        event_data_batch.add(EventData(str(new_salesmemo_data)))
        client.send_batch(event_data_batch)
        logging.info('Data sent to Private Event Hub')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
