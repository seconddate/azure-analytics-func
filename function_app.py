"""
Created By - ayaan@mz.co.kr
Created At - 2023.12.04
"""

import azure.functions as func
import random
import logging
import os
import sys
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData
from datetime import datetime

# 경로 설정
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)

# Function App 정의
app = func.FunctionApp()
load_dotenv()


@app.function_name(name="fact-generator")
@app.route(route="fact-generator")
def test_function(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    logging.info('HTTP trigger function executed successfully!')

    # Private Event Hub 연결 정보
    eventhub_name = os.environ['EVENTHUB_NAME']
    eventhub_connection = os.environ['EVENT_HUB_CONNECTION']

    try:
        fact_data = generate_fact_data_list()

        # Event Hub 전송 로직
        client = EventHubProducerClient.from_connection_string(eventhub_connection, eventhub_name)
        event_data_batch = client.create_batch()

        for data in fact_data:
            event_data_batch.add(EventData(str(data)))

        client.send_batch(event_data_batch)
    except Exception as ex:
        logging.error(f'Error : {str(ex)}')

    return func.HttpResponse(f"Sent {len(fact_data)} items")


@app.function_name(name="fact-timer")
@app.schedule(schedule="0 0 1 * * *", arg_name="facttimer", run_on_startup=True, use_monitor=False)
def main(facttimer: func.TimerRequest) -> None:

    logging.info('Time trigger function executed successfully!')

    # Private Event Hub 연결 정보
    eventhub_name = os.environ['EVENTHUB_NAME']
    eventhub_connection = os.environ['EVENT_HUB_CONNECTION']

    try:
        fact_data = generate_fact_data_list()

        # Event Hub 전송 로직
        client = EventHubProducerClient.from_connection_string(eventhub_connection, eventhub_name)
        event_data_batch = client.create_batch()

        for data in fact_data:
            event_data_batch.add(EventData(str(data)))

        client.send_batch(event_data_batch)
    except Exception as ex:
        logging.error(f'Error : {str(ex)}')

    logging.info(f"Sent {len(fact_data)} items")


def get_mssql_connect():
    server = os.environ['MSSQL_SERVER']
    database = os.environ['MSSQL_DATABASE']
    username = os.environ['MSSQL_USERNAME']
    password = os.environ['MSSQL_PASSWORD']
    port = os.environ['MSSQL_PORT']

    # MSSQL 연결
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};PORT={port}'
    logging.info(f'MSSQL Connection String -> {conn_str}')
    return pyodbc.connect(conn_str)


def create_fact_data(dim_product, dim_event_type):
    non_billable_list = ['미팅', '제안', '제안서 작업', '컨퍼런스', '세미나', '웨비나']

    fact_data = {
        'CUSTOMER_ID': dim_product['CUSTOMER_ID'],
        'PRODUCT_ID': dim_product['ID'],
        'EVENT_TYPE_ID': dim_event_type['ID'],
        'EVENTED_AT': datetime.now(),
        'RECEIVED_AMOUNT': 0,
        'SPENT_AMOUNT': 0
    }

    if dim_event_type['EVENT_TYPE_NAME'] not in non_billable_list:
        first_digit = random.randint(1, 9)
        if dim_event_type['EVENT_TYPE_NAME'] == '계약금 지불':
            fact_data['RECEIVED_AMOUNT'] = dim_product['PRODUCT_PRICE'] * 0.05
        elif dim_event_type['EVENT_TYPE_NAME'] == '교육':
            fact_data['RECEIVED_AMOUNT'] = first_digit * 10000
        elif dim_event_type['EVENT_TYPE_NAME'] == '워크샵':
            fact_data['RECEIVED_AMOUNT'] = first_digit * 10000
        elif dim_event_type['EVENT_TYPE_NAME'] == '출장':
            fact_data['SPENT_AMOUNT'] = round(random.randint(100000, 300000), -2)
        elif dim_event_type['EVENT_TYPE_NAME'] == '고객 대접':
            fact_data['SPENT_AMOUNT'] = round(random.randint(100000, 1000000), -2)

    return fact_data


def generate_fact_data_list():
    fact_data = []
    dim_data = {}
    dim_tables = ['DIM_EVENT_TYPES', 'DIM_PRODUCTS']
    try:
        conn = get_mssql_connect()
        cursor = conn.cursor()
        logging.info('MSSQL Connection Success.')

        for dim_table in dim_tables:
            query = f"""
                SELECT TOP 2000 *
                FROM {dim_table}
                ORDER BY NEWID()
            """
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]

            # logging.info(results)
            dim_data[dim_table] = results
    except Exception as ex:
        logging.error(f'MSSQL Connection Fail. Error: {str(ex)}')
        raise ex

    for dim_product in dim_data['DIM_PRODUCTS']:
        dim_event_types = random.sample(dim_data['DIM_EVENT_TYPES'], 8)
        dim_event_types += random.sample(dim_data['DIM_EVENT_TYPES'], 8)

        for dim_event_type in dim_event_types:
            if dim_event_type['EVENT_TYPE_NAME'] != '잔금 지불':
                fact_data.append(create_fact_data(dim_product, dim_event_type))

    logging.info(f'Fact Data Sample : {fact_data[:4]}')

    df = pd.DataFrame(fact_data)

    start = pd.to_datetime('9:00').timestamp()
    end = pd.to_datetime('20:00').timestamp()

    seconds_per_bin = (end - start) / len(df)
    bins = pd.interval_range(start, periods=len(df), freq=f'{seconds_per_bin}S')

    df['event_at_group'] = pd.cut(df['EVENT_AT'], bins)

    # 결과 반환
    return df.to_dict('records')
