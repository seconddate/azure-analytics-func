"""
Created By - ayaan@mz.co.kr
Created At - 2023.12.04
"""

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
from datetime import datetime
import logging
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


def get_service_client_token_credential(account_name) -> DataLakeServiceClient:
  account_url = f"https://{account_name}.dfs.core.windows.net"
  token_credential = DefaultAzureCredential()
  service_client = DataLakeServiceClient(account_url, credential=token_credential)
  return service_client


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


@app.function_name(name="HttpTrigger1")
@app.route(route="main")
def main(req: func.HttpRequest) -> func.HttpResponse:
  logging.info('Python HTTP trigger function processed a request.')

  # 데이터 생성
  employee_data = generate_employee_access_data()

  try:
    # Azure Data Lake Storage 설정
    storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
    service_client = get_service_client_token_credential(storage_account_name)

    file_system_client = service_client.get_file_system_client(file_system="pnp")
    directory_client = file_system_client.get_directory_client("2023")
    file_client = directory_client.get_file_client("test.json")

    # 데이터를 JSON 형식으로 변환
    data = json.dumps(employee_data)

    # ADLS에 데이터 적재
    file_client.append_data(data=data, offset=0, length=len(data))
    file_client.flush_data(len(data))

  except Exception as e:
    return func.HttpResponse(f"Error: {str(e)}", status_code=500)

  return func.HttpResponse("Data written to Azure Data Lake Storage successfully", status_code=200)
