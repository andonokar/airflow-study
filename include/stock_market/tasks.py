import json
from io import BytesIO

import requests
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
import logging
from minio import Minio

BUCKET_NAME = 'stock-market'


def get_minio_client():
    minio = BaseHook.get_connection("minio")
    return Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )


def get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection("stock_api")
    response = requests.get(url, api.extra_dejson['headers'])
    logging.info(response.text)
    return json.dumps(response.json()["chart"]["result"][0])


def store_prices(stock):
    client = get_minio_client()
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf-8')
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    return f"{objw.bucket_name}/{symbol}"


def get_formatter_csv(path):
    client = get_minio_client()
    prefix_name = f"{path.split('/')[-1]}/formatted_prices/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith(".csv"):
            return obj.object_name
    raise AirflowNotFoundException("The csv file does not exist")
