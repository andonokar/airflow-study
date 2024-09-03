import requests
from airflow.decorators import dag, task
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.providers.slack.notifications.slack import SlackNotifier
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from include.stock_market.tasks import get_stock_prices, store_prices, get_formatter_csv, BUCKET_NAME
from airflow.providers.docker.operators.docker import DockerOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

SYMBOL = "AAPL"


@dag(
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['stock_market'],
    on_success_callback=SlackNotifier(
        text="{{ dag.dag_id }} DAG succeeded!",
        channel="#monitoring",
        slack_conn_id="slack")
)
def stockmarket():
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        print(response.text)
        condition = response.json()['finance']['error'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_prices_task = PythonOperator(
        task_id='get_stock_prices',
        python_callable=get_stock_prices,
        op_kwargs={'symbol': SYMBOL, "url": '{{ task_instance.xcom_pull(task_ids="is_api_available") }}'},
    )

    store_prices_task = PythonOperator(
        task_id='store_prices',
        python_callable=store_prices,
        op_kwargs={"stock": '{{ task_instance.xcom_pull(task_ids="get_stock_prices") }}'}
    )

    format_prices_task = DockerOperator(
        task_id='format_prices',
        image="airflow/spark-app",
        container_name="format_prices",
        api_version="auto",
        auto_remove="force",
        docker_url="tcp://docker-proxy:2375",
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            "SPARK_APPLICATION_ARGS": '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        }
    )

    get_formatter_csv_task = PythonOperator(
        task_id='get_formatter_csv',
        python_callable=get_formatter_csv,
        op_kwargs={
            "path": '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        }
    )

    load_to_dw_task = aql.load_file(
        task_id='load_to_dw',
        input_file=File(path=f"s3://{BUCKET_NAME}/" '{{ task_instance.xcom_pull(task_ids="get_formatter_csv") }}',
                        conn_id="minio"),
        output_table=Table(
            name='stock_market',
            conn_id="postgres",
            metadata=Metadata(
                schema="public"
            )
        )
    )

    (is_api_available() >> get_stock_prices_task >> store_prices_task >>
     format_prices_task >> get_formatter_csv_task >> load_to_dw_task)


stockmarket()
