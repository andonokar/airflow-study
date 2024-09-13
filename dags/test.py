from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.docker.decorators.docker import docker_task


@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['test']
)
def test():
    
    @task
    def start():
        print('Hi')
        
    start()


test()
