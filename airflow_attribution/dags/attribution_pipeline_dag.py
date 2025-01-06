from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create DAG
dag = DAG(
    'attribution_pipeline',
    default_args=default_args,
    description='Attribution pipeline with multiple Docker containers',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
)

# Define the shared volume configurations
volume_config = {
    '/path/to/your/local/data': {'bind': '/app/data', 'mode': 'rw'},
    '/path/to/your/local/output': {'bind': '/app/output', 'mode': 'rw'}
}

# Task 1: Database Setup
setup_db = DockerOperator(
    task_id='setup_database',
    image='attribution-pipeline',
    command='python /app/pipeline/setup_db.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    auto_remove=True,
    mount_tmp_dir=False,
    mounts=[
        {
            'source': '/Users/dhruvchandel/VSCodeProjects/HaenselDataEngg/data/',
            'target': '/app/data',
            'type': 'bind'
        }
    ],
    dag=dag
)

# Task 2: Build Customer Journey
build_journey = DockerOperator(
    task_id='build_customer_journey',
    image='attribution-pipeline',
    command='python /app/pipeline/build_customer_journey.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    auto_remove=True,
    mount_tmp_dir=False,
    mounts=[
        {
            'source': '/Users/dhruvchandel/VSCodeProjects/HaenselDataEngg/data/',
            'target': '/app/data',
            'type': 'bind'
        }
    ],
    dag=dag
)

# Task 3: Send to API
send_to_api = DockerOperator(
    task_id='send_to_api',
    image='attribution-pipeline',
    command='python /app/pipeline/send_to_api.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    auto_remove=True,
    mount_tmp_dir=False,
    environment={
        'API_KEY': '17e9cc65-2e46-4345-9210-36860a63f435',
        'CONV_TYPE_ID': 'data_engineering_challenge'
    },
    mounts=[
        {
            'source': '/Users/dhruvchandel/VSCodeProjects/HaenselDataEngg/data/',
            'target': '/app/data',
            'type': 'bind'
        }
    ],
    dag=dag
)

# Task 4: Process Attribution
process_attribution = DockerOperator(
    task_id='process_attribution',
    image='attribution-pipeline',
    command='python /app/pipeline/attribution_processor.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    auto_remove=True,
    mount_tmp_dir=False,
    environment={
        'API_KEY': '17e9cc65-2e46-4345-9210-36860a63f435',
        'CONV_TYPE_ID': 'data_engineering_challenge',
        'OUTPUT_PATH': '/app/output/channel_reporting.csv'
    },
    mounts=[
        {
            'source': '/Users/dhruvchandel/VSCodeProjects/HaenselDataEngg/data/',
            'target': '/app/data',
            'type': 'bind'
        },
        {
            'source': '/Users/dhruvchandel/Desktop/',
            'target': '/app/output',
            'type': 'bind'
        }
    ],
    dag=dag
)

# Set task dependencies
setup_db >> build_journey >> send_to_api >> process_attribution