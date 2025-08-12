
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
import os

@dag(
    dag_id='weather_update_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=30),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 3,
    }
)
def weather_pipeline():
    """Weather data pipeline DAG"""
    
    run_weather_script = BashOperator(
        task_id='fetch_weather_data',
        bash_command='cd /path/to/your/scripts && WEATHER_DATA_PATH=/mnt/ssd/weather python3 weather_script.py',
        env={
            'WEATHER_DATA_PATH': '/mnt/ssd/weather'
        }
    )
    
    return run_weather_script

# Instantiate the DAG
weather_pipeline()