
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import subprocess

@dag(
    dag_id='weather_pipeline',
    schedule=timedelta(minutes=30),
    start_date=datetime(2025, 8, 8),
    catchup=False,
    max_active_runs=1
)
def weather_dag():
    @task
    def fetch_weather():
        result = subprocess.run([
            'python3', '/home/charlie.c/transit-etl/etl/weather_for_stats.py'
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"Weather script failed: {result.stderr}")
        
        print(result.stdout)
        return "Weather updated"
    
    fetch_weather()

weather_dag()