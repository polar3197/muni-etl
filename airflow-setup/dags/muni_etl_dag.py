
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import subprocess

@dag(
    dag_id='muni_pipeline', 
    schedule=timedelta(seconds=60),
    start_date=datetime(2025, 8, 8),
    catchup=False,
    max_active_runs=1
)
def muni_dag():
    @task
    def fetch_hot_muni():
        result = subprocess.run([
            'python3', '/home/charlie.c/transit-etl/etl/muni_for_map.py'
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"Muni script failed: {result.stderr}")
            
        print(result.stdout)
        return "Muni data processed"
    
    @task
    def read_vehicle_into_pgres():
        
    
    fetch_hot_muni()

muni_dag()
