import json
from pendulum import datetime

from airflow.decorators import (
    dag,
    task,
)

@dag(
    schedule="@daily",
    start_date
)