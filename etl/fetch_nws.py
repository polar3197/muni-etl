# Fetches and parses pb file from GTFS into df and then parquet 
# and stores in $RT_MUNI_PATH

from datetime import datetime, timezone
import pandas as pd
import requests
import json
import os

output_dir = os.getenv("WEATHER_DATA_PATH")

try:
    url = "https://api.openweathermap.org/data/2.5/weather?lat=37.7749&lon=-122.4194&appid=b238d81d1a09771d9a8fed72e8db6fd6"
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Weather API call failed: {response.status_code} - {response.text}")
    
    weather_json = response.json()
    local_dt = datetime.fromtimestamp(weather_json["dt"])
    weather_json["timestamp_iso"] = local_dt.isoformat()

    df = pd.json_normalize(weather_json)

    timestamp_dt = datetime.fromisoformat(weather_json["timestamp_iso"])
    timestamp_str = timestamp_dt.strftime("%Y%m%d_%H%M%S")
    parquet_filename = f"weather_{timestamp_str}.parquet"
    # translate to parquet
    print(output_dir)
    df.to_parquet(os.path.join(output_dir, parquet_filename))

except Exception as e:
    print("Error:", e)