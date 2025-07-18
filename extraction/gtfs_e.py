import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import shutil
import os
import requests
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
import pytz

# Fetch Protocol Buffer contents from MUNI API
url = "http://api.511.org/transit/vehiclepositions?api_key=59a9ae05-3f9c-440e-8343-e3256be79b84&agency=SF"
response = requests.get(url)

print(response.status_code)
print(response.headers.get('Content-Type'))

feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(response.content)
time_fetched = datetime.now().isoformat()

# Iterate through MUNI vehicles in operation
count = 0
vehicles = []
for entity in feed.entity:
    if entity.HasField("vehicle"):
        v = entity.vehicle

        dt = datetime.fromtimestamp(v.timestamp, tz=timezone.utc)
        local_tz = pytz.timezone("America/Los_Angeles")
        dt_local = dt.astimezone(local_tz)
        dt_local_12hr = dt_local.strftime("%I:%M:%S %p")

        # grab trip data, null if no current trip
        t = v.trip if v.HasField("trip") else None
        trip_id = t.trip_id if t else None
        route_id = t.route_id if t else None
        direction_id = t.direction_id if t else None
        start_date = t.start_date if t else None
        schedule_relationship = t.schedule_relationship if t else None

        vehicle = {
            # time data
            "timestamp_iso": dt_local.isoformat(),
            "year": dt_local.year,
            "month": dt_local.month,
            "day": dt_local.day,
            "hour": dt_local.hour,
            "day": dt_local.minute,
            
            # trip data
            "trip_id": trip_id,
            "route_id": route_id,
            "direction_id": direction_id,
            "start_date": start_date,
            "schedule_relationship": schedule_relationship,

            # vehicle data
            "vehicle_id": v.vehicle.id,
            "latitude": v.position.latitude,
            "longitude": v.position.longitude,
            #"bearing": v.position.bearing,
            "speed_mps": v.position.speed,
            "current_stop_sequence": v.current_stop_sequence,
            "current_status": v.current_status,
            "stop_id": v.stop_id,
            "occupancy_status": v.occupancy_status
        }
        vehicles.append(vehicle)
        count += 1
        # print(vehicle_json)

# convert data -> Pandas -> Parquet
vehicles_df = pd.DataFrame(vehicles)
vehicles_pq = vehicles_df.to_parquet(
    './temp_pq_store', 
    'pyarrow', 
    partition_cols=['year', 'month', 'day'],
    index=False
)

# push parquet file to S3 bucket
local_dir = './temp_pq_store'
bucket_name = 'muni-parquet-data'

s3 = boto3.client('s3')

for root, dirs, files in os.walk(local_dir):
    for file in files:
        local_path = os.path.join(root, file)
        relative_path = os.path.relpath(local_path, local_dir)
        s3_key = relative_path.replace("\\", "/")  # Windows fix

        print(f"Uploading {local_path} to s3://{bucket_name}/{s3_key}")
        s3.upload_file(local_path, bucket_name, s3_key)

shutil.rmtree('./temp_pq_store', ignore_errors=True)

    
    


