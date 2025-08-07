
# Fetches and parses pb file from GTFS into df and then parquet 
# and stores in $RT_MUNI_PATH

from google.transit import gtfs_realtime_pb2
from datetime import datetime, timezone
import pandas as pd
import requests
import os
import json
import pytz

output_dir = os.getenv("MUNI_RT_DATA_PATH")

try:
    # Fetch Protocol Buffer contents from MUNI API
    url = "http://api.511.org/transit/vehiclepositions?api_key=5e6bf5d8-1d98-4eb3-b927-69dcab843474&agency=SF"
    response = requests.get(url)

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    time_fetched = datetime.now().isoformat()

    # Iterate through MUNI vehicles in operation
    count = 0
    vehicles = []
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v = entity.vehicle

            # collect time information
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
                "minute": dt_local.minute,
                
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
    if vehicles:
        latest_datetime = max(datetime.fromisoformat(v["timestamp_iso"]) for v in vehicles)
        timestamp_str = latest_datetime.strftime("%Y%m%d_%H%M%S")
        parquet_filename = f"vehicles_{timestamp_str}.parquet"
        df = pd.DataFrame(vehicles)
        # translate to parquet
        df.to_parquet(os.path.join(output_dir, parquet_filename), engine="pyarrow")
except Exception as e:
    print("Error:", e)