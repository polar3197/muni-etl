import requests
import json
import boto3
import os
from datetime import datetime, timedelta

API_KEY = "MW9S-E7SL-26DU-VV8V"  # Public demo key
STATION_ABBR = "EMBR"  # Embarcadero station

url = f"https://api.bart.gov/api/etd.aspx?cmd=etd&orig={STATION_ABBR}&key={API_KEY}&json=y"

# grab EMBR data
response = requests.get(url)
data = response.json()

# create dict of trimmed data
station = data["root"]["station"][0]
now = datetime.now().isoformat()
trimmed = {
    "timestamp": now,
    "station": station["name"],
    "abbr": station["abbr"],
    "etd": []
}

# print(station["etd"][0]["estimate"])

# for station EMBR
for etd in station["etd"]:
    destination = etd["destination"]
    # iterate through trains
    for estimate in etd["estimate"]:
        minutes = estimate["minutes"]
        if minutes.lower() == "leaving":
            minutes = 0
        trimmed["etd"].append({
            "destination": destination,
            "minutes": int(minutes),
            "delay": int(estimate["delay"])
        })

json_trimmed = json.dumps(trimmed, indent=2)

# print(json_trimmed)

filename = f"{now}-EMBR-data.json"
with open(filename, "a") as f:
  f.write(json_trimmed)

# store json on s3 bucket
s3 = boto3.client('s3')

try:
    s3.upload_file(f"{now}-EMBR-data.json", "rock-bart-data", f"embarcadero/{filename}")
    os.remove(filename)
    print(f"Uploaded and deleted local file: {filename}")
except Exception as e:
    print(f"Upload failed: {e}")
