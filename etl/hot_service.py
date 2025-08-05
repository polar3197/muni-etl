from fastapi import FastAPI
from fastapi.responses import JSONResponse
import json
import os

app = FastAPI()

# mounted on RPi's 128GB ssd drive
HOT_DATA_PATH = "/mnt/ssd/hot/map_data.json"

# when someone runs get on /hot-data, run the following code
@app.get("/hot-data")
async def get_hot_data():
    if not os.path.exists(HOT_DATA_PATH):
        return JSONResponse(content={"error": "No data yet"}, status_code=404)
    try:
        with open(HOT_DATA_PATH, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)