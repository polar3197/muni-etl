import json
import os

# want to read from PATH_TO_HOT_MUNI_DATA

with open(os.environ.get('PATH_TO_HOT_MUNI_DATA')) as vehicle_json:
    data = json.load(vehicle_json)

print(data)

# parse what is read into a python dict (with json dump?)

# format data according to columns of postgres vehicles table

# open connection to my postgres db

# insert bulk vehicle data into postgres 'transit' db into 'vehicles' table