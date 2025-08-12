import json
import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

def read_vehicle_into_postgres():
    """Read the hot JSON file and insert into PostgreSQL"""
    
    hot_file = "/mnt/ssd/hot/map_data.json"
    
    # Check if file exists
    if not os.path.exists(hot_file):
        raise Exception(f"Hot file {hot_file} not found")
    
    # Check file age (optional - skip if file is too old)
    file_age = datetime.now().timestamp() - os.path.getmtime(hot_file)
    if file_age > 300:  # 5 minutes old
        print(f"Warning: File is {file_age:.1f} seconds old")
    
    # Read the JSON data
    try:
        with open(hot_file, 'r') as f:
            vehicles = json.load(f)
    except json.JSONDecodeError as e:
        raise Exception(f"Invalid JSON in {hot_file}: {e}")
    
    if not vehicles:
        print("No vehicle data to process")
        return "No data found"
    
    print(f"Found {len(vehicles)} vehicles to process")
    
    # Database configuration
    DB_CONFIG = {
        'host': "localhost",
        'database': "transit",      # database name
        'user': "postgres",
        'port': 5432
        'password': "Chuckles2001!" 
    }
    TABLE_NAME = "vehicle_positions"  # table name
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False  # Use transactions
        
    except psycopg2.Error as e:
        raise Exception(f"Database connection failed: {e}")
    
    try:
        cur = conn.cursor()
        
        # Prepare data for bulk insert
        insert_data = []
        for vehicle in vehicles:
            # Handle None/null values and convert types as needed
            row = (
                vehicle.get('timestamp_iso'),
                vehicle.get('year'),
                vehicle.get('month'), 
                vehicle.get('day'),
                vehicle.get('hour'),
                vehicle.get('minute'),
                vehicle.get('trip_id'),
                vehicle.get('route_id'),
                vehicle.get('direction_id'),
                vehicle.get('start_date'),
                vehicle.get('schedule_relationship'),
                vehicle.get('vehicle_id'),
                vehicle.get('latitude'),
                vehicle.get('longitude'), 
                vehicle.get('speed_mps'),
                vehicle.get('current_stop_sequence'),
                vehicle.get('current_status'),
                vehicle.get('stop_id'),
                vehicle.get('occupancy_status')
            )
            insert_data.append(row)
        
        # Bulk insert using execute_values (much faster than executemany)
        insert_sql = f"""
            INSERT INTO {TABLE_NAME} (
                timestamp_iso, year, month, day, hour, minute,
                trip_id, route_id, direction_id, start_date, schedule_relationship,
                vehicle_id, latitude, longitude, speed_mps, current_stop_sequence,
                current_status, stop_id, occupancy_status
            ) VALUES %s
        """
        
        execute_values(
            cur, 
            insert_sql, 
            insert_data,
            template=None,
            page_size=100
        )
        
        # Commit the transaction
        conn.commit()
        
        print(f"Successfully inserted {len(vehicles)} vehicle records")
        return f"Inserted {len(vehicles)} vehicles at {datetime.now().isoformat()}"
        
    except psycopg2.Error as e:
        # Rollback on error
        conn.rollback()
        raise Exception(f"Database insert failed: {e}")
        
    except Exception as e:
        conn.rollback()
        raise Exception(f"Unexpected error: {e}")
        
    finally:
        cur.close()
        conn.close()