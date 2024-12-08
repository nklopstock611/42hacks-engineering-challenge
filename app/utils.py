import time
import requests
import numpy as np
import pandas as pd
from threading import Semaphore

API_LIMIT = 60  # requests per second
ERROR_LIMIT = 344  # requests per second in case of error
semaphore = Semaphore(API_LIMIT)

def rate_limited_request(url: str, params=None, retries=3, timeout=10, limit=API_LIMIT):
    """
    Performs an HTTP request with error handling and retries, respecting
    the rate of requests. It uses a semaphore to limit the number of
    concurrent requests that can be made to the external API.
    """
    for attempt in range(retries):
        with semaphore: # ensures that the rate limit is respected
            try:
                response = requests.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(1)  # waits before doing another attempt
                else:
                    raise e

def get_user_latitude_longitude(user_id: int) -> dict:
    """
    Returns the latitude and longitude of a user given their id.
    """
    url = 'https://sccr8pgns0.execute-api.us-east-1.amazonaws.com/dev/locations/' + str(user_id)
    response = rate_limited_request(url)
    data = response['data']
    latitude = float(data['latitude']['N'])
    longitude = float(data['longitude']['N'])
    
    return {
        'latitude': latitude,
        'longitude': longitude
    }

def extract_lat_lon(csv_path: str, lat_column: str, lon_column: str) -> list:
    """
    Extracts a list of tuples with the latitudes and longitudes from a CSV file.
    """
    df = pd.read_csv(csv_path)
    lat_lon_array = list(zip(df[lat_column], df[lon_column]))
    
    return lat_lon_array

def haversine(lat1, lon1, lats, lons, radius=6378) -> np.array:
    """   
    Calculates the distance between two points using the Haversine formula.
    It's using numpy, because it's more efficient than using math.
    Important to note that the radius is the radius from the Earth and it is in km.
    """
    # to radians
    lat1, lon1 = np.radians(lat1), np.radians(lon1)
    lats, lons = np.radians(lats), np.radians(lons)
    
    # deltas for lat and lon
    delta_lat = lats - lat1
    delta_lon = lons - lon1
    
    # haversine formula
    a = np.sin(delta_lat / 2) ** 2 + np.cos(lat1) * np.cos(lats) * np.sin(delta_lon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    
    # final distance in kms
    distance = radius * c
    
    return distance

def find_nearest_airport(user_id: int) -> dict:
    """
    Uses numpy to find the nearest airport to a user, given their id.   
    """
    user_location = get_user_latitude_longitude(user_id)
    user_lat = user_location['latitude']
    user_lon = user_location['longitude']
    
    df = pd.read_csv('data/airports_w_wiki.csv')
    lats = df['latitude_deg'].astype(float).values
    lons = df['longitude_deg'].astype(float).values
    ids = df['id'].values
    names = df['name'].values
    
    # calculates all distances at once (because it's using numpy)
    distances = haversine(user_lat, user_lon, lats, lons)
    
    # looks for the index of the nearest airport
    min_idx = np.argmin(distances)
    
    return {
        'airport_id': int(ids[min_idx]),
        'airport_name': str(names[min_idx]),
        'distance': float(distances[min_idx])
    }