import requests
import numpy as np
import pandas as pd

def get_user_latitude_longitude(user_id: int):
    """
    Retorna la latitud y longitud de un usuario dado su id.
    
    Args:
        user_id (int): El id del usuario.
        
    Returns:
        dict: Un diccionario con las llaves 'latitude', 'longitude' y el id del usuario.
    """
    response = requests.get('https://sccr8pgns0.execute-api.us-east-1.amazonaws.com/dev/locations/' + str(user_id))
    data = response.json()['data']
    latitude = float(data['latitude']['N'])
    longitude = float(data['longitude']['N'])
    
    return {
        'latitude': latitude,
        'longitude': longitude
    }

def extract_lat_lon(csv_path, lat_column, lon_column):
    """
    Extrae un arreglo de tuplas con las latitudes y longitudes de un archivo CSV.

    Args:
        csv_path (str): Ruta del archivo CSV.
        lat_column (str): Nombre de la columna de latitudes.
        lon_column (str): Nombre de la columna de longitudes.

    Returns:
        list[tuple]: Una lista de tuplas (latitud, longitud).
    """
    df = pd.read_csv(csv_path)
    lat_lon_array = list(zip(df[lat_column], df[lon_column]))
    return lat_lon_array

def haversine(lat1, lon1, lats, lons, radius=6371):
    """
    Calcula la distancia entre dos puntos usando la fórmula de Haversine.
    Se calculan usando la librería numpy, porque es más eficiente que math.

    Args:
        lat1, lon1 (float): Latitud y longitud del primer punto en grados.
        lat2, lon2 (float): Latitud y longitud del segundo punto en grados.
        radius (float): Radio de la Tierra (6371 km).

    Returns:
        float: Distancia entre los dos puntos en la unidad del radio.
    """
    # primero se convierten las medidas a radianes
    lat1, lon1 = np.radians(lat1), np.radians(lon1)
    lats, lons = np.radians(lats), np.radians(lons)
    
    # se sacan ambos deltas, lat y lon
    delta_lat = lats - lat1
    delta_lon = lons - lon1
    
    a = np.sin(delta_lat / 2)**2 + np.cos(lat1) * np.cos(lats) * np.sin(delta_lon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    
    # distancia final en kms
    distance = radius * c
    
    return distance

def find_nearest_airport(user_id: int):
    """
    Usa numpy para encontrar el aeropuerto más cercano a un usuario, dado su id.
    
    Args:
        user_id (int): El id del usuario.
        
    Returns:
        dict: Un diccionario con las llaves 'id', 'name' y 'distance' del aeropuerto más cercano.    
    """
    user_location = get_user_latitude_longitude(user_id)
    user_lat = user_location['latitude']
    user_lon = user_location['longitude']
    
    df = pd.read_csv('data/airports_w_wiki.csv')
    lats = df['latitude_deg'].astype(float).values
    lons = df['longitude_deg'].astype(float).values
    ids = df['id'].values
    names = df['name'].values
    
    # como se usa numpy, se calculan todas las distancias a la vez
    distances = haversine(user_lat, user_lon, lats, lons)
    
    # se busca el índice del aeropuerto más cercano
    min_idx = np.argmin(distances)
    
    return {
        'airport_id': int(ids[min_idx]),
        'airport_name': str(names[min_idx]),
        'distance': float(distances[min_idx])
    }