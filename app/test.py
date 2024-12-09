import time
import requests

import app.utils as app_utils

# BASE_PATH = 'http://127.0.0.1:8000'
BASE_PATH = 'https://four2hacks-engineering-challenge.onrender.com'

def test_endpoints(user_id: int):
    # Medir tiempo del primer request
    start_time = time.time()
    response_airport = requests.get(BASE_PATH + '/nearest_airports/' + str(user_id))
    delta_time_airport = time.time() - start_time

    airport_id = response_airport.json()["airport_id"]

    print(f"Tiempo de respuesta para '/nearest_airports/': {delta_time_airport:.4f} segundos")

    # Medir tiempo del segundo request
    start_time = time.time()
    response_wikipedia = requests.get(BASE_PATH + '/nearest_airports_wikipedia/' + str(user_id))
    delta_time_wikipedia = time.time() - start_time

    wikipedia_page = response_wikipedia.json()["wikipedia_page"]

    print(f"Tiempo de respuesta para '/nearest_airports_wikipedia/': {delta_time_wikipedia:.4f} segundos")

    print(f"El aeropuerto más cercano al usuario es: {airport_id} - Wikipedia: {wikipedia_page}")

def test_distance_calculation(user_id: int):
    start_time = time.time()
    app_utils.find_nearest_airport(user_id)
    delta_time_wikipedia = time.time() - start_time
    
    print(f"Tiempo de respuesta para 'find_nearest_airport': {delta_time_wikipedia:.4f} segundos")

def test_root():
    start_time = time.time()
    requests.get(BASE_PATH + '/')
    delta_time = time.time() - start_time
    print(f"Tiempo de respuesta para '/': {delta_time:.4f} segundos")
    
if __name__ == "__main__":
    user_id = 0
    test_root()
    test_distance_calculation(user_id)
    test_endpoints(user_id)