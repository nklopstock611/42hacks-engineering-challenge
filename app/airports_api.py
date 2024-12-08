import time
import requests
from fastapi import FastAPI
from sqlmodel import Session
from threading import Thread
from contextlib import asynccontextmanager
from fastapi.responses import ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware

from database.db import engine
import database.utils as db_utils

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting the application...")
    Thread(target=warm_up_server).start()
    yield

app = FastAPI(default_response_class=ORJSONResponse, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def warm_up_server():
    """
    Warm-up function that makes a request to the server and checks if it's running.
    """
    time.sleep(2)
    BASE_PATH = "https://four2hacks-engineering-challenge.onrender.com"
    try:
        response = requests.get(BASE_PATH + "/nearest_airports/1") # request de "calentamiento"
        if response.status_code == 200:
            print("Server warmed up successfully.")
        else:
            print(f"Warm-up request failed with status code: {response.status_code}")
    except Exception as e:
        print(f"Error warming up the server: {e}")

@app.get("/")
def read_root():
    return { 'message': 'Welcome to the airports API!' }

@app.get("/nearest_airports/{user_id}")
def get_nearest_airport_for_user(user_id: int):
    """
    Endpoint that returns the nearest airport to a user given their id.
    """
    with Session(engine) as session:
        nearest = db_utils.get_user_airport(session, user_id)
        
    return { 'airport_id': nearest if nearest else 'User not found' }

@app.get("/nearest_airports_wikipedia/{user_id}")
def get_nearest_airports_for_user_wikipedia(user_id: int):
    """
    Endpoint that returns the Wikipedia page link of the nearest airport
    to a user given their id.
    """
    with Session(engine) as session:
        nearest = db_utils.get_user_airport(session, user_id)
        
        if nearest:
            wiki_link = db_utils.get_airports_wiki_link(session, nearest)
        
    return { 'wikipedia_page': wiki_link if wiki_link else 'User not found' }
