import time
import requests
from fastapi import FastAPI
from sqlmodel import Session
from threading import Thread
from functools import lru_cache
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
    print("Shutting down the application...")

app = FastAPI(default_response_class=ORJSONResponse, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/healthcheck")
def healthcheck():
    return {"status": "ok"}

def warm_up_server():
    time.sleep(2)
    BASE_PATH = "http://127.0.0.1:8000"
    try:
        response = requests.get(BASE_PATH + "/nearest_airports/1") # request de "calentamiento"
        if response.status_code == 200:
            print("Server warmed up successfully.")
        else:
            print(f"Warm-up request failed with status code: {response.status_code}")
    except Exception as e:
        print(f"Error warming up the server: {e}")

@lru_cache(maxsize=1000)
@app.get("/nearest_airports/{user_id}")
def get_nearest_airport_for_user(user_id: int):
    with Session(engine) as session:
        nearest = db_utils.get_user_airport(session, user_id)
        
    return { 'airport_id': nearest if nearest else 'User not found' }

@app.get("/nearest_airports_wikipedia/{user_id}")
def get_nearest_airports_for_user_wikipedia(user_id: int):
    with Session(engine) as session:
        nearest = db_utils.get_user_airport(session, user_id)
        
        if nearest:
            wiki_link = db_utils.get_airports_wiki_link(session, nearest)
        
    return { 'wikipedia_page': wiki_link if wiki_link else 'User not found' }
