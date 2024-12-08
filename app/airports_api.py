from fastapi import FastAPI
from sqlmodel import Session
from functools import lru_cache
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from database.db import engine
import database.utils as db_utils

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.options("/{path:path}")
async def preflight_handler():
    return JSONResponse({"message": "CORS preflight OK"}, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS, POST",
        "Access-Control-Allow-Headers": "Authorization, Content-Type"
    })
    
@app.get("/")
def home():
    return {"message": "Hello World"}

@lru_cache(maxsize=1000)
@app.get("/nearest_airports/{user_id}")
def get_nearest_airport_for_user(user_id: int):
    with Session(engine) as session:
        nearest = db_utils.get_user_airport(session, user_id)
        
    if nearest is None:
        return {"error": f"User ID {user_id} not found in database."}
        
    return { 'airport_id': nearest }

@app.get("/nearest_airports_wikipedia/{user_id}")
def get_nearest_airports_for_user_wikipedia(user_id: int):
    with Session(engine) as session:
        nearest = db_utils.get_user_airport(session, user_id)
        
        if nearest is None:
            return {"error": f"User ID {user_id} not found in database."}
        
        wiki_link = db_utils.get_airports_wiki_link(session, nearest)
        
    if wiki_link is None:
        return {"error": f"Wikipedia link not found for airport {nearest}"}
        
    return { 'wikipedia_page': wiki_link }
