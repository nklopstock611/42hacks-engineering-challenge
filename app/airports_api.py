import requests

from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, File, UploadFile, HTTPException

import app.utils as utils

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

@app.get("/nearest_airports/{user_id}")
def get_nearest_airport_for_user(user_id: int):
    nearest = utils.find_nearest_airport(user_id)
    return { 'airport_id': nearest['airport_id'] }

@app.get("/nearest_airports_wikipedia/{user_id}")
def get_nearest_airoports_for_user_wikipedia(user_id: int):
    return { 'wikipedia_page': 'www' }
