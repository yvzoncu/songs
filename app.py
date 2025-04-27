from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from queries import SongDatabase
from sqlalchemy.exc import OperationalError, InterfaceError
import pymysql
import time
import asyncio
from sqlalchemy import text
from pydantic import BaseModel
from transformers import pipeline
from typing import Dict

app = FastAPI()

# CORS middleware
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create an instance of the SongDatabase class
song_db = SongDatabase()


# Add a connection health check middleware
@app.middleware("http")
async def db_connection_middleware(request: Request, call_next):
    """Middleware to ensure database connection is healthy before processing requests."""
    global song_db

    # Skip connection check for non-API routes (like docs, static files)
    if not request.url.path.startswith("/song"):
        return await call_next(request)

    # Check if connection is valid
    try:
        # Try a simple query to validate the connection
        song_db.session.execute(text("SELECT 1"))
    except Exception as e:
        print(f"Database connection check failed: {str(e)}")
        # Refresh connection
        song_db.refresh_connection()
        print("Database connection refreshed successfully")

    # Process the request
    response = await call_next(request)
    return response


# Enhanced retry logic for database operations
def execute_with_retry(query_function, retries=3, delay=5):
    """Retry database operation on various connection issues."""
    last_exception = None

    for attempt in range(retries):
        try:
            # Try to execute the function
            result = query_function()
            return result

        except (OperationalError, InterfaceError, pymysql.err.InterfaceError) as e:
            last_exception = e
            error_str = str(e)
            print(f"Database error (attempt {attempt + 1}/{retries}): {error_str}")

            # Handle connection loss
            if 'Lost connection' in error_str or '(0, \'\')' in error_str:
                print(f"Connection lost, retrying after {delay}s")
                time.sleep(delay)
                song_db.refresh_connection()

            # Handle transaction issues
            elif 'invalid transaction' in error_str or 'rolled back' in error_str:
                print(f"Transaction issue, rolling back")
                song_db.refresh_connection()
                time.sleep(delay)

            else:
                # For other database errors, try to reconnect
                print(f"Other database error, refreshing connection")
                song_db.refresh_connection()
                time.sleep(delay)

        except Exception as e:
            # Catch any other unexpected errors
            last_exception = e
            print(f"Unexpected error: {str(e)}")
            # Consider if these should be retried or just raised
            if attempt < retries - 1:
                song_db.refresh_connection()
                time.sleep(delay)
            else:
                raise

    # If we've exhausted all retries
    raise Exception(f"Max retries reached, failed to execute database query. Last error: {str(last_exception)}")


# Start periodic connection check
@app.on_event("startup")
async def start_connection_monitor():
    asyncio.create_task(periodic_connection_check())


async def periodic_connection_check():
    """Periodically check database connection health."""
    while True:
        try:
            # Wait for 60 seconds
            await asyncio.sleep(60)

            # Test connection
            try:
                song_db.session.execute(text("SELECT 1"))
                print("Database connection healthy")
            except Exception as e:
                print(f"Connection check failed: {str(e)}")
                # Refresh connection
                song_db.refresh_connection()
                print("Database connection refreshed")
        except Exception as e:
            print(f"Error in connection monitor: {str(e)}")


@app.get("/song/recommend")
def recommend_song(energy: float, tempo: float):
    try:
        song = execute_with_retry(lambda: song_db.get_songs_by_slider(energy, tempo))
        if song:
            return song
        return {"error": "No matching song found"}
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}


@app.get("/song/less-energy")
def get_less_energy_song(energy: float, tempo: float):
    try:
        song = execute_with_retry(lambda: song_db.get_less_energy_song(energy, tempo))
        if song:
            return song
        return {"error": "No song with less energy found"}
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}


@app.get("/song/more-energy")
def get_more_energy_song(energy: float, tempo: float):
    try:
        song = execute_with_retry(lambda: song_db.get_more_energy_song(energy, tempo))
        if song:
            return song
        return {"error": "No song with more energy found"}
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}


@app.get("/song/less-tempo")
def get_less_tempo_song(energy: float, tempo: float):
    try:
        song = execute_with_retry(lambda: song_db.get_less_tempo_song(energy, tempo))
        if song:
            return song
        return {"error": "No song with less tempo found"}
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}


@app.get("/song/more-tempo")
def get_more_tempo_song(energy: float, tempo: float):
    try:
        song = execute_with_retry(lambda: song_db.get_more_tempo_song(energy, tempo))
        if song:
            return song
        return {"error": "No song with more tempo found"}
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}


@app.get("/song/mood_info")
def get_mood_info(energy: float, tempo: float):
    try:
        mood = execute_with_retry(lambda: song_db.get_mood_info(energy, tempo))
        if mood:
            return mood
        return {"error": "No mood text found"}
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}


@app.get("/song/{song_id}")
def get_song(song_id: str):
    try:
        song = execute_with_retry(lambda: song_db.get_song_by_id(song_id))
        if song:
            return song
        return {"error": "Song not found"}
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}



# Load the emotion classification pipeline
classifier = pipeline(
    task="text-classification",
    model="SamLowe/roberta-base-go_emotions",
    top_k=None  # Return all labels with their scores
)

class TextInput(BaseModel):
    text: str

@app.post("/predict")
async def predict_emotions(input_data: TextInput) -> Dict[str, float]:
    results = classifier(input_data.text)[0]
    emotions = {result['label']: result['score'] for result in results if result['score'] >= 0.2}
    return emotions
