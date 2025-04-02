from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from queries import SongDatabase

app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


song_db = SongDatabase()


@app.get("/song/recommend")
def recommend_song(energy: float, tempo: float):
    song = song_db.get_songs_by_slider(energy, tempo)
    if song:
        return song
    return {"error": "No matching song found"}

@app.get("/song/less-energy")
def get_less_energy_song(energy: float, tempo: float):
    song = song_db.get_less_energy_song(energy, tempo)
    if song:
        return song
    return {"error": "No song with less energy found"}

@app.get("/song/more-energy")
def get_more_energy_song(energy: float, tempo: float):
    song = song_db.get_more_energy_song(energy, tempo)
    if song:
        return song
    return {"error": "No song with more energy found"}

@app.get("/song/less-tempo")
def get_less_tempo_song(energy: float, tempo: float):
    song = song_db.get_less_tempo_song(energy, tempo)
    if song:
        return song
    return {"error": "No song with less tempo found"}

@app.get("/song/more-tempo")
def get_more_tempo_song(energy: float, tempo: float):
    song = song_db.get_more_tempo_song(energy, tempo)
    if song:
        return song
    return {"error": "No song with more tempo found"}


@app.get("/song/{song_id}")
def get_song(song_id: str):
    song = song_db.get_song_by_id(song_id)
    if song:
        return song
    return {"error": "Song not found"}


