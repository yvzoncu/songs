import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import sessionmaker, declarative_base
from pydantic import BaseModel

# Load environment variables from a .env file
load_dotenv()

Base = declarative_base()


class Song(Base):
    __tablename__ = "subtracks"
    id = Column(Integer, primary_key=True, index=True)
    artists = Column(String, nullable=False)
    name = Column(String, nullable=False)
    energy = Column(Float, nullable=False)
    normalized_tempo = Column(Float, nullable=False)


class SongSchema(BaseModel):
    id: str
    artists: str
    name: str
    energy: float
    normalized_tempo: float
    distance: float | None = None

    class Config:
        from_attributes = True


class MoodText(BaseModel):
    title: str
    explanation: str

    class Config:
        from_attributes = True


class MoodPicture(BaseModel):
    url: str

    class Config:
        from_attributes = True


class SongDatabase:
    def __init__(self):
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')
        db_url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}'
        self.engine = create_engine(db_url)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.session = SessionLocal()

    def _fetch_song(self, query, item_type):
        try:
            result = self.session.execute(text(query)).fetchone()
            if result:
                if item_type == 'song':
                    return SongSchema(**result._asdict())
                elif item_type == 'mood':
                    return MoodText(**result._asdict())
                elif item_type == 'picture':
                    return MoodPicture(**result._asdict())
            return None
        except Exception as e:
            print(f"Database error: {e}")
            return None

    def get_songs_by_slider(self, energy, tempo):
        query = f"""
            SELECT id, artists, name, energy, normalized_tempo,
                   ROUND(SQRT(POW(energy - {energy}, 2) + POW(normalized_tempo - {tempo}, 2)), 6) AS distance
            FROM subtracks
            ORDER BY distance ASC
            LIMIT 1
        """
        return self._fetch_song(query, 'song')

    def get_less_energy_song(self, energy, tempo):
        query = f"""
            SELECT id, artists, name, energy, normalized_tempo,
                   ROUND(SQRT(POW(energy - {energy}, 2) + POW(normalized_tempo - {tempo}, 2)), 6) AS distance
            FROM subtracks
            WHERE energy < {energy}
            ORDER BY distance ASC
            LIMIT 1
        """
        return self._fetch_song(query, 'song')

    def get_less_tempo_song(self, energy, tempo):
        query = f"""
            SELECT id, artists, name, energy, normalized_tempo,
                   ROUND(SQRT(POW(energy - {energy}, 2) + POW(normalized_tempo - {tempo}, 2)), 6) AS distance
            FROM subtracks
            WHERE normalized_tempo < {tempo}
            ORDER BY distance ASC
            LIMIT 1
        """
        return self._fetch_song(query, 'song')

    def get_more_energy_song(self, energy, tempo):
        query = f"""
            SELECT id, artists, name, energy, normalized_tempo,
                   ROUND(SQRT(POW(energy - {energy}, 2) + POW(normalized_tempo - {tempo}, 2)), 6) AS distance
            FROM subtracks
            WHERE energy > {energy}
            ORDER BY distance ASC
            LIMIT 1
        """
        return self._fetch_song(query, 'song')

    def get_more_tempo_song(self, energy, tempo):
        query = f"""
            SELECT id, artists, name, energy, normalized_tempo,
                   ROUND(SQRT(POW(energy - {energy}, 2) + POW(normalized_tempo - {tempo}, 2)), 6) AS distance
            FROM subtracks
            WHERE normalized_tempo > {tempo}
            ORDER BY distance ASC
            LIMIT 1
        """
        return self._fetch_song(query, 'song')

    def get_mood_text(self, energy, tempo):
        query = f"""
            SELECT title, explanation
            FROM moods
            WHERE tempo = {tempo} and energy = {energy}
            LIMIT 1
        """
        return self._fetch_song(query, 'mood')

    def get_mood_picture(self, energy, tempo):
        query = f"""
            SELECT url
            FROM mood_pictures
            WHERE tempo = {tempo} and energy = {energy}
            LIMIT 1
        """
        return self._fetch_song(query, 'picture')

    def get_song_by_id(self, id):
        query = f"""
            SELECT id, artists, name, energy, normalized_tempo
            FROM subtracks
            WHERE id = '{id}'
            LIMIT 1
        """
        return self._fetch_song(query, 'song')


a = SongDatabase()
m= a.get_mood_picture(0.5, 0.5)
print(m)
