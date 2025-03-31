from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import sessionmaker, declarative_base
from pydantic import BaseModel

Base = declarative_base()


class Song(Base):
    __tablename__ = "subtracks"  # Fixed double underscores
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


class SongDatabase:
    def __init__(self, db_url='mysql+pymysql://admin:SLaeLxy6LAo9Sag9@songs.cje0wa8qioij.eu-north-1.rds.amazonaws.com/tracks'):
        self.engine = create_engine(db_url)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.session = SessionLocal()

    def _fetch_song(self, query):  # Fixed method name with underscore
        try:
            result = self.session.execute(text(query)).fetchone()  # Added text() wrapper
            return SongSchema(**result._asdict()) if result else None
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
        return self._fetch_song(query)

    def get_less_energy_song(self, energy, tempo):
        query = f"""
            SELECT id, artists, name, energy, normalized_tempo, 
                   ROUND(SQRT(POW(energy - {energy}, 2) + POW(normalized_tempo - {tempo}, 2)), 6) AS distance
            FROM subtracks
            WHERE energy < {energy}
            ORDER BY distance ASC
            LIMIT 1
        """
        return self._fetch_song(query)

    def get_less_tempo_song(self, energy, tempo):
        query = f"""
            SELECT id, artists, name, energy, normalized_tempo, 
                   ROUND(SQRT(POW(energy - {energy}, 2) + POW(normalized_tempo - {tempo}, 2)), 6) AS distance
            FROM subtracks
            WHERE normalized_tempo < {tempo}
            ORDER BY distance ASC
            LIMIT 1
        """
        return self._fetch_song(query)

    def get_more_energy_song(self, energy, tempo):
        query = f"""
            SELECT id, artists, name, energy, normalized_tempo, 
                   ROUND(SQRT(POW(energy - {energy}, 2) + POW(normalized_tempo - {tempo}, 2)), 6) AS distance
            FROM subtracks
            WHERE energy > {energy}
            ORDER BY distance ASC
            LIMIT 1
        """
        return self._fetch_song(query)

    def get_more_tempo_song(self, energy, tempo):
        query = f"""
            SELECT id, artists, name, energy, normalized_tempo, 
                   ROUND(SQRT(POW(energy - {energy}, 2) + POW(normalized_tempo - {tempo}, 2)), 6) AS distance
            FROM subtracks
            WHERE normalized_tempo > {tempo}
            ORDER BY distance ASC
            LIMIT 1
        """
        return self._fetch_song(query)

    def get_song_by_id(self, id):
        query = f"""
            SELECT id, artists, name, energy, normalized_tempo
            FROM subtracks
            WHERE id = '{id}'
            LIMIT 1
        """
        return self._fetch_song(query)


