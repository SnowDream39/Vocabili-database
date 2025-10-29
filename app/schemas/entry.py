from pydantic import BaseModel

class ProducerEdit(BaseModel):
    id: int
    entry: str

class VocalEdit(BaseModel):
    id: int
    entry: str

class SongEdit(BaseModel):
    id: int
    entry: str

class ProducerSongAdd(BaseModel):
    producer_id: int
    song_id: int
    description: str | None = None
    image: str | None = None

class ProducerSongRemove(BaseModel):
    producer_id: int
    song_id: int

class ProducerSongOut(BaseModel):
    id: int
    producer_id: int
    song_id: int
    description: str | None