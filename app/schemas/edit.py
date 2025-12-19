from pydantic import BaseModel
from app.schemas.artist import BasicArtistOut
from typing import Literal

class ConfirmRequest(BaseModel):
    task_id: str

class SongEdit(BaseModel):
    id: int
    name: str
    type: Literal['原创', '翻唱', '本家重置', '串烧']
    vocadb_id: int | None
    display_name: str
    
class VideoEdit(BaseModel):
    bvid: str
    title: str
    copyright: int
    disabled: bool