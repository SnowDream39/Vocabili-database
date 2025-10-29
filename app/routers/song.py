from fastapi import APIRouter, Depends, Query, Body, HTTPException

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, and_
from sqlalchemy.dialects.postgresql import insert

from app.session import get_async_session
from app.models import Song
from app.schemas.entry import SongEdit

router = APIRouter(prefix="/song", tags=["song"])
