from fastapi import APIRouter, Depends, Query

from sqlalchemy.ext.asyncio import AsyncSession

from app.session import get_async_session
from app.crud.select import get_songs_detail, get_artist_songs, get_ranking, get_artist, get_song, get_song_by_achievement, get_video_snapshot_by_date, get_song_ranking, get_latest_ranking, get_ranking_top5, get_song_snapshot
from typing import Literal

router = APIRouter(prefix='/select', tags=['select'])

@router.get("/songs", description='不要一次查太多')
async def songs_detail(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):    
    return await get_songs_detail(page, page_size, session)

@router.get("/artist_songs")
async def artist_songs(
    artist_type: str = Query(),
    artist_id: int = Query(),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    return await get_artist_songs(artist_type, artist_id, page, page_size, session)

@router.get("/ranking")
async def ranking(
    board: str = Query("vocaloid-daily"),
    part: str = Query("main"),
    issue: int | None = Query(default=None, ge=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    order_type: Literal['score','view','favorite','coin','like'] = Query(default='score'),
    seperate: bool = Query(False),
    session: AsyncSession = Depends(get_async_session)
):
    return await get_ranking(board, part, issue, page, page_size, order_type, seperate, session)
    
@router.get('/ranking/top5')
async def ranking_top5(
    board: str = Query("vocaloid-daily"),
    part: str = Query("main"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    return await get_ranking_top5(board, part, page, page_size, session)
    
    
@router.get('/latest_ranking')
async def latest_ranking(
    board: str = Query("vocaloid-daily"),
    session: AsyncSession = Depends(get_async_session)
):
    return await get_latest_ranking(board, session)
    
@router.get("/song")
async def song(
    id: int = Query(),
    session: AsyncSession = Depends(get_async_session)
):    
    return await get_song(id, session)

@router.get("/song/ranking")
async def song_ranking(
    id: int = Query(),
    board: str = Query("vocaloid-daily"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    return await get_song_ranking(id, board, page, page_size, session)

@router.get("/song/by_achievement")
async def song_by_achievement(
    item: Literal['view', 'favorite', 'coin', 'like'] = Query(...),
    level: int = Query(1, ge=1, le=4),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    return await get_song_by_achievement(item, level, page, page_size, session)

@router.get("/song/by_artist")
async def song_by_artist(
    type: Literal['vocalist', 'producer', 'synthesizer', 'uploader'] = Query(...),
    id: int = Query(),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    return await get_artist_songs(type, id, page, page_size, session)

@router.get("/artist")
async def artist(
    type: Literal['vocalist', 'producer', 'synthesizer', 'uploader'] = Query(...),
    id: int = Query(),
    session: AsyncSession = Depends(get_async_session)
):
    return await get_artist(type, id, session)
    

@router.get("/video/snapshot")
async def song_snapshot(
    bvid: str = Query(),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    return await get_song_snapshot(bvid, page, page_size, session)


@router.get("/video/snapshot/by_date")
async def video_snapshot_by_date(
    bvid: str = Query(),
    start_date: str = Query("2025-10-20"),
    end_date: str = Query("2025-10-24"),
    session: AsyncSession = Depends(get_async_session)
):
    return await get_video_snapshot_by_date(bvid, start_date, end_date, session)