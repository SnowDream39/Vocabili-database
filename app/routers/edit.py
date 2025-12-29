from fastapi import APIRouter, Body, Depends, HTTPException

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import TABLE_MAP, REL_MAP, Video, Song, Video
from app.session import get_async_session
from app.crud.edit import check_artist
from app.schemas.edit import ConfirmRequest, SongEdit, VideoEdit
from app.utils.task import task_manager

router = APIRouter(prefix='/edit', tags=['edit'])

@router.post("/artist/check")
async def edit_artist(
    type: str = Body(),
    id: int = Body(),
    name: str = Body(),
    session: AsyncSession = Depends(get_async_session)
):
    return await check_artist(type, id, name, session)

@router.post("/artist/confirm")
async def confirm_edit_artist(
    request: ConfirmRequest = Body(),
):
    token = request.task_id
    
    task = task_manager.get_task(token)
    
    if not task:
        return HTTPException(status_code=404, detail="任务不存在")
    
    return await task

@router.post("/song")
async def edit_song(
    song: SongEdit = Body(),
    session: AsyncSession = Depends(get_async_session)
):
    stmt = select(Song).where(Song.name == song.name)
    result = await session.execute(stmt)
    exist_song = result.scalar_one_or_none()
    if exist_song and exist_song.id != song.id: 
        raise HTTPException(status_code=400, detail=f'名称"{song.name}"已存在')
    
    stmt = (
        update(Song)
        .where(Song.id == song.id)
        .values(
            name=song.name,
            type=song.type,
            vocadb_id=song.vocadb_id,
            display_name=song.display_name,
        )
    )
    
    await session.execute(stmt)
    await session.commit()

@router.post("/video")
async def edit_video(
    video: VideoEdit = Body(),
    session: AsyncSession = Depends(get_async_session)
):
    stmt = (
        update(Video)
        .where(Video.bvid == video.bvid)
        .values(
            title=video.title,
            copyright=video.copyright,
        )
    )
    
    await session.execute(stmt)
    await session.commit()
