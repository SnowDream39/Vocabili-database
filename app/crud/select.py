from fastapi import APIRouter, Depends, Query

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from sqlalchemy.orm import selectinload, aliased

from app.session import get_async_session, engine
from app.models import Song, Producer, Synthesizer, Vocalist, Uploader, Video, Ranking, Snapshot, TABLE_MAP, REL_MAP

from datetime import datetime


async def get_songs_detail(
    page: int = Query(1, ge=1),
    page_size: int = Query(1, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    total_result = await session.execute(select(func.count()).select_from(Song))
    total = total_result.scalar_one()  # 获取总数

    stmt = (
        select(Song)
        .options(
            selectinload(Song.producers),
            selectinload(Song.synthesizers),
            selectinload(Song.vocalists),
            selectinload(Song.videos).selectinload(Video.uploader)
        )
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await session.execute(stmt)
    data = result.scalars().all()
    return data
