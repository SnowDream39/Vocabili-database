from fastapi import APIRouter, Depends, Query, Body, HTTPException

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from sqlalchemy.orm import selectinload, aliased

from app.session import get_async_session, engine
from app.models import Song, Producer, Synthesizer, Vocalist, Uploader, Video, song_producer, song_synthesizer, song_vocalist, Ranking, Snapshot

from datetime import datetime

TABLE_MAP = {
    'producer': Producer,
    'synthesizer': Synthesizer,
    'vocalist': Vocalist,
    'uploader': Uploader
}

REL_MAP = {
    'producer': song_producer,
    'synthesizer': song_synthesizer,
    'vocalist': song_vocalist,
}


router = APIRouter(prefix='/select', tags=['select'])

@router.get("/songs", description='开销极大，慎用')
async def songs_detail(
    page: int = Query(1, ge=1),
    page_size: int = Query(1, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    total_result = await session.execute(select(func.count()).select_from(Song))
    total = total_result.scalar_one()  # 获取总数

    stmt = select(Song).options(selectinload(Song.videos)).offset((page - 1) * page_size).limit(page_size)
    result = await session.execute(stmt)
    data = result.scalars().all()
    return {
        'status': 'ok',
        'data': data,
        'total': total
    }

@router.get("/artist_songs")
async def artist_songs(
    artist_type: str = Query(),
    artist_id: int = Query(),
    page: int = Query(1, ge=1),
    page_size: int = Query(1, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    table = TABLE_MAP[artist_type]
    if table in [Producer, Synthesizer, Vocalist]:
        rel = REL_MAP[artist_type]
        stmt = (
            select(Song)
            .options(selectinload(Song.videos).selectinload(Video.uploader))
            .join(rel, Song.id == rel.c.song_id)
            .where(rel.c.artist_id == artist_id)
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    elif table == Uploader:
        stmt = (
            select(Song)
            .join(Song.videos)                    # 先 join video
            .where(Video.uploader_id == artist_id)  # 筛选条件
            .options(selectinload(Song.videos).selectinload(Video.uploader))
            .where()
        )
    else:
        raise Exception('artist类型不符合条件')
        
    result = await session.execute(stmt)
    data = result.scalars().all()
    return {
        'status': 'ok',
        'data': data
    }

@router.get("/ranking")
async def ranking(
    board: str = Query("vocaloid-daily"),
    part: str = Query("main"),
    issue: int = Query(1, ge=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(1, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    prev_issue = issue - 1
    lr_cte = (
        select(
            Ranking.id.label("last_id"),
            Video.song_id.label("song_id")
        )
        .join(Video, Ranking.bvid == Video.bvid)
        .where(
            Ranking.board == board,
            Ranking.part == part,
            Ranking.issue == prev_issue
        )
        .cte("lr")
    )
    PrevRanking = aliased(Ranking)
    stmt = (
        select(Ranking, PrevRanking)
        .join(Video, Ranking.bvid == Video.bvid)
        .outerjoin(lr_cte, Video.song_id == lr_cte.c.song_id)
        .outerjoin(PrevRanking, PrevRanking.id == lr_cte.c.last_id)
        .options(
            selectinload(Ranking.video).selectinload(Video.song), 
            selectinload(Ranking.video).selectinload(Video.uploader)
        )
        .where(Ranking.board == board, Ranking.part == part, Ranking.issue == issue)
        .order_by(Ranking.rank)
        .offset((page-1) * page_size)
        .limit(page_size)
    )
    result = await session.execute(stmt)
    rows = result.all()  # 每行是 (cur_ranking, prev_ranking_or_None)

    # 4) 把 prev_ranking 作为 runtime attribute 绑定到 cur_ranking.last 上
    data = []
    for cur, prev in rows:
        # 动态添加属性（只在内存中），不会影响 DB/ORM 配置
        setattr(cur, "last", prev)
        data.append(cur)

    return {
        'status': 'ok',
        'data': data
    }
    

@router.get("/snapshot/by_date")
async def snapshot(
    bvid: str = Query(),
    start_date: str = Query("2025-10-20"),
    end_date: str = Query("2025-10-24"),
    session: AsyncSession = Depends(get_async_session)
):
    start_date_ = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_ = datetime.strptime(end_date, "%Y-%m-%d")
    
    stmt = (
        select(Snapshot)
        .where(and_(
            Snapshot.bvid == bvid,
            Snapshot.date >= start_date_,
            Snapshot.date <= end_date_
        ))
        .order_by(Snapshot.date.desc())
    )
    
    result = await session.execute(stmt)
    data = result.scalars().all()

    return {
        'status': 'ok',
        'data': data
    }