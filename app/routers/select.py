from fastapi import APIRouter, Depends, Query

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

@router.get("/songs", description='不要一次查太多')
async def songs_detail(
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
            selectinload(Song.videos)
        )
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
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
            .options(
                selectinload(Song.producers),
                selectinload(Song.synthesizers),
                selectinload(Song.vocalists),
                selectinload(Song.videos).selectinload(Video.uploader)
            )
            .join(rel, Song.id == rel.c.song_id)
            .where(rel.c.artist_id == artist_id)
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        total_result = await session.execute(
            select(func.count())
            .select_from(Song)
            .join(rel, Song.id == rel.c.song_id)
            .where(rel.c.artist_id == artist_id)
        )
        total = total_result.scalar_one()
    elif table == Uploader:
        stmt = (
            select(Song)
            .join(Song.videos)                    # 先 join video
            .where(Video.uploader_id == artist_id)  # 筛选条件
            .options(
                selectinload(Song.producers),
                selectinload(Song.synthesizers),
                selectinload(Song.vocalists),
                selectinload(Song.videos).selectinload(Video.uploader)
            )
            .where()
        )
        total_result = await session.execute(
            select(func.count())
            .select_from(Song)
            .join(Song.videos)
            .where(Video.uploader_id == artist_id)
        )
        total = total_result.scalar_one()
    else:
        raise Exception('artist类型不符合条件')
        
    result = await session.execute(stmt)
    data = result.scalars().all()
    return {
        'status': 'ok',
        'data': data,
        'total': total
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
    PrevRanking = aliased(Ranking)
    stmt = (
        select(Ranking, PrevRanking)
        .join(Song, Ranking.song_id == Song.id)
        .join(Video, Ranking.bvid == Video.bvid)
        .outerjoin(PrevRanking, and_(
            PrevRanking.song_id == Ranking.song_id,
            PrevRanking.issue == prev_issue
        ))
        .options(
            selectinload(Ranking.song).selectinload(Song.vocalists),
            selectinload(Ranking.song).selectinload(Song.producers),
            selectinload(Ranking.song).selectinload(Song.synthesizers),
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
        
    # 5) 查询本期排行的总量
    
    total_result = await session.execute(
        select(func.count())
        .select_from(Ranking)
        .where(Ranking.board == board, Ranking.part == part, Ranking.issue == issue)
    )
    total = total_result.scalar_one()

    return {
        'status': 'ok',
        'data': data,
        'total': total
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