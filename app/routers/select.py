from fastapi import APIRouter, Depends, Query

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from sqlalchemy.orm import selectinload, aliased

from app.session import get_async_session
from app.models import Song, Producer, Synthesizer, Vocalist, Uploader, Video, Ranking, Snapshot, TABLE_MAP, REL_MAP, song_load_full
from app.crud.select import get_songs_detail
from datetime import datetime
from typing import Literal

router = APIRouter(prefix='/select', tags=['select'])

@router.get("/songs", description='不要一次查太多')
async def songs_detail(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    total_result = await session.execute(select(func.count()).select_from(Song))
    total = total_result.scalar_one()  # 获取总数

    
    data = await get_songs_detail(page, page_size, session)
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
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    table = TABLE_MAP[artist_type]
    if table in [Producer, Synthesizer, Vocalist]:
        rel = REL_MAP[artist_type]
        stmt = (
            select(Song)
            .options(*song_load_full)
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
            .options(*song_load_full)
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
    issue: int | None = Query(default=None, ge=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    order_type: Literal['score','view','favorite','coin','like'] = Query(default='score'),
    session: AsyncSession = Depends(get_async_session)
):
    if (issue == None):
        result = await session.execute(
            select(func.max(Ranking.issue))
            .where(Ranking.board == board, Ranking.part == part)
        )
        issue = int(result.scalar_one())
        
    order_map = {
        'score': Ranking.rank,
        'view': Ranking.view_rank,
        'favorite': Ranking.favorite_rank,
        'coin': Ranking.coin_rank,
        'like': Ranking.like_rank
    }
    
    prev_issue = issue - 1
    PrevRanking = aliased(Ranking)
    stmt = (
        select(Ranking, PrevRanking)
        .join(Song, Ranking.song_id == Song.id)
        .join(Video, Ranking.bvid == Video.bvid)
        .outerjoin(PrevRanking, and_(
            PrevRanking.song_id == Ranking.song_id,
            PrevRanking.board == board,
            PrevRanking.part == part,
            PrevRanking.issue == prev_issue
        ))
        .options(
            selectinload(Ranking.song).selectinload(Song.vocalists),
            selectinload(Ranking.song).selectinload(Song.producers),
            selectinload(Ranking.song).selectinload(Song.synthesizers),
            selectinload(Ranking.video).selectinload(Video.uploader)
        )
        .where(Ranking.board == board, Ranking.part == part, Ranking.issue == issue)
        .order_by(order_map[order_type])
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
    
@router.get('/ranking/top5')
async def ranking_top5(
    board: str = Query("vocaloid-daily"),
    part: str = Query("main"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):

    
    stmt = (
        select(Ranking)
        .where(
            Ranking.rank <= 5,
            Ranking.board == board,
            Ranking.part == part
        )
        .options(
            selectinload(Ranking.song).selectinload(Song.vocalists),
            selectinload(Ranking.song).selectinload(Song.producers),
            selectinload(Ranking.song).selectinload(Song.synthesizers),
            selectinload(Ranking.video).selectinload(Video.uploader)
        )
        .order_by(Ranking.issue.desc(), Ranking.rank)
        .offset((page-1) * page_size * 5)
        .limit(page_size*5)
    )
    result = await session.execute(stmt)
    original_data = result.scalars().all()
    
    data = []
    length = len(original_data)
    i = 0
    while i*5 < length:
        data.append({
            'issue': original_data[i*5].issue,
            'rankings': []
        })
        for j in range(5):
            data[i]['rankings'].append(original_data[i*5+j])
        i+=1
            

    # 查询排行的总期数
    total_result = await session.execute(
        select(func.count(func.distinct(Ranking.issue)))
        .where(Ranking.board == board, Ranking.part == part)
    )
    total = total_result.scalar_one()

    return {
        'status': 'ok',
        'data': data,
        'total': total
    }
    
    
@router.get('/latest_ranking')
async def latest_ranking(
    board: str = Query("vocaloid-daily"),
    session: AsyncSession = Depends(get_async_session)
):
    stmt = (
        select(Ranking.issue)
        .select_from(Ranking)
        .where(Ranking.board == board)
        .order_by(Ranking.issue.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    issue = int(result.scalars().one())
    return issue
    
@router.get("/song")
async def get_song(
    id: int = Query(),
    session: AsyncSession = Depends(get_async_session)
):    
    stmt = (
        select(Song)
        .options(*song_load_full)
        .where(Song.id == id)
    )
    result = await session.execute(stmt)
    data = result.scalars().one()
    return {
        'status': 'ok',
        'data': data
    }
    

@router.get("/song/ranking")
async def song_ranking(
    id: int = Query(),
    board: str = Query("vocaloid-daily"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    stmt = (
        select(Ranking)
        .distinct(Ranking.issue)
        .where(and_(
            Ranking.board == board,
            Ranking.part == "main",
            Ranking.song_id == id
        ))
        .order_by(
            Ranking.issue.desc(),
            Ranking.rank.asc()
        )
        .offset((page-1) * page_size)
        .limit(page_size)
    )
    
    result = await session.execute(stmt)
    data = result.scalars().all()
    
    totalResult = await session.execute(
        select(func.count())
        .where(and_(
            Ranking.board == board,
            Ranking.part == "main",
            Ranking.song_id == id
        ))
    )
    total = totalResult.scalar_one()
    return {
        'status': 'ok',
        'data': data,
        'total': total
    }

@router.get("/song/by_achievement")
async def song_by_achievement(
    item: Literal['view', 'favorite', 'coin', 'like'] = Query(...),
    level: int = Query(1, ge=1, le=4),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    bottom = 10 ** (level + 3)
    top = 10 ** (level + 4)
    
    item_attr = getattr(Snapshot, item)
    
    subq = (
        select(
            Snapshot.bvid.label("bvid"),
            func.max(Snapshot.date).label("latest")
        )
        .where(
            getattr(Snapshot, item) >= bottom,
            getattr(Snapshot, item) < top,
        )
        .group_by(Snapshot.bvid)
        .cte()
    )
    
    stmt = (
        select(Song, Video, Snapshot)
            .select_from(subq)
            .join(Video, Video.bvid == subq.c.bvid)
            .join(Song, Song.id == Video.song_id)
            .join(Snapshot, and_(
                subq.c.bvid == Snapshot.bvid,
                subq.c.latest == Snapshot.date
            ))
            .options(
                selectinload(Song.vocalists),
                selectinload(Song.producers),
                selectinload(Song.synthesizers),
                selectinload(Video.uploader)
            )
            .order_by(getattr(Snapshot, item).desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
    )
    
    
    
    result = await session.execute(stmt)

    resp = []
    for song, video, snapshot in result.all():
        resp.append({
            "song": song,
            "video": video,
            "snapshot": snapshot,
        })
        
    totalResult = await session.execute(
        select(func.count())
        .where(
            getattr(Snapshot, item) >= bottom,
            getattr(Snapshot, item) < top,
        )
    )
    
    total = totalResult.scalar_one()
    
    return {
        'status': 'ok',
        'data': resp,
        'total': total
    }

@router.get("/song/by_artist")
async def song_by_artist(
    type: Literal['vocalist', 'producer', 'synthesizer', 'uploader'] = Query(...),
    id: int = Query(),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    if type == 'uploader':
        rel = Uploader
        stmt = (
            select(Song)
            .select_from(Video)
            .join(Song, Song.id == Video.song_id)
            .where(Video.uploader_id == id)
            .options(*song_load_full)
            .offset((page-1) * page_size)
            .limit(page_size)
        )
        result = await session.execute(stmt)
        data = result.scalars().all()
        
        stmt = (
            select(func.count())
            .select_from(Video)
            .join(Song, Song.id == Video.song_id)
            .where(Video.uploader_id == id)
        )
        result = await session.execute(stmt)
        total = result.scalar_one()
        
        return {
            'status': 'ok',
            'data': data,
            'total': total
        }

    else:
        rel = REL_MAP[type]
        stmt = (
            select(rel)
            .where(rel.c.artist_id == id)
            .offset((page-1) * page_size)
            .limit(page_size)
        )
        songs = await session.execute(stmt) 
        song_ids = [song.song_id for song in songs]
        
        stmt = (
            select(func.count())
            .select_from(rel)
            .where(rel.c.artist_id == id)
        )
        result = await session.execute(stmt)
        total = result.scalar_one()
        
        stmt = (
            select(Song)
            .where(Song.id.in_(song_ids))
            .options(*song_load_full)
        )
        songs = await session.execute(stmt)
        return {
            'status': 'ok',
            'data': songs.scalars().all(),
            'total': total
        }
    
@router.get("/artist")
async def get_artist(
    type: Literal['vocalist', 'producer', 'synthesizer', 'uploader'] = Query(...),
    id: int = Query(),
    session: AsyncSession = Depends(get_async_session)
):
    table = TABLE_MAP[type]
    stmt = (
        select(table)
        .where(table.id == id)
    )
    result = await session.execute(stmt)
    data = result.scalars().one()
    return {
        'status': 'ok',
        'data': data
    }
    

@router.get("/video/snapshot")
async def song_snapshot(
    bvid: str = Query(),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    stmt = (
        select(Snapshot)
        .where(Snapshot.bvid == bvid)
        .order_by(Snapshot.date.desc())
        .offset((page-1) * page_size)
        .limit(page_size)
    )
    result = await session.execute(stmt)
    data = result.scalars().all()
    
    totalResult = await session.execute(
        select(func.count())
        .where(Snapshot.bvid == bvid)
    )
    total = totalResult.scalar_one()
    
    return {
        'status': 'ok',
        'data': data,
        'total': total
    }


@router.get("/video/snapshot/by_date")
async def video_snapshot_by_date(
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
    
    