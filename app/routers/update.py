from fastapi import APIRouter, Depends, Query, Body, HTTPException

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from app.session import get_async_session
from app.models import Song, Producer, Synthesizer, Vocalist, Uploader, Video, song_producer, song_synthesizer, song_vocalist, Snapshot, Ranking

from ..utils import validate_excel, read_excel
from ..utils.filename import generate_board_file_path
from ..utils.cache import Cache
from ..crud.insert import execute_import_rankings, execute_import_snapshots, execute_import_songs

import pandas as pd
from datetime import datetime, timedelta


router = APIRouter(prefix='/update', tags=['update'])

@router.get("/test")
async def test_insert(session: AsyncSession = Depends(get_async_session)):
    # 1️⃣ 查询前 5 条 producer，获取 id
    result = await session.execute(select(Producer.id).limit(5))
    producer_ids = [row[0] for row in result.all()]
    print("Producer IDs:", producer_ids)

    # 2️⃣ 插入 song
    song_name = "测试歌曲"
    song_type = 1  # 注意类型需匹配数据库定义
    result = await session.execute(
        pg_insert(Song)
        .values(name=song_name, type=song_type)
        .returning(Song.id)
    )
    song_id = result.scalar_one()
    print("Inserted song.id:", song_id)

    # 4️⃣ 提交事务
    await session.commit()
    return {"status": "ok", "song_id": song_id, "producer_ids": producer_ids}


@router.get('/songs')
async def import_songs(session: AsyncSession = Depends(get_async_session)):
    df = read_excel('./收录曲目.xlsx')
    await execute_import_songs(session, df, False)        

@router.get('/snapshots')
async def import_snapshots(
    date: str = Query(description="格式类似'2025-10-28'"),
    old: bool = Query(False),
    session: AsyncSession = Depends(get_async_session)
):
    """
    插入数据库记录。
    规定，在插入排名记录之后执行。
    除了更新数据记录之外，最多只会插入新视频。
    """
    try:
        await execute_import_snapshots(session, date, not old)
        await session.commit()

    except IntegrityError as e:
        await session.rollback()
        print("❌ 插入数据出错:", e)
        

@router.get('/batch_snapshots')
async def batch_import_snapshots(
    start_date: str = Query(),
    end_date: str = Query(),
    session: AsyncSession = Depends(get_async_session)
):
    cache = Cache()
    
    start_date_ = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_ = datetime.strptime(end_date, "%Y-%m-%d")
    try:
        date = start_date_
        while date <= end_date_:
            print(f'正在处理：{date.strftime("%Y-%m-%d")}')
            await execute_import_snapshots(session, date.strftime("%Y-%m-%d"), False, cache)
            date += timedelta(1)

        await session.commit()
    except IntegrityError as e:

        await session.rollback()
        print("❌ 插入数据出错:", e)

@router.get('/ranking')
async def import_rankings(
    board: str = Query(),
    part: str = Query('main'),
    issue: int = Query(),
    old: bool = Query(False),
    session: AsyncSession = Depends(get_async_session),
):
    """
    插入排名记录。会同时更新曲目。
    """
    strict = not old
    df: pd.DataFrame = read_excel(
        generate_board_file_path(board, part, issue),
    ).assign( board = board, part = part, issue = issue)

    if strict:
        validate_excel(df)
    
    cache = Cache()
    
    try:
        await execute_import_songs(session, df, strict, cache)
        await execute_import_rankings(session, board, part, issue, strict, cache)

        await session.commit()

    except IntegrityError as e:
        await session.rollback()
        print("❌ 插入数据出错:", e)

@router.get('/batch_ranking')
async def batch_import_ranking(
    board: str = Query(),
    part: str = Query('main'),
    start_issue: int = Query(),
    end_issue: int = Query(),
    session: AsyncSession = Depends(get_async_session)
):
    cache = Cache()
    try:
        for issue in range(start_issue, end_issue+1):
            print(f'正在处理：{issue}期')
            await execute_import_rankings(session, board, part, issue, False, cache)

        await session.commit()
    except IntegrityError as e:

        await session.rollback()
        print("❌ 插入数据出错:", e)