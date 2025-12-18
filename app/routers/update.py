from fastapi import APIRouter, Depends, Query 
from fastapi.responses import StreamingResponse

from sqlalchemy.ext.asyncio import AsyncSession

from app.session import get_async_session
from app.models import Song, Producer

from ..utils import validate_excel, read_excel
from ..utils.filename import generate_board_file_path
from ..utils.cache import Cache
from ..crud.insert import execute_import_rankings, execute_import_snapshots

import pandas as pd
from datetime import datetime, timedelta


router = APIRouter(prefix='/update', tags=['update'])

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
    await execute_import_snapshots(session, date, not old)
        

@router.get('/batch_snapshots')
async def batch_import_snapshots(
    start_date: str = Query(),
    end_date: str = Query(),
    session: AsyncSession = Depends(get_async_session)
):
    cache = Cache()
    
    start_date_ = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_ = datetime.strptime(end_date, "%Y-%m-%d")
    date = start_date_
    while date <= end_date_:
        print(f'正在处理：{date.strftime("%Y-%m-%d")}')
        await execute_import_snapshots(session, date.strftime("%Y-%m-%d"), False, cache)


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
    
    return StreamingResponse(
        execute_import_rankings(session, board, part, issue, strict, cache),
        media_type='text/event-stream'
    )
    



@router.get('/check_ranking')
async def check_ranking(
    board: str = Query(),
    part: str = Query('main'),
    issue: int = Query()
):
    df: pd.DataFrame = read_excel(
        generate_board_file_path(board, part, issue),
    ).assign( board = board, part = part, issue = issue)
    
    errors = validate_excel(df)
    return {
        'detail': '\n'.join(errors)
    }
    
    

@router.get('/batch_ranking')
async def batch_import_ranking(
    board: str = Query(),
    part: str = Query('main'),
    start_issue: int = Query(),
    end_issue: int = Query(),
    session: AsyncSession = Depends(get_async_session)
):
    cache = Cache()
    for issue in range(start_issue, end_issue+1):
        print(f'正在处理：{issue}期')
        async for s in execute_import_rankings(session, board, part, issue, False, cache):
            print(s)
