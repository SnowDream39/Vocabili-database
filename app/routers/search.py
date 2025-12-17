from fastapi import APIRouter, Query, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.session import get_async_session
from app.crud.search import normal_search
from typing import Literal

router = APIRouter(prefix='/search', tags=['search'])

@router.get("/{type}")
async def search(
    type: Literal['song', 'video', 'producer', 'vocalist', 'synthesizer', 'uploader'],
    keyword: str = Query(...),
    includeEmpty: bool = Query(False),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    session: AsyncSession = Depends(get_async_session)
):
    return await normal_search(type, keyword, includeEmpty, page, page_size, session)
    
