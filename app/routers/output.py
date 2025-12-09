"""
导出xlsx文件
"""

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.session import get_async_session
from app.models import Song
from app.crud.select import get_songs_detail, get_all_included_songs
from app.utils.misc import make_artist_str
import pandas as pd
import os
import tempfile


router = APIRouter(prefix='/output', tags=['output'])

# =========== 小工具函数  ===========




@router.get('/songs')
async def export_songs(
    session: AsyncSession = Depends(get_async_session)
):
    records = await get_all_included_songs(session=session)
    

            
    # 创建临时文件
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
    temp_file.close()
    
    # 保存DataFrame到临时文件
    pd.DataFrame(records).to_excel(temp_file.name, index=False)
    
    # 返回文件响应，提供下载
    return FileResponse(
        path=temp_file.name,
        filename="songs.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=songs.xlsx"}
    )