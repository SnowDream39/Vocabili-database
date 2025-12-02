"""
导出xlsx文件
"""

from fastapi import APIRouter, Depends, HTTPException

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.session import get_async_session
from app.models import Song
from app.crud.select import get_songs_detail
from app.utils.misc import make_artist_str
import pandas as pd


router = APIRouter(prefix='/output', tags=['output'])

# =========== 小工具函数  ===========




@router.get('/songs')
async def export_songs(
    session: AsyncSession = Depends(get_async_session)
):
    data = await get_songs_detail(1, 100000, session=session)
    
    records = []
    for song in data:
        
        for video in song.videos:
            
            records.append({
                'name': song.name,
                'title': video.title,
                'bvid': video.bvid,
                'image_url': video.thumbnail,
                'pubdate': video.pubdate.strftime('%Y-%m-%d %H:%M:%S'),
                'copyright': video.copyright,
                'uploader': video.uploader.name if video.uploader else None,
                'vocal': make_artist_str(song.vocalists),
                'author': make_artist_str(song.producers),
                'synthesizer': make_artist_str(song.synthesizers),
                'type': song.type,
            })
            
    pd.DataFrame(records).to_excel('songs.xlsx', index=False)