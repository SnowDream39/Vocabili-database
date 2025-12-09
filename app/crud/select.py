from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from sqlalchemy.orm import selectinload, aliased
from sqlalchemy.dialects.postgresql import array_agg

from app.session import get_async_session, engine
from app.models import Song, song_producer, song_synthesizer, song_vocalist, Producer, Synthesizer, Vocalist, Uploader, Video, Ranking, Snapshot, TABLE_MAP, REL_MAP, song_load_full

from app.utils.misc import make_artist_str
from app.utils.bilibili_id import bv2av
from datetime import datetime

from abv_py import bv2av

async def get_names(
    type: str,
    session: AsyncSession
):
    table = TABLE_MAP[type]
    if not table:
        raise ValueError("Invalid type")
    elif table == Video:
        result = await session.execute(select(Video.title))
    else:
        result = await session.execute(select(table.name))

    names = result.scalars().all()
    return names


async def get_songs_detail(
    page: int,
    page_size: int,
    session: AsyncSession
):
    stmt = (
        select(Song)
        .options(*song_load_full)
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await session.execute(stmt)
    data = result.scalars().all()
    return data

async def get_all_included_songs(session: AsyncSession):

    # 最新日期
    latest_date_stmt = select(func.max(Snapshot.date))
    latest_date = (await session.execute(latest_date_stmt)).scalar_one()

    # 聚合查询
    stmt = (
        select(
            Video.title,
            Video.bvid,
            Video.pubdate,
            Video.copyright,
            Video.thumbnail,

            Song.name.label("song_name"),
            Song.type.label("song_type"),
            Song.display_name.label("song_display_name"),

            Uploader.name.label("uploader_name"),

            Snapshot.view,

            # 多对多聚合
            array_agg(Producer.name).filter(Producer.name.isnot(None)).label("producers"),
            array_agg(Synthesizer.name).filter(Synthesizer.name.isnot(None)).label("synthesizers"),
            array_agg(Vocalist.name).filter(Vocalist.name.isnot(None)).label("vocalists"),
        )
        .select_from(Snapshot)
        .join(Video, Snapshot.bvid == Video.bvid)
        .join(Song, Video.song_id == Song.id)
        .join(Uploader, Video.uploader_id == Uploader.id)

        # 多对多 LEFT JOIN
        .join(song_producer, song_producer.c.song_id == Song.id, isouter=True)
        .join(Producer, song_producer.c.artist_id == Producer.id, isouter=True)

        .join(song_synthesizer, song_synthesizer.c.song_id == Song.id, isouter=True)
        .join(Synthesizer, song_synthesizer.c.artist_id == Synthesizer.id, isouter=True)

        .join(song_vocalist, song_vocalist.c.song_id == Song.id, isouter=True)
        .join(Vocalist, song_vocalist.c.artist_id == Vocalist.id, isouter=True)

        .where(Snapshot.date == latest_date)
        .group_by(
            Video.title,
            Video.bvid,
            Video.pubdate,
            Video.thumbnail,
            Video.copyright,
            Song.name,
            Song.type,
            Song.display_name,
            Uploader.name,
            Snapshot.view,
        )
        .order_by(Snapshot.view.desc())
    )

    rows = (await session.execute(stmt)).all()

    records = []
    for (
        title, bvid, pubdate, copyright,
        thumbnail, song_name, song_type, song_display_name,
        uploader_name, view,
        producers, synthesizers, vocalists
    ) in rows:

        records.append({
            "title": title,
            "bvid": bvid,
            "aid": str(bv2av(bvid)),
            "name": song_name,
            "display_name": song_display_name,
            "view": view,
            "pubdate": pubdate.strftime("%Y-%m-%d %H:%M:%S"),
            "author": '、'.join(producers or []),
            "uploader": uploader_name,
            "copyright": copyright,
            "synthesizer": '、'.join(synthesizers or []),
            "vocal": '、'.join(vocalists or []),
            "type": song_type,
            "image_url": thumbnail,
        })

    return records