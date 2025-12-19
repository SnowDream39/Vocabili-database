from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text, distinct, and_
from sqlalchemy.orm import selectinload, aliased
from sqlalchemy.dialects.postgresql import aggregate_order_by, array_agg

from app.session import get_async_session, engine
from app.models import Song, song_producer, song_synthesizer, song_vocalist, Producer, Synthesizer, Vocalist, Uploader, Video, Ranking, Snapshot, TABLE_MAP, REL_MAP, song_load_full

from app.utils.misc import make_artist_str
from app.utils.bilibili_id import bv2av
from app.utils.date import get_last_census_date, get_seperate_start_end_issues
from datetime import datetime

from typing import Literal
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
    total_result = await session.execute(select(func.count()).select_from(Song))
    total = total_result.scalar_one()  # 获取总数

    stmt = (
        select(Song)
        .options(*song_load_full)
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await session.execute(stmt)
    data = result.scalars().all()
    
    return {
        'data': data,
        'total': total
    }

async def get_all_included_songs(session: AsyncSession):

    # 最新日期
    latest_date_stmt = select(func.max(Snapshot.date))
    latest_date = (await session.execute(latest_date_stmt)).scalar_one()
    last_census_date = get_last_census_date(latest_date)

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

            # —— ⭐ 分别聚合两个日期的视图 —— #
            func.max(Snapshot.view)
                .filter(Snapshot.date == latest_date)
                .label("latest_view"),

            func.max(Snapshot.view)
                .filter(Snapshot.date == last_census_date)
                .label("census_view"),

            # 多对多聚合
            array_agg(distinct(Producer.name)).filter(Producer.name.isnot(None)).label("producers"),
            array_agg(distinct(Synthesizer.name)).filter(Synthesizer.name.isnot(None)).label("synthesizers"),
            array_agg(distinct(Vocalist.name)).filter(Vocalist.name.isnot(None)).label("vocalists"),
            
            Video.streak,
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

        .where(Snapshot.date.in_([latest_date, last_census_date]))
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
            Video.streak
        )
        .order_by(text("census_view DESC"))
    )

    rows = (await session.execute(stmt)).all()

    records = []
    for (
        title, bvid, pubdate, copyright,
        thumbnail, song_name, song_type, song_display_name,
        uploader_name, latest_view, census_view,
        producers, synthesizers, vocalists, streak,
    ) in rows:

        records.append({
            "title": title,
            "bvid": bvid,
            "aid": str(bv2av(bvid)),
            "name": song_name,
            "display_name": song_display_name,
            "view": latest_view or census_view,
            "pubdate": pubdate.strftime("%Y-%m-%d %H:%M:%S"),
            "author": '、'.join(producers or []),
            "uploader": uploader_name,
            "copyright": copyright,
            "synthesizer": '、'.join(synthesizers or []),
            "vocal": '、'.join(vocalists or []),
            "type": song_type,
            "image_url": thumbnail,
            "streak": streak,
        })

    return records

async def get_artist_songs(
    artist_type: str,
    artist_id: int,
    page: int,
    page_size: int,
    session: AsyncSession
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
            .offset((page - 1) * page_size)
            .limit(page_size)
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
        'data': data,
        'total': total
    }
    
async def get_ranking(
    board: str,
    part: str,
    issue: int | None,
    page: int,
    page_size: int ,
    order_type: Literal['score','view','favorite','coin','like'] ,
    seperate: bool,
    session: AsyncSession 
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
    
    seperate_board_map = {
        'vocaloid-weekly': 'vocaloid-daily',
        'vocaloid-monthly': 'vocaloid-weekly',
    }

    prev_issue = issue - 1
    PrevRanking = aliased(Ranking)
    
    if seperate:
        SeperateRanking = aliased(Ranking)
        seperate_start_issue, seperate_end_issue = get_seperate_start_end_issues(board, issue)
        
        stmt = (
            select(
                Ranking, 
                PrevRanking,
                array_agg(aggregate_order_by(SeperateRanking.rank, SeperateRanking.issue))
            )
            .join(Song, Ranking.song_id == Song.id)
            .join(Video, Ranking.bvid == Video.bvid)
            .outerjoin(PrevRanking, and_(
                PrevRanking.song_id == Ranking.song_id,
                PrevRanking.board == board,
                PrevRanking.part == part,
                PrevRanking.issue == prev_issue
            ))
            .outerjoin(SeperateRanking, and_(
                SeperateRanking.song_id == Ranking.song_id,
                SeperateRanking.board == seperate_board_map[board],
                SeperateRanking.part == part,
                SeperateRanking.issue >= seperate_start_issue,
                SeperateRanking.issue <= seperate_end_issue,
            ))
            .options(
                selectinload(Ranking.song).selectinload(Song.vocalists),
                selectinload(Ranking.song).selectinload(Song.producers),
                selectinload(Ranking.song).selectinload(Song.synthesizers),
                selectinload(Ranking.video).selectinload(Video.uploader)
            )
            .where(Ranking.board == board, Ranking.part == part, Ranking.issue == issue)
            .group_by(Ranking.id, PrevRanking.id)
            .order_by(order_map[order_type])
            .offset((page-1) * page_size)
            .limit(page_size)
        )
        result = await session.execute(stmt)
        rows = result.all()  # 每行是 (cur_ranking, prev_ranking_or_None)

        # 4) 把 prev_ranking 作为 runtime attribute 绑定到 cur_ranking.last 上
        data = []
        for cur, prev, seperates in rows:
            # 动态添加属性（只在内存中），不会影响 DB/ORM 配置
            setattr(cur, "last", prev)
            setattr(cur, "seperates", seperates)
            data.append(cur)
    else:
         
        stmt = (
            select(
                Ranking, 
                PrevRanking
            )
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
            .group_by(Ranking.id, PrevRanking.id)
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
 
async def get_latest_ranking(
    board: str,
    session: AsyncSession
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
    
async def get_ranking_top5(
    board: str,
    part: str,
    page: int,
    page_size: int,
    session: AsyncSession
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
        'data': data,
        'total': total
    }

async def get_song(
    id: int,
    session: AsyncSession
):    
    stmt = (
        select(Song)
        .options(*song_load_full)
        .where(Song.id == id)
    )
    result = await session.execute(stmt)
    data = result.scalars().one()
    return {
        'data': data
    }
    

async def get_song_ranking(
    id: int,
    board: str,
    page: int,
    page_size: int,
    session: AsyncSession
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
        'data': data,
        'total': total
    }

async def get_song_by_achievement(
    item: Literal['view', 'favorite', 'coin', 'like'],
    level: int,
    page: int,
    page_size: int,
    session: AsyncSession
):
    bottom = 10 ** (level + 3)
    top = 10 ** (level + 4)
    
    item_attr = getattr(Snapshot, item)
    
    subq = (
        select(
            Snapshot.bvid.label("bvid"),
            func.max(Snapshot.date).label("latest")
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
                subq.c.latest == Snapshot.date,
            ))
            .where(
                getattr(Snapshot, item) >= bottom,
                getattr(Snapshot, item) < top,
            )
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
        'data': resp,
        'total': total
    }

async def get_song_by_artist(
    type: Literal['vocalist', 'producer', 'synthesizer', 'uploader'],
    id: int,
    page: int,
    page_size: int,
    session: AsyncSession
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
            'data': songs.scalars().all(),
            'total': total
        }
    
async def get_artist(
    type: Literal['vocalist', 'producer', 'synthesizer', 'uploader'],
    id: int,
    session: AsyncSession
):
    table = TABLE_MAP[type]
    stmt = (
        select(table)
        .where(table.id == id)
    )
    result = await session.execute(stmt)
    data = result.scalars().one()
    return {
        'data': data
    }
    

async def get_song_snapshot(
    bvid: str,
    page: int,
    page_size: int,
    session: AsyncSession
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
        'data': data,
        'total': total
    }

async def get_video(
    bvid: str,
    session: AsyncSession
):    
    stmt = (
        select(Video)
        .where(Video.bvid == bvid)
    )
    result = await session.execute(stmt)
    data = result.scalars().one()
    return {
        'data': data
    }

async def get_video_snapshot_by_date(
    bvid: str,
    start_date: str,
    end_date: str,
    session: AsyncSession
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
        'data': data
    }
    
    



