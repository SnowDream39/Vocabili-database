
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from app.models import Song, Producer, Synthesizer, Vocalist, Uploader, Video, song_producer, song_synthesizer, song_vocalist, Snapshot, Ranking

from ..utils import validate_excel, read_excel
from ..utils.filename import generate_board_file_path
from ..utils.cache import Cache

import pandas as pd
from datetime import datetime, timedelta



BATCH_SIZE = 200

# =================  比较小的操作，不对外公开  ====================

async def insert_artists(
    session: AsyncSession, 
    df,
    cache: Cache | None = None
    ):
    if not cache:
        cache = Cache()
    await cache.ensure_loaded(session, ['artist_maps'])

    artist_map = {
        Producer: set(),
        Synthesizer: set(),
        Vocalist: set(),
        Uploader: set(),
    }

    for _, row in df.iterrows():
        for table, col in (
            (Producer, 'author'),
            (Synthesizer, 'synthesizer'),
            (Vocalist, 'vocal'),
            (Uploader, 'uploader'),
        ):
            if not pd.isna(row[col]):
                for name in row[col].split('、'):
                    artist_map[table].add(name)

    for table, names in artist_map.items():
        if not names:
            continue
        new_names = names - cache.artist_maps[table].keys()

        if new_names:
            # 构造要插入的数据
            values = [{"name": name} for name in new_names]
            stmt = pg_insert(table).values(values).on_conflict_do_nothing().returning(table.name, table.id)
            result = await session.execute(stmt)
            records = result.all()
            cache.artist_maps[table].update({r[0]: r[1] for r in records})


    await session.flush()

async def insert_songs(session: AsyncSession, df, cache: Cache | None = None):
    if not cache:
        cache = Cache()
    await cache.ensure_loaded(session, ['song_map'])
    song_records = []
    for row in df.to_dict(orient='records'):
        name = row['name']
        song_type = row['type'] if not pd.isna(row['type']) else None
        song_records.append((name, song_type))
        
    song_records = list(set(song_records))
    new_songs = [r for r in song_records if r[0] not in cache.song_map.keys()]
    
    if new_songs:
        excluded = pg_insert(Song).excluded
        stmt = pg_insert(Song).values([{'name': n, 'type': t} for n, t in new_songs]).on_conflict_do_update(
            index_elements=['name'],
            set_={  'type': excluded['type'] }
        ).returning(Song.id, Song.name)
        result = await session.execute(stmt)
        await session.flush()
        rows = result.all()
        
        cache.song_map.update({r[1]: r[0] for r in rows})
            
    return new_songs

async def insert_relations(
    session: AsyncSession,
    df: pd.DataFrame,
    strict: bool,
    new_songs: list,
    cache: Cache | None = None
    ):
    if not cache:
        cache = Cache()
    await cache.ensure_loaded(session, ['song_map', 'artist_maps', 'song_artist_maps'])
       
    new_song_names = list(map(lambda x: x[0], new_songs))
    new_song_df = df.loc[df['name'].isin(new_song_names)][['name', 'synthesizer', 'author', 'vocal']].copy()
    
    # 非严格模式下再检查一遍不需要插入的内容
    if not strict:
        new_song_df = new_song_df.loc[new_song_df['synthesizer'].notna() & new_song_df['author'].notna() & new_song_df['vocal'].notna()]
    
    for cls, table, field in (
        (Producer, song_producer, 'author'),
        (Synthesizer, song_synthesizer, 'synthesizer'),
        (Vocalist, song_vocalist, 'vocal')
    ):
        rel_df = (
            new_song_df[['name', field]]
            .assign( song_id=lambda df: df['name'].map(cache.song_map))
        )
        
        rel_df[field] = rel_df[field].astype(str).str.split('、')
        rel_df = rel_df.explode(field)
        rel_df = rel_df[rel_df[field].notna()]
        
        rel_df['artist_id'] = rel_df[field].map(cache.artist_maps[cls])
        rel_df = rel_df[rel_df['artist_id'].notna()]
        
        rel_records = set(rel_df[['song_id', 'artist_id']].itertuples(index=False, name=None))
        existing_rel_set = set(tuple(x) for x in cache.song_artist_maps[cls])
        new_rel_records = list(rel_records - existing_rel_set)

        if new_rel_records:
            new_rel_dicts = [{'song_id': t[0], 'artist_id': t[1]} for t in new_rel_records]
            stmt = pg_insert(table).values(new_rel_dicts).on_conflict_do_nothing()
            await session.execute(stmt)
            cache.song_artist_maps[cls].update(new_rel_records)

async def insert_videos(
    session: AsyncSession, 
    df: pd.DataFrame, 
    cache: Cache | None = None
    ):
    if not cache:
        cache = Cache()
    """
    插入视频。冲突更新。
    """

    await cache.ensure_loaded(session, ['video_map', 'song_map', 'artist_maps'])
        
    if not df.empty:
        missing_video_df: pd.DataFrame = (
            df.loc[~df['bvid'].isin(cache.video_map.keys())]
            .assign(
                song_id= lambda df: df['name'].map(cache.song_map),
                uploader_id=lambda df: df['uploader'].map(cache.artist_maps[Uploader])
            )
            .rename(columns={'image_url': 'thumbnail'})[['bvid', 'title', 'pubdate', 'song_id', 'uploader_id', 'copyright', 'thumbnail']]
        )
        
        # 允许 uploader 为空，只是需要防止出现 NaN
        missing_video_df["uploader_id"] = missing_video_df["uploader_id"].astype("Int64")
        # 因为有莫名其妙缺copyright的情况，可能是以前，唉随便了
        missing_video_df["copyright"] = missing_video_df["copyright"].astype("Int64")
        missing_video_df = missing_video_df.replace({pd.NA: None})

        if (len(missing_video_df) > 0):
            missing_video_records = missing_video_df.to_dict(orient='records')
            excluded = pg_insert(Video).excluded
            stmt = pg_insert(Video).values(missing_video_records).on_conflict_do_update(
                index_elements=['bvid'],
                set_={field: excluded[field] for field in [
                    'title', 'pubdate', 'uploader_id', 'song_id', 'copyright', 'thumbnail'
                ]}
            )
            await session.execute(stmt)
            cache.video_map.update({v['bvid']: v['song_id'] for v in missing_video_df.to_dict('records')})
        
# =============   直接被调用的操作  =========
        
async def execute_import_songs(session: AsyncSession, df: pd.DataFrame, strict: bool,    cache: Cache | None = None ):
    if not cache:
        cache = Cache()
    try:
        await cache.ensure_loaded(session, ['video_map', 'song_map', 'artist_maps', 'song_artist_maps'])
        
        for start in range(0, len(df), BATCH_SIZE):
            print(start)
            batch_df = df.iloc[start: start + BATCH_SIZE]
            
            await insert_artists(session, batch_df, cache)
            new_songs = await insert_songs(session, batch_df, cache)
            if new_songs:
                await insert_relations(session, batch_df, strict, new_songs, cache)
            await insert_videos(session, batch_df, cache)
            
        
        await session.commit()

    except IntegrityError as e:
        await session.rollback()
        print("插入数据出错:", e)
    
async def execute_import_snapshots(
    session: AsyncSession,
    date: str,
    strict: bool,
    cache: Cache | None = None
    ):
    if not cache:
        cache = Cache()
    date_ = datetime.strptime(date, "%Y-%m-%d")
    df = read_excel(f'./data/数据/{date_.strftime("%Y%m%d")}.xlsx').assign(date=date_)

    # ---------- 原有记录清空 -------------
    delete_stmt = delete(Snapshot).where(
        Snapshot.date == date_
    )    
    await session.execute(delete_stmt)

    total = len(df)
    for start in range(0, total, BATCH_SIZE):
        end = start + BATCH_SIZE
        batched_df = df.iloc[start:end]
        await insert_videos(session, batched_df, cache)

        # -------- 插入数据记录 ---------
        snapshots = batched_df[["bvid", "date", "view", "favorite", "coin", "like"]].to_dict(orient="records")
        stmt = pg_insert(Snapshot).values(snapshots).on_conflict_do_update(
            index_elements=['bvid', 'date'],
            set_={field: pg_insert(Snapshot).excluded[field] for field in ['view', 'favorite', 'coin', 'like']}
        )
        await session.execute(stmt)
        await session.flush()
    
async def execute_import_rankings(
    session: AsyncSession, 
    board: str, 
    part: str, 
    issue: int,  
    strict: bool,
    cache: Cache | None = None
    ):
    if not cache:
        cache = Cache()

    delete_stmt = delete(Ranking).where(
        (Ranking.board == board) &
        (Ranking.part == part) &
        (Ranking.issue == issue)
    )
    
    await session.execute(delete_stmt)
    
    df: pd.DataFrame = read_excel(
        generate_board_file_path(board, part, issue),
    ).assign( board = board, part = part, issue = issue)
    
    if strict:
        # 严格模式的意义就在于这里有验证
        # 验证过后，还是按照一般那样，很多字段允许null
        errors = validate_excel(df)
        if len(errors) >= 1:
            raise Exception("\n".join(errors))

    
    total = len(df)
    for start in range(0, total, BATCH_SIZE):
        end = start + BATCH_SIZE
        batch_df = df.iloc[start: end]
        if (part != 'new'):
            await insert_artists(session, batch_df, cache)
            new_songs = await insert_songs(session, batch_df, cache)
            if new_songs:
                await insert_relations(session, batch_df, strict, new_songs, cache)
            await insert_videos(session, batch_df, cache)

            insert_df = batch_df.assign(
                board=board,
                issue=issue,
                part=part
            )[['board', 'part', 'issue', 'rank','bvid','count','point','view','favorite','coin','like','view_rank','favorite_rank','coin_rank','like_rank']]
            insert_df['count'] = insert_df['count'].astype("Int64")
            insert_df['song_id'] = insert_df['bvid'].map(cache.video_map)
        
        else: 
            await insert_videos(session, batch_df, cache)
            insert_df = batch_df.assign(
                board=board,
                issue=issue,
                part=part
            )[['board', 'part', 'issue', 'rank','bvid','point','view','favorite','coin','like','view_rank','favorite_rank','coin_rank','like_rank']]
            insert_df['song_id'] = insert_df['bvid'].map(cache.video_map)

        insert_df = insert_df.replace({pd.NA: None})
        records = insert_df.to_dict(orient='records')
        
        insert_stmt = pg_insert(Ranking).values(records).on_conflict_do_nothing()
        await session.execute(insert_stmt)

