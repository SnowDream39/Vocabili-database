
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, and_, update, delete, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from app.models import Song, Producer, Synthesizer, Vocalist, Uploader, Video, song_producer, song_synthesizer, song_vocalist, Snapshot, Ranking
from app.utils.misc import make_duration_int

from ..utils import validate_excel, read_excel, ensure_columns, normalize_nullable_int_columns, normalize_nullable_str_columns
from ..utils.filename import generate_board_file_path
from ..utils.cache import Cache

import pandas as pd
from datetime import datetime, timedelta

from collections import namedtuple



BATCH_SIZE = 200

# =================  比较小的操作，不对外公开  ====================

async def resolve_changed_names(
    session: AsyncSession, 
    df: pd.DataFrame,
    cache: Cache | None = None
):
    if not cache:
        cache = Cache()
    await cache.ensure_loaded(session, ['song_map', 'video_map'])
    

    song_map = cache.song_map        # {name -> song_id}
    video_map = cache.video_map      # {bvid -> song_id}

    new_song_names = []             # 需要新建的Song名称
    video_updates = []              # (bvid, new_song_id)

    # === 第一步：收集所有需要创建的新 name ===
    for _, row in df.iterrows():
        bvid = row["bvid"]
        new_name = row["name"]

        if bvid not in video_map:
            continue

        old_song_id = video_map[bvid]

        # 如果新名字已经有关联，则看看是否等于 old_song_id
        if new_name in song_map:
            if song_map[new_name] != old_song_id:
                # Video 的 song_id 要改到新的 song_id
                video_updates.append((bvid, song_map[new_name]))
            continue

        # 新名字不存在 → 后续要创建一个新的 Song
        new_song_names.append(new_name)

    # 去重
    new_song_names = list(set(new_song_names))

    # === 第二步：批量插入新的 Song ===
    created_name_to_id = {}
    if new_song_names:
        insert_data = [{"name": name} for name in new_song_names]
        stmt = pg_insert(Song).returning(Song.id, Song.name)
        rows = (await session.execute(stmt, insert_data)).fetchall()

        for sid, name in rows:
            created_name_to_id[name] = sid
            cache.song_map[name] = sid  # 更新缓存

    # === 第三步：对所有 Video 更新 song_id ===
    # 再执行 df 遍历一次，找出因新 Song 创建而需要更新的项
    for _, row in df.iterrows():
        bvid = row["bvid"]
        new_name = row["name"]

        if bvid not in video_map:
            continue

        # 如果 name 属于新创建的 Song，则更新到新 id
        if new_name in created_name_to_id:
            new_id = created_name_to_id[new_name]
            video_updates.append((bvid, new_id))
            continue

    # 执行 update
    for bvid, new_song_id in video_updates:
        stmt = (
            update(Video)
            .where(Video.bvid == bvid)
            .values(song_id=new_song_id)
        )
        await session.execute(stmt)

        # 缓存同步更新
        video_map[bvid] = new_song_id

    await session.commit()

    return {
        "created_songs": len(new_song_names),
        "updated_videos": len(video_updates),
    }


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
        ):
            if not pd.isna(row[col]):
                for name in row[col].split('、'):
                    artist_map[table].add(name)
                    
        if not pd.isna(row['uploader']):  # 上传者
            artist_map[Uploader].add(row['uploader'])

    for table, names in artist_map.items():
        if not names:
            continue
        new_names = names - cache.artist_maps[table].keys()
        if (len(new_names) > 0):
            print(f"{table.__tablename__} 创建artist：{new_names}")

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
    ensure_columns(df, ['image_url'])
    await cache.ensure_loaded(session, ['song_map'])
    # (name, type)
    SongRecord = namedtuple('SongRecord', ['name', 'type'])
    song_records: list[SongRecord] = []
    for row in df.to_dict(orient='records'):
        name = row['name']
        song_type = row['type'] if not pd.isna(row['type']) else None
        if not pd.isna(name):
            song_records.append(SongRecord(name, song_type))
        
    song_records = list(set(song_records))
    if song_records:
        excluded = pg_insert(Song).excluded
        song_table_columns = ['name', 'type']
        stmt = pg_insert(Song).values([
            {k: v for k, v in s._asdict().items() if k in song_table_columns}
            for s in song_records
            ]).on_conflict_do_update(
            index_elements=['name'],
            # 目前不更新 display_name
            set_={ 'type': excluded['type'] }
        ).returning(Song.id, Song.name)
        result = await session.execute(stmt)
        await session.flush()
        rows = result.all()
        
        cache.song_map.update({r[1]: r[0] for r in rows})
    


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
        new_song_df = new_song_df.loc[new_song_df['synthesizer'].notna() & new_song_df['author'].notna() & new_song_df['vocal'].notna()].copy()
    
    for cls, table, field in (
        (Producer, song_producer, 'author'),
        (Synthesizer, song_synthesizer, 'synthesizer'),
        (Vocalist, song_vocalist, 'vocal')
    ):
        rel_df = (
            new_song_df[['name', field]]
            .assign( song_id=lambda df: df['name'].map(cache.song_map))
            .copy()
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
            
            
async def update_relations(
    session: AsyncSession,
    df: pd.DataFrame,
    cache: Cache | None = None
    ):
    """
    更新全部artist关系
    """
    if not cache:
        cache = Cache()
    await cache.ensure_loaded(session, ['song_map', 'artist_maps'])
       
    song_df = df[['name', 'synthesizer', 'author', 'vocal']].copy()
    
    for cls, table, field in (
        (Producer, song_producer, 'author'),
        (Synthesizer, song_synthesizer, 'synthesizer'),
        (Vocalist, song_vocalist, 'vocal')
    ):
        rel_df = (
            song_df[['name', field]]
            .assign( song_id=lambda df: df['name'].map(cache.song_map))
            .copy()
        )
        
        rel_df = rel_df[rel_df['song_id'].notna()]
        rel_df = rel_df[rel_df[field].notna()].copy()
        rel_df[field] = rel_df[field].astype(str).str.split('、')
        rel_df = rel_df.explode(field)
        rel_df['artist_id'] = rel_df[field].map(cache.artist_maps[cls])
        rel_df = rel_df[rel_df['artist_id'].notna()].copy()

        song_ids = rel_df['song_id'].unique().tolist()
        if song_ids:
            stmt = delete(table).where(table.c.song_id.in_(song_ids))
            await session.execute(stmt)
            

        rel_records = set(map(tuple, rel_df[['song_id', 'artist_id']].to_numpy()))


        if rel_records:
            new_rel_dicts = [{'song_id': t[0], 'artist_id': t[1]} for t in rel_records]
            stmt = pg_insert(table).values(new_rel_dicts).on_conflict_do_nothing()
            await session.execute(stmt)


async def insert_videos(
    session: AsyncSession, 
    df: pd.DataFrame, 
    update: bool = False,
    cache: Cache | None = None
    ):

    """
    插入视频。冲突更新。
    
    如果歌曲不存在就不插入。
    
    如果数据里面没有image_url，不会更新。
    """
    
    if not cache:
        cache = Cache()
    
    has_thumbnail = 'image_url' in df.columns
    use_cols = ['bvid', 'title', 'pubdate', 'duration', 'page', 'song_id', 'uploader_id', 'copyright']
    update_cols = ['title', 'pubdate', 'uploader_id', 'duration', 'page', 'copyright', 'thumbnail']
    
    normalize_nullable_int_columns(df, ['page', 'copyright'])
    normalize_nullable_str_columns(df, ['duration', 'title'])
    await cache.ensure_loaded(session, ['video_map', 'song_map', 'artist_maps'])
    df = df.assign(
        song_id = lambda d: d['name'].map(cache.song_map),
        uploader_id = lambda d: d['uploader'].map(cache.artist_maps[Uploader]),
        duration = lambda d: d['duration'].map(make_duration_int)
    )
    
    if has_thumbnail:  
        df = df.rename(columns={'image_url': 'thumbnail'})
        use_cols.append('thumbnail')
        normalize_nullable_str_columns(df, ['thumbnail'])
    
    df = df.loc[df['song_id'].notna()][use_cols].copy()
    
    normalize_nullable_int_columns(df, ['uploader_id'])
    
    # 转换成 record dict
    records = df.to_dict(orient="records")

    # ----------- UPSERT（核心）-----------
    if update:
        excluded = pg_insert(Video).excluded
        stmt = (
            pg_insert(Video)
            .values(records)
            .on_conflict_do_update(
                index_elements=["bvid"],
                set_={
                    field: excluded[field] for field in update_cols
                }
            )
        )
    else:
        stmt = pg_insert(Video).values(records).on_conflict_do_nothing(
            index_elements=["bvid"]
        )
        
        
    await session.execute(stmt)

    # ----------- 更新 cache -----------
    for row in records:
        cache.video_map[row["bvid"]] = row["song_id"]

        
# =============   直接被调用的操作  =========
        

    
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
    
    if strict:
        validate_excel(df)
        
    # ---------- 原有记录清空 -------------
    delete_stmt = delete(Snapshot).where(
        Snapshot.date == date_
    )    
    await session.execute(delete_stmt)

    total = len(df)
    for start in range(0, total, BATCH_SIZE):
        end = start + BATCH_SIZE
        batched_df = df.iloc[start:end].copy()
        await insert_videos(session, batched_df, False, cache)

        batched_df = batched_df[batched_df['bvid'].isin(cache.video_map.keys())]
        if not batched_df.empty:
        # -------- 插入数据记录 ---------
            snapshots = batched_df[["bvid", "date", "view", "favorite", "coin", "like"]].to_dict(orient="records")
            stmt = pg_insert(Snapshot).values(snapshots).on_conflict_do_update(
                index_elements=['bvid', 'date'],
                set_={field: pg_insert(Snapshot).excluded[field] for field in ['view', 'favorite', 'coin', 'like']}
            )
            await session.execute(stmt)
            await session.flush()
            await session.commit()
    
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

    try:
        
        total = len(df)
        for start in range(0, total, BATCH_SIZE):
            end = start + BATCH_SIZE
            batch_df = df.iloc[start: end].copy()
            print(f"{start} ~ {end}")
            if (part != 'new' and board in ['vocaloid-daily', 'vocaloid-weekly']):
                await resolve_changed_names(session, batch_df, cache)
                await insert_artists(session, batch_df, cache)
                await insert_songs(session, batch_df, cache)
                await update_relations(session, batch_df, cache)
                await insert_videos(session, batch_df, True, cache)

                insert_df = batch_df.assign(
                    board=board,
                    issue=issue,
                    part=part
                )[['board', 'part', 'issue', 'rank','bvid','count','point','view','favorite','coin','like','view_rank','favorite_rank','coin_rank','like_rank']]
                insert_df['count'] = insert_df['count'].astype("Int64")
                insert_df['song_id'] = insert_df['bvid'].map(cache.video_map)
            
            else: 
                await insert_videos(session, batch_df, False, cache)
                insert_df = batch_df.assign(
                    board=board,
                    issue=issue,
                    part=part
                )[['board', 'part', 'issue', 'rank','bvid','point','view','favorite','coin','like','view_rank','favorite_rank','coin_rank','like_rank']]
                insert_df['song_id'] = insert_df['bvid'].map(cache.video_map)

            insert_df = insert_df.dropna(subset=['song_id'])
            insert_df = insert_df.replace({pd.NA: None})
            records = insert_df.to_dict(orient='records')
            
            insert_stmt = pg_insert(Ranking).values(records).on_conflict_do_nothing()
            await session.execute(insert_stmt)
            await session.commit()
    
    except IntegrityError as e:
        await session.rollback()
        print("插入数据出错:", e)
    

