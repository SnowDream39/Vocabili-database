from typing import Dict, Set, Tuple, Type
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, Table
from ..models import Producer, Synthesizer, Vocalist, Uploader, Song, Video, song_producer, song_synthesizer, song_vocalist
from typing import Any

type ORMTable = Producer | Synthesizer | Vocalist | Uploader

class Cache:

    """
    请求级缓存，用于函数间传递和更新。
    每次请求创建一个实例，不共享全局数据。
    """

    def __init__(self):

        # 歌曲映射: name -> id
        self.song_map: Dict[str, int] = {}
        # 视频映射: bvid -> song_id
        self.video_map: Dict[str, int] = {}
        # 艺术家映射: 类 -> {name -> id}
        self.artist_maps: Dict[type, Dict[str, int]] = {}
        # 歌曲-艺术家关系映射: 类 -> set[(song_id, artist_id)]
        self.song_artist_maps: Dict[type, Set[Tuple[int, int]]] = {}

    # ---------- 异步加载方法 ----------
    async def load_artists(self, session: AsyncSession, artist_tables: list[Any]):
        """按需加载艺术家"""
        for table in artist_tables:
            result = await session.execute(select(table.id, table.name))
            self.artist_maps[table] = {r[1]: r[0] for r in result.all()}

    async def load_songs(self, session: AsyncSession):
        """按需加载歌曲"""
        result = await session.execute(select(Song.id, Song.name))
        self.song_map = {r[1]: r[0] for r in result.all()}

    async def load_videos(self, session: AsyncSession):
        """按需加载歌曲"""
        result = await session.execute(select(Video.bvid, Video.song_id))
        self.video_map = {r[1]: r[0] for r in result.all()}

    async def load_song_artist_relations(self, session: AsyncSession, rel_tables: Dict[Type, Table]):
        """按需加载歌曲-艺术家关系"""
        for cls, table in rel_tables.items():
            result = await session.execute(select(table.c.song_id, table.c.artist_id))
            self.song_artist_maps[cls] = set(result.all()) 

            
    def has_videos(self) -> bool:
        return bool(self.video_map)

    def has_songs(self) -> bool:
        return bool(self.song_map)

    def has_artists(self) -> bool:
        # 至少有一个类映射非空就算有
        return any(bool(m) for m in self.artist_maps.values())

    def has_song_artist_relations(self) -> bool:
        # 至少有一个类的关系非空就算有
        return any(bool(s) for s in self.song_artist_maps.values())
    
    # ---------- 统一懒加载方法 ----------
    async def ensure_loaded(self, session, cache_keys: list[str]):
        """
        确保所需缓存已经加载，没有则从数据库载入。

        session: SQLAlchemy AsyncSession
        cache_keys: 需要保证加载的缓存列表，可选值：
            'video_map', 'song_map', 'artist_maps', 'song_artist_maps'
        """
        if 'video_map' in cache_keys and not self.has_videos():
            await self.load_videos(session)

        if 'song_map' in cache_keys and not self.has_songs():
            await self.load_songs(session)

        if 'artist_maps' in cache_keys and not self.has_artists():
            tables = [Producer, Synthesizer, Vocalist, Uploader]
            await self.load_artists(session, tables)

        if 'song_artist_maps' in cache_keys and not self.has_song_artist_relations():
            rel_tables = {
                Producer: song_producer,
                Synthesizer: song_synthesizer,
                Vocalist: song_vocalist
            }
            await self.load_song_artist_relations(session, rel_tables)
