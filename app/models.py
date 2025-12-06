from sqlalchemy import Column, ForeignKey, String, Date, SmallInteger, Integer, DateTime, Text, UniqueConstraint, Table, MetaData, PrimaryKeyConstraint, Index, and_
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase, selectinload 
from datetime import datetime
from datetime import date as datetype
from typing import List

metadata = MetaData(schema="public")
class Base(DeclarativeBase):
    metadata = metadata
    pass

# =============  关系表  ==============

song_producer = Table(
    "song_producer", Base.metadata,
    Column("song_id", ForeignKey("song.id"), primary_key=True, autoincrement=False),
    Column("artist_id", ForeignKey("producer.id"), primary_key=True, autoincrement=False),
)

song_synthesizer = Table(
    "song_synthesizer", Base.metadata,
    Column("song_id", ForeignKey("song.id"), primary_key=True, autoincrement=False),
    Column("artist_id", ForeignKey("synthesizer.id"), primary_key=True, autoincrement=False),
)

song_vocalist = Table(
    "song_vocalist", Base.metadata,
    Column("song_id", ForeignKey("song.id"), primary_key=True, autoincrement=False),
    Column("artist_id", ForeignKey("vocalist.id"), primary_key=True, autoincrement=False),
)


# ===============  对象表  ================


class Artist:
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text, unique=True)
    vocadb_id: Mapped[int] = mapped_column(Integer, nullable=True)

class Producer(Artist, Base):
    """
    P主
    """
    __tablename__ = "producer"
    songs: Mapped[List["Song"]] = relationship(
        secondary=song_producer,
        back_populates="producers"
    )

class Vocalist(Artist, Base):
    """
    歌手
    """
    __tablename__ = "vocalist"
    songs: Mapped[List["Song"]] = relationship(
        secondary=song_vocalist,
        back_populates="vocalists"
    )

class Synthesizer(Artist, Base):
    """
    引擎
    """
    __tablename__ = "synthesizer"
    songs: Mapped[List["Song"]] = relationship(
        secondary=song_synthesizer,
        back_populates="synthesizers"
    )

class Uploader(Artist, Base):
    """
    UP主
    """
    __tablename__ = "uploader"
    videos: Mapped[List["Video"]] = relationship("Video", back_populates="uploader")

class Song(Base):
    """
    歌曲
    """
    __tablename__ = "song"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, unique=True)
    type: Mapped[str] = mapped_column(String(4))

    producers: Mapped[List["Producer"]] = relationship(
        secondary=song_producer,
        back_populates="songs"
    )
    synthesizers: Mapped[List["Synthesizer"]] = relationship(
        secondary=song_synthesizer,
        back_populates="songs"
    )
    vocalists: Mapped[List["Vocalist"]] = relationship(
        secondary=song_vocalist,
        back_populates="songs"
    )
    videos: Mapped[List["Video"]] = relationship(
        "Video",
        back_populates="song"
    )
    rankings: Mapped[List["Ranking"]] = relationship(
        "Ranking", 
        back_populates="song"
    )

class Video(Base):
    """
    视频
    """
    __tablename__ = "video"
    bvid: Mapped[str] = mapped_column(String(12), primary_key=True, autoincrement=False)
    title: Mapped[str] = mapped_column(Text)
    pubdate: Mapped[datetime] = mapped_column(TIMESTAMP)
    uploader_id: Mapped[int] = mapped_column(Integer, ForeignKey('uploader.id'), nullable=True)
    song_id: Mapped[int] = mapped_column(Integer, ForeignKey('song.id'), nullable=False)
    copyright: Mapped[int] = mapped_column(SmallInteger, nullable=True)
    thumbnail: Mapped[str] = mapped_column(Text, nullable=True)
    duration: Mapped[int] = mapped_column(SmallInteger, nullable=True)
    page: Mapped[int] = mapped_column(SmallInteger, nullable=True)

    uploader: Mapped["Uploader"] = relationship("Uploader", back_populates="videos")
    song: Mapped["Song"] = relationship("Song", back_populates="videos")
    snapshots: Mapped[List["Snapshot"]] = relationship("Snapshot", back_populates="video")
    rankings: Mapped[List["Ranking"]] = relationship("Ranking", back_populates="video")

class Snapshot(Base):
    """
    数据记录
    """
    __tablename__ = 'snapshot'
    bvid: Mapped[str] = mapped_column(String, ForeignKey("video.bvid"), autoincrement=False)
    date: Mapped[datetype] = mapped_column(Date)

    view: Mapped[int] = mapped_column(Integer)
    favorite: Mapped[int] = mapped_column(Integer)
    coin: Mapped[int] = mapped_column(Integer)
    like: Mapped[int] = mapped_column(Integer)
    
    video: Mapped["Video"] = relationship("Video", back_populates="snapshots")
    
    __table_args__ = (
        PrimaryKeyConstraint("bvid", 'date'),
    )
    
class Ranking(Base):
    """
    排名记录
    """
    __tablename__ = 'ranking'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    board: Mapped[str] = mapped_column(String(20))
    part: Mapped[str] = mapped_column(String(20))
    issue: Mapped[str] = mapped_column(SmallInteger, index=True)
    rank: Mapped[int] = mapped_column(Integer, index=True)
    song_id: Mapped[int] = mapped_column(Integer, ForeignKey('song.id'), index=True)
    bvid: Mapped[str] = mapped_column(String(12), ForeignKey('video.bvid'), index=True)
    count: Mapped[int] = mapped_column(SmallInteger, nullable=True)
    point: Mapped[int] = mapped_column(Integer)
    view: Mapped[int] = mapped_column(Integer)
    favorite: Mapped[int] = mapped_column(Integer)
    coin: Mapped[int] = mapped_column(Integer)
    like: Mapped[int] = mapped_column(Integer)
    view_rank: Mapped[int] = mapped_column(Integer)
    favorite_rank: Mapped[int] = mapped_column(Integer)
    coin_rank: Mapped[int] = mapped_column(Integer)
    like_rank: Mapped[int] = mapped_column(Integer)

    song: Mapped["Song"] = relationship("Song", back_populates="rankings")
    video: Mapped["Video"] = relationship("Video", back_populates="rankings")
    
    __table_args__ = (
        Index('idx_ranking_board_part', 'board', 'part'),
    )
    

TABLE_MAP = {
    'song': Song,
    'video': Video,
    'producer': Producer,
    'synthesizer': Synthesizer,
    'vocalist': Vocalist,
    'uploader': Uploader
}

REL_MAP = {
    'producer': song_producer,
    'synthesizer': song_synthesizer,
    'vocalist': song_vocalist,
}

song_load_full = [
    selectinload(Song.videos).selectinload(Video.uploader),
    selectinload(Song.producers),
    selectinload(Song.synthesizers),
    selectinload(Song.vocalists)
]
