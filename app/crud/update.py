from sqlalchemy import select, func, and_, update, exists
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from datetime import date, timedelta

from app.models import Video, Snapshot, Producer, Song

MIN_TOTAL_VIEW = 10000
BASE_THRESHOLD = 100

async def update_video_streaks(session: AsyncSession, current_date: date):
    """
    更新 Video.streak 字段
    """

    # -----------------------------
    # 0. 所有已毕业视频置0
    # -----------------------------
    graduated = exists().where(
        and_(
            Snapshot.bvid == Video.bvid,
            Snapshot.view >= MIN_TOTAL_VIEW
        )
    )
    
    stmt = (
        update(Video)
        .where(graduated)
        .values(streak=0)
        )
    
    await session.execute(stmt)
    
    
    # -----------------------------
    # 1. 获取所有需要更新streak的视频
    # -----------------------------
    stmt = (
        select(Video)
        .where(
            ~graduated,
            Video.streak_date < current_date
        )
    )

    videos = (await session.execute(stmt)).scalars().all()
    if not videos:
        return

    # -----------------------------
    # 2. 获取所有 video 最新 Snapshot
    # -----------------------------

    latest_snaps = (
        await session.execute(
            select(Snapshot)
            .where(Snapshot.date == current_date)
        )
    ).scalars().all()

    latest_map = {s.bvid: s for s in latest_snaps}

    # -----------------------------
    # 3. 获取各视频之前的 Snapshot（用于计算日涨）
    # -----------------------------
    all_prev = (
        select(
            Snapshot.bvid,
            Snapshot.view,
            Snapshot.date,
            func.row_number().over(
                partition_by=Snapshot.bvid,
                order_by=Snapshot.date.desc()
            ).label('rn')
        )
        .where(Snapshot.date < current_date)
    ).subquery()
    
    prev_rows = (await session.execute(
        select(all_prev).where(all_prev.c.rn == 1)
    )).all()
            
    prev_map = {s.bvid: s for s in prev_rows}

    # -----------------------------
    # 4. 遍历每个视频，执行 streak 更新逻辑
    # -----------------------------
    for video in videos:
        bvid = video.bvid
        streak = video.streak
        latest = latest_map.get(bvid)
        prev = prev_map.get(bvid)

        # ==============================================================
        # A. 当天有 Snapshot
        # ==============================================================
        if latest:
            if not prev:
                # 没有上次Snapshot，说明新曲，不给streak
                streak = 0
            else:
                daily_increase = (latest.view - prev.view) / ((latest.date - prev.date).days or 1)
                if daily_increase >= BASE_THRESHOLD:  # 涨速 >= 100
                    streak = 0
                else:
                    streak += 1

        # ==============================================================
        # B. 当天无 Snapshot
        # ==============================================================        
        else:
            streak += 1

        # 保存
        video.streak = streak
        video.streak_date = current_date

    await session.commit()


