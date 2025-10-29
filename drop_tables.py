import asyncio
from app.session import engine
from app.models import Base
import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def drop_all():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

asyncio.run(drop_all())