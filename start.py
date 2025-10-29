import sys
import asyncio

# 🔧 Windows 下必须加这一行，放在所有 import 的最前面！
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import uvicorn
from app.config import settings

if __name__ == "__main__":
    uvicorn.run("app.main:app", host=settings.API_HOST, port=int(settings.PORT), log_level="info")